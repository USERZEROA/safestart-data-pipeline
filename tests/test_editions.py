"""The crawl store's editions, changes and runs: a store of the first version migrated in place; and `ssp
official backfill-hashes` (resumable, objects only read)."""

from __future__ import annotations

import hashlib
import json
import sqlite3

import pytest

from safestart_pipeline import cli
from safestart_pipeline.crawl import backfill as backfill_module
from safestart_pipeline.crawl.backfill import backfill_hashes
from safestart_pipeline.crawl.content import main_text, page_hash
from safestart_pipeline.crawl.locks import SiteLock
from safestart_pipeline.crawl.store import ADDED_URL_COLUMNS, Store
from test_content import MAIN, drupal_page
from test_crawler import HOST, pdf_bytes

# The store as the first crawls wrote it (state/crawl-osha.sqlite on the workstation is of this version).
FIRST_SCHEMA = """
create table if not exists urls (
  url text primary key, source text not null, in_sitemap integer not null default 0, sitemap_lastmod text,
  first_seen_at text not null, discovered_from text, state text not null, skip_reason text, http_status integer,
  content_type text, content_length integer, sha256 text, location text, etag text, last_modified text,
  fetched_at text, attempts integer not null default 0, next_attempt_at text, error text
);
create index if not exists urls_by_state on urls (state, next_attempt_at);
create table if not exists links (from_url text not null, to_url text not null, primary key (from_url, to_url))
  without rowid;
create index if not exists links_by_target on links (to_url);
create table if not exists fetches (url text not null, fetched_at text not null, http_status integer, sha256 text,
  content_length integer, etag text, last_modified text, error text);
create table if not exists sitemaps (url text primary key, parent text, fetched_at text, http_status integer,
  kind text, entries integer, sha256 text, error text);
create table if not exists meta (key text primary key, value text);
"""
SOFT_ERROR = b"\r\n<!DOCTYPE html>\n<html><head><title>OSHA</title></head><body><p>Page not found</p></body></html>"
FETCHED_AT = "2026-11-18T00:24:09Z"


def first_store(path, rows: list[dict]) -> None:
    """A store of the first version holding `rows` (url, state and the copy's columns)."""
    db = sqlite3.connect(path)
    db.executescript(FIRST_SCHEMA)
    for row in rows:
        values = {"source": "sitemap", "first_seen_at": "2026-11-17T19:20:00Z", **row}
        db.execute(f"insert into urls ({', '.join(values)}) values ({', '.join('?' for _ in values)})",
                   tuple(values.values()))
        if row.get("sha256"):
            db.execute("insert into fetches (url, fetched_at, http_status, sha256, content_length)"
                       " values (?, ?, ?, ?, ?)", (row["url"], row.get("fetched_at"), row.get("http_status"),
                                                   row["sha256"], row.get("content_length")))
    db.commit()
    db.close()


def held(url: str, body: bytes, content_type: str, *, archive=None, **columns) -> dict:
    """A fetched row of the first version for `body`, the body stored under `archive` when one is given."""
    digest = hashlib.sha256(body).hexdigest()
    if archive is not None:
        path = archive / digest[:2] / digest[2:4] / digest
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    return {"url": url, "state": "fetched", "http_status": 200, "content_type": content_type,
            "content_length": len(body), "sha256": digest, "fetched_at": FETCHED_AT, "attempts": 1, **columns}


# The store ------------------------------------------------------------------------------------------------------------


def test_a_store_of_the_first_version_is_migrated_in_place(tmp_path):
    path = tmp_path / "state" / "crawl-old.sqlite"
    path.parent.mkdir()
    first_store(path, [held(f"{HOST}/a", b"<html><body>A</body></html>", "text/html", etag='"a1"'),
                       {"url": f"{HOST}/gone", "state": "not_found", "http_status": 404},
                       {"url": f"{HOST}/logo.css", "state": "skipped", "skip_reason": "asset"}])
    with sqlite3.connect(path) as db:
        before = [tuple(row) for row in db.execute("select * from urls order by url")]
        width = len(before[0])
    store = Store(path)
    columns = [row[1] for row in store.db.execute("pragma table_info(urls)")]
    assert columns[width:] == list(ADDED_URL_COLUMNS)
    assert [tuple(row)[:width] for row in store.db.execute("select * from urls order by url")] == before
    assert all(value is None for row in store.db.execute(f"select {', '.join(ADDED_URL_COLUMNS)} from urls")
               for value in row)
    tables = {row[0] for row in store.db.execute("select name from sqlite_master where type = 'table'")}
    assert {"editions", "changes", "runs"} <= tables
    index = store.db.execute("select sql from sqlite_master where name = 'fetches_by_url'").fetchone()[0]
    assert "fetches (url, fetched_at)" in index
    assert store.get(f"{HOST}/a")["state"] == "fetched" and store.counts() == {"fetched": 1, "not_found": 1,
                                                                                "skipped": 1}
    store.close()
    again = Store(path)  # opening a migrated store changes nothing
    assert [row[1] for row in again.db.execute("pragma table_info(urls)")] == columns
    assert again.db.execute("pragma integrity_check").fetchone()[0] == "ok"
    with pytest.raises(sqlite3.IntegrityError):
        again.add_change(f"{HOST}/a", "updated", observed_at=FETCHED_AT, run_id=None)
    again.close()


def test_a_store_two_processes_open_at_once_gains_each_column_once(tmp_path, monkeypatch):
    path = tmp_path / "crawl-old.sqlite"
    first_store(path, [])
    Store(path).close()  # the other process migrated it between this one's first look and its write lock
    looks = []

    def first_look_before_the_other(store):
        looks.append(store)
        return list(ADDED_URL_COLUMNS) if len(looks) == 1 else real(store)

    real = Store.missing_columns
    monkeypatch.setattr(Store, "missing_columns", first_look_before_the_other)
    store = Store(path)  # looks again under the lock: no "duplicate column name"
    assert len(looks) == 2 and [row[1] for row in store.db.execute("pragma table_info(urls)")][-1] == (
        ADDED_URL_COLUMNS[-1])
    store.close()


# The backfill ---------------------------------------------------------------------------------------------------------


@pytest.fixture
def first_version(tmp_path):
    """A store of the first version and a separate archive of its objects, as on the workstation."""
    archive = tmp_path / "archive"
    page, report = drupal_page(MAIN), pdf_bytes(1500)
    rows = [
        held(f"{HOST}/hazcom", page, "text/html; charset=utf-8", archive=archive),
        held(f"{HOST}/report.pdf", report, "application/pdf", archive=archive),
        held(f"{HOST}/fr/1997-02-07.pdf", SOFT_ERROR, "application/pdf", archive=archive),
        held(f"{HOST}/fr/2017-05-22-1.pdf", pdf_bytes(3000)[:2500], "application/pdf", archive=archive),
        held(f"{HOST}/lost-page", b"<html><body>Lost</body></html>", "text/html"),  # no object
        held(f"{HOST}/lost.xlsx", b"PK\x03\x04 lost", "application/vnd.ms-excel"),  # no object
        {"url": f"{HOST}/gone", "state": "not_found", "http_status": 404},
        {"url": f"{HOST}/photo.jpg", "state": "skipped", "skip_reason": "not_needed"},
    ]
    path = tmp_path / "root" / "state" / "crawl-osha.sqlite"
    path.parent.mkdir(parents=True)
    first_store(path, rows)
    return path, archive, page


def listing(folder) -> dict[str, tuple[int, int]]:
    return {str(path.relative_to(folder)): (path.stat().st_size, path.stat().st_mtime_ns)
            for path in folder.rglob("*")}


def test_the_backfill_examines_every_held_copy_and_only_reads_the_objects(first_version):
    path, archive, page = first_version
    before = listing(archive)
    store = Store(path)
    report = backfill_hashes(store, archive, batch=2)
    assert {key: report[key] for key in ("examined", "usable", "missing_objects", "rejected", "still_unexamined")} == {
        "examined": 6, "usable": 2, "missing_objects": 2, "rejected": {"soft_error": 1, "unreadable": 1},
        "still_unexamined": 2}
    assert sorted(item["url"].removeprefix(HOST) for item in report["rejected_examples"]) == [
        "/fr/1997-02-07.pdf", "/fr/2017-05-22-1.pdf"]
    assert listing(archive) == before  # nothing written beside the objects, not even a tmp folder

    hazcom, report_pdf = store.get(f"{HOST}/hazcom"), store.get(f"{HOST}/report.pdf")
    assert (hazcom["content_sha256"], hazcom["detected_type"]) == (page_hash(main_text(page)), "html")
    assert (report_pdf["content_sha256"], report_pdf["detected_type"]) == (report_pdf["sha256"], "pdf")
    assert hazcom["last_checked_at"] == FETCHED_AT and hazcom["next_check_at"] == "2026-12-18T00:24:09Z"
    assert report_pdf["next_check_at"] == "2026-11-25T00:24:09Z"
    assert [(row["url"].removeprefix(HOST), row["first_seen_at"]) for row in store.db.execute(
        "select * from editions order by url")] == [("/hazcom", FETCHED_AT), ("/report.pdf", FETCHED_AT)]
    soft, cut = store.get(f"{HOST}/fr/1997-02-07.pdf"), store.get(f"{HOST}/fr/2017-05-22-1.pdf")
    assert (soft["state"], soft["detected_type"], soft["content_sha256"]) == ("rejected", "html", None)
    assert soft["error"].startswith("rejected soft_error") and cut["error"].startswith("rejected unreadable")
    # Copies whose objects are missing, page or file, stay as they were: nothing is usable unless its bytes were seen.
    for url in (f"{HOST}/lost.xlsx", f"{HOST}/lost-page"):
        lost = store.get(url)
        assert (lost["state"], lost["content_sha256"], lost["detected_type"]) == ("fetched", None, None), url
    assert store.changes() == [] and store.get_meta("content_hash_format") == "main-text/1"
    assert store.get(f"{HOST}/gone")["detected_type"] is None and store.counts() == {
        "fetched": 4, "not_found": 1, "rejected": 2, "skipped": 1}

    again = backfill_hashes(store, archive)  # only the copies without an object are left to try
    assert (again["examined"], again["usable"], again["missing_objects"]) == (2, 0, 2)
    store.close()


def test_an_interrupted_backfill_resumes_where_it_stopped(first_version, monkeypatch):
    path, archive, _ = first_version
    real, calls = backfill_module.examine_held, []

    def failing(store, folder, row):
        calls.append(row["url"])
        if len(calls) == 4:
            raise KeyboardInterrupt
        return real(store, folder, row)

    monkeypatch.setattr(backfill_module, "examine_held", failing)
    store = Store(path)
    with pytest.raises(KeyboardInterrupt):
        backfill_hashes(store, archive, batch=2)
    store.close()  # as the command does on its way out, committing whatever is left
    store = Store(path)
    # The batch it finished is kept; the one it was in is dropped whole, the copy it had examined there included.
    assert store.unexamined_count() == 4
    assert store.get(f"{HOST}/fr/1997-02-07.pdf")["state"] == "fetched"
    assert len(store.db.execute("select * from editions").fetchall()) == 2
    monkeypatch.setattr(backfill_module, "examine_held", real)
    report = backfill_hashes(store, archive)
    assert (report["examined"], report["usable"], report["still_unexamined"]) == (4, 0, 2)
    assert store.counts() == {"fetched": 4, "not_found": 1, "rejected": 2, "skipped": 1}  # as after one whole run
    assert len(store.db.execute("select * from editions").fetchall()) == 2
    store.close()


def test_ssp_official_backfill_hashes(first_version, monkeypatch, capsys):
    path, archive, _ = first_version
    root = path.parent.parent
    monkeypatch.setenv("SAFESTART_DATA_ROOT", str(root))
    monkeypatch.setattr(cli, "setup_logging", lambda log: None)
    assert cli.main(["official", "backfill-hashes", "osha", "--objects-from", str(archive)]) == 0
    printed = json.loads(capsys.readouterr().out)
    assert (printed["site"], printed["objects"], printed["usable"]) == ("osha", str(archive), 2)
    assert cli.main(["status", "osha"]) == 0
    assert json.loads(capsys.readouterr().out)["copies_without_content_hash"] == 2
    with SiteLock(root / "state" / "crawl-osha.lock"), pytest.raises(RuntimeError, match="one crawl per site"):
        cli.main(["official", "backfill-hashes", "osha", "--objects-from", str(archive)])
    with pytest.raises(SystemExit, match="no object folder"):
        cli.main(["official", "backfill-hashes", "osha"])  # <data root>/raw/osha/objects does not exist here
