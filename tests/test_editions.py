"""The crawl store's editions, changes and runs: a store of the first version migrated in place."""

from __future__ import annotations

import hashlib
import sqlite3

import pytest

from safestart_pipeline.crawl.store import ADDED_URL_COLUMNS, Store
from test_crawler import HOST

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
