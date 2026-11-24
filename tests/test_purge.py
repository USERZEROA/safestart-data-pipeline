"""The purge of retired originals (official/purge.py): reference counts across URLs, editions and sitemaps on a
hand-made store, the 30-day wait, a held current object never deleted, objects not on this host or referenced by
nothing, a dry run, a site whose crawl lock another process holds, a store of the first version, and the whole cycle
through real daily runs: a file gone upstream is deleted 30 days later, one restored before is kept."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from safestart_pipeline import cli
from safestart_pipeline.crawl.crawler import iso
from safestart_pipeline.crawl.locks import SiteLock
from safestart_pipeline.crawl.store import Store
from safestart_pipeline.official import purge
from test_crawler import Clock, pdf_bytes
from test_official import PDF, START, Bench, baseline, site_answers

NOW = datetime(2026, 12, 30, 12, 0, tzinfo=UTC)
LONG_AGO, RECENTLY = NOW - timedelta(days=40), NOW - timedelta(days=10)


def digest(name: str) -> str:
    return hashlib.sha256(name.encode()).hexdigest()


def put_object(root: Path, site: str, sha256: str, size: int = 10) -> Path:
    path = root / "raw" / site / "objects" / sha256[:2] / sha256[2:4] / sha256
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    return path


def add_url(store: Store, url: str, sha256: str, *, gone: datetime | None = None, state: str = "fetched",
            editions: tuple[str, ...] = ()) -> None:
    """A held URL with its current copy `sha256` and earlier `editions`, retired at `gone` when given."""
    store.insert_url(url, source="sitemap", state=state, now="2026-11-17T00:00:00Z")
    store.update(url, sha256=sha256, content_sha256=sha256, detected_type="pdf", content_length=10,
                 gone_since=iso(gone) if gone else None,
                 purge_after=iso(gone + timedelta(days=30)) if gone else None)
    for number, earlier in enumerate((*editions, sha256)):
        store.add_edition(url, content_sha256=earlier, sha256=earlier, size=10, content_type="application/pdf",
                          detected_type="pdf", first_seen_at=f"2026-11-1{number}T00:00:00Z")


@pytest.fixture
def store_root(tmp_path) -> Path:
    """One site's store and objects: every case of the reference count."""
    root = tmp_path
    store = Store(root / "state" / "crawl-osha.sqlite")
    https = "https://www.osha.gov"
    add_url(store, f"{https}/held.pdf", digest("shared"))  # current: keeps "shared"
    add_url(store, f"{https}/alias.pdf", digest("shared"), gone=LONG_AGO, state="not_found")  # retired, but shares it
    add_url(store, f"{https}/gone.pdf", digest("gone"), gone=LONG_AGO, state="not_found")
    add_url(store, f"{https}/recent.pdf", digest("recent"), gone=RECENTLY, state="not_found")  # 30 days not yet over
    add_url(store, f"{https}/edited.pdf", digest("edited-now"), editions=(digest("edited-before"),))  # history kept
    add_url(store, f"{https}/gone-edited.pdf", digest("gone-now"), editions=(digest("gone-before"),), gone=LONG_AGO,
            state="not_found")
    add_url(store, f"{https}/moved.pdf", digest("moved"), state="redirect")  # moved, not retired
    add_url(store, f"{https}/elsewhere.pdf", digest("elsewhere"), gone=LONG_AGO, state="not_found")  # not on this host
    add_url(store, f"{https}/sitemap-body.pdf", digest("sitemap"), gone=LONG_AGO, state="not_found")
    store.record_sitemap(f"{https}/sitemap.xml", parent=None, fetched_at="2026-11-17T00:00:00Z", status=200,
                         kind="urlset", entries=1, sha256=digest("sitemap"), error=None)
    store.close()
    for name in ("shared", "gone", "recent", "edited-now", "edited-before", "gone-now", "gone-before", "moved",
                 "sitemap", "nobody"):
        put_object(root, "osha", digest(name))
    (root / "raw" / "osha" / "objects" / "tmp").mkdir()
    (root / "raw" / "osha" / "objects" / "tmp" / "download-in-progress").write_bytes(b"...")
    return root


def present(root: Path, name: str) -> bool:
    sha256 = digest(name)
    return (root / "raw" / "osha" / "objects" / sha256[:2] / sha256[2:4] / sha256).exists()


KEPT = ("shared", "recent", "edited-now", "edited-before", "moved", "sitemap", "nobody")
FREED = ("gone", "gone-now", "gone-before")


def test_an_object_goes_only_when_every_url_and_edition_referencing_it_was_retired_30_days_ago(store_root):
    clock = Clock()
    clock.now = NOW
    dry = purge.purge(store_root, delete=False, sites=["osha"], now=lambda: clock.now, sleep=clock.sleep)["osha"]
    assert (dry["outcome"], dry["selected"], dry["deleted"], dry["bytes"], dry["absent"]) == ("dry_run", 3, 0, 30, 1)
    assert all(present(store_root, name) for name in (*KEPT, *FREED))  # a dry run deletes nothing

    done = purge.purge(store_root, delete=True, sites=["osha"], now=lambda: clock.now, sleep=clock.sleep)["osha"]
    assert (done["outcome"], done["selected"], done["deleted"], done["bytes"]) == ("done", 3, 3, 30)
    assert sorted(item["sha256"] for item in done["examples"]) == sorted(digest(name) for name in FREED)
    # Every deleted object is reported (a dry run lists only examples of what it would delete).
    assert sorted(item["sha256"] for item in done["deleted_objects"]) == sorted(digest(name) for name in FREED)
    assert done["failed"] == [] and "deleted_objects" not in dry
    assert not any(present(store_root, name) for name in FREED)
    assert all(present(store_root, name) for name in KEPT)
    assert done["absent"] == 1  # elsewhere.pdf's object is not on this host (the server lacks the crawls' archive)
    assert done["unreferenced"]["files"] == 1 and done["unreferenced"]["examples"] == [digest("nobody")]  # kept
    assert (store_root / "raw" / "osha" / "objects" / "tmp" / "download-in-progress").exists()
    gone = next(item for item in done["examples"] if item["sha256"] == digest("gone"))
    assert (gone["url"], gone["references"], gone["goneSince"]) == ("https://www.osha.gov/gone.pdf", 2, iso(LONG_AGO))

    again = purge.purge(store_root, delete=True, sites=["osha"], now=lambda: clock.now, sleep=clock.sleep)["osha"]
    assert (again["deleted"], again["absent"]) == (0, 4)  # deleted before: no longer on this host
    recorded = purge.latest_purges(store_root)["osha"]
    assert (recorded["outcome"], recorded["deleted"], recorded["details"]["absent"]) == ("done", 0, 4)
    with closing(sqlite3.connect(store_root / "state" / "official.sqlite")) as db:
        assert db.execute("select outcome, deleted, deleted_bytes from purges order by id").fetchall() == [
            ("dry_run", 0, 0), ("done", 3, 30), ("done", 0, 0)]  # a dry run records no deletion
        details = json.loads(db.execute("select details from purges order by id limit 1 offset 1").fetchone()[0])
    assert sorted(item["sha256"] for item in details["deleted_objects"]) == sorted(digest(name) for name in FREED)


def test_an_object_that_cannot_be_deleted_is_reported_and_the_others_still_go(store_root, monkeypatch):
    clock = Clock()
    clock.now = NOW
    stuck, real_unlink = digest("gone-now"), Path.unlink

    def unlink(path: Path, *args, **kwargs) -> None:
        if path.name == stuck:
            raise PermissionError("the file is in use")
        real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", unlink)
    with pytest.raises(RuntimeError, match="purge failed for osha .*1 could not be deleted"):
        purge.purge(store_root, delete=True, sites=["osha"], now=lambda: clock.now, sleep=clock.sleep)
    monkeypatch.undo()
    assert present(store_root, "gone-now")
    assert not present(store_root, "gone") and not present(store_root, "gone-before")
    recorded = purge.latest_purges(store_root)["osha"]
    assert (recorded["outcome"], recorded["deleted"], recorded["deleted_bytes"]) == ("error", 2, 20)
    assert [item["sha256"] for item in recorded["details"]["failed"]] == [stuck]
    assert sorted(item["sha256"] for item in recorded["details"]["deleted_objects"]) == sorted(
        digest(name) for name in ("gone", "gone-before"))


def test_only_the_server_which_holds_the_feeds_signing_key_deletes_in_the_flow():
    assert purge.deletes_here({"OFFICIAL_FEED_SIGNING_KEY": "a key"})
    assert not purge.deletes_here({}) and not purge.deletes_here({"OFFICIAL_FEED_SIGNING_KEY": " "})


def test_a_site_another_process_is_crawling_is_left_for_the_next_run(store_root):
    clock = Clock()
    clock.now = NOW
    with SiteLock(store_root / "state" / "crawl-osha.lock"):
        found = purge.purge(store_root, delete=True, sites=["osha"], now=lambda: clock.now, sleep=clock.sleep)["osha"]
    assert found["outcome"] == "skipped" and "crawl-osha.lock" in found["note"]
    assert all(present(store_root, name) for name in (*KEPT, *FREED))
    assert clock.now - NOW >= purge.LOCK_WAIT  # waited for a short holder first


def test_a_store_of_the_first_version_or_none_is_skipped(tmp_path):
    (tmp_path / "state").mkdir()
    with closing(sqlite3.connect(tmp_path / "state" / "crawl-msha.sqlite")) as db:
        db.execute("create table urls (url text primary key, sha256 text, state text)")
    report = purge.purge(tmp_path, delete=True)
    assert report["msha"]["outcome"] == "skipped" and "first version" in report["msha"]["note"]
    assert report["osha"] == {"outcome": "skipped", "note": "no crawl inventory on this host"}
    assert {site: found["outcome"] for site, found in purge.latest_purges(tmp_path).items()} == {
        "osha": "skipped", "msha": "skipped", "arlweb": "skipped"}


@pytest.fixture
def bench(tmp_path):
    """The daily tests' example site and a crawl of it (test_official.py)."""
    site, clock = site_answers(), Clock()
    clock.now = START
    client = httpx.Client(transport=httpx.MockTransport(site.handler))
    yield Bench(tmp_path, site, clock, client)
    client.close()


def test_a_file_gone_upstream_is_deleted_30_days_later_and_one_restored_before_is_kept(bench):
    baseline(bench)
    objects = bench.root / "raw" / "example" / "objects"
    copy_of = {name: hashlib.sha256(pdf_bytes(size)).hexdigest() for name, size in (("c", 320), ("e", 330))}
    bench.site.set("/files/c.pdf", (404, b"", {}))
    bench.site.set("/files/e.pdf", (410, b"", {}))
    bench.run(days=7)
    bench.site.set("/files/e.pdf", (200, pdf_bytes(330), PDF))  # back, with its old bytes
    bench.run(days=7)
    assert bench.row("/files/c.pdf")["gone_since"] and not bench.row("/files/e.pdf")["gone_since"]

    def purged(days: int) -> dict:
        moment = bench.clock.now + timedelta(days=days)
        return purge.purge(bench.root, delete=True, sites=["example"], now=lambda: moment, sleep=bench.clock.sleep)

    assert purged(days=10)["example"]["deleted"] == 0  # 17 days after c.pdf went
    report = purged(days=24)["example"]  # 31 days after
    assert [item["sha256"] for item in report["examples"]] == [copy_of["c"]]
    assert not (objects / copy_of["c"][:2] / copy_of["c"][2:4] / copy_of["c"]).exists()
    assert (objects / copy_of["e"][:2] / copy_of["e"][2:4] / copy_of["e"]).exists()
    assert bench.row("/files/c.pdf")["sha256"] == copy_of["c"]  # the record of the copy stays; only the bytes go


def test_ssp_official_purge_reports_unless_told_to_delete(store_root, monkeypatch, capsys):
    monkeypatch.setenv("SAFESTART_DATA_ROOT", str(store_root))
    monkeypatch.setattr(cli, "setup_logging", lambda path: None)
    monkeypatch.setattr(purge, "utc_now", lambda: NOW)
    assert cli.main(["official", "purge"]) == 0
    assert '"outcome": "dry_run"' in capsys.readouterr().out and present(store_root, "gone")
    assert cli.main(["official", "purge", "--delete"]) == 0
    assert '"outcome": "done"' in capsys.readouterr().out and not present(store_root, "gone")
