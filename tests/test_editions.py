"""The crawl store's editions, changes and runs: a store of the first version migrated in place; what the crawler
records for a URL it holds (new, changed, the same content again, gone, restored, moved, a refused body, a failed
answer, a robots.txt refusal); the first download's retries kept; a time limit that holds for every wait; an
interrupted run that keeps no answer half recorded; a copy whose object is not kept (the VM); and `ssp official
backfill-hashes` (resumable, objects only read)."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import timedelta

import httpx
import pytest

from safestart_pipeline import cli
from safestart_pipeline.crawl import backfill as backfill_module
from safestart_pipeline.crawl.backfill import backfill_hashes
from safestart_pipeline.crawl.content import main_text, page_hash
from safestart_pipeline.crawl.crawler import Crawler, iso, parse_iso
from safestart_pipeline.crawl.locks import SiteLock
from safestart_pipeline.crawl.store import ADDED_URL_COLUMNS, Store
from test_content import MAIN, drupal_page
from test_crawler import HOST, Clock, example_site, pdf_bytes

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
PDF = {"content-type": "application/pdf"}
PAGE = {"content-type": "text/html; charset=utf-8"}
SOFT_ERROR = b"\r\n<!DOCTYPE html>\n<html><head><title>OSHA</title></head><body><p>Page not found</p></body></html>"
FETCHED_AT = "2026-11-18T00:24:09Z"


class Answers:
    """A site whose answers are set per path: each request takes the next answer set for its path and the last one
    repeats. An answer is (status, body, headers), or "no answer" for a connection that fails. robots.txt is missing
    (allowing everything) unless answers are set for it."""

    def __init__(self):
        self.answers: dict[str, list] = {}
        self.requests: list[httpx.Request] = []

    def set(self, path: str, *answers) -> None:
        self.answers[path] = list(answers)

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.url.path == "/robots.txt" and "/robots.txt" not in self.answers:
            return httpx.Response(404)
        queue = self.answers.get(request.url.path) or [(404, b"", {})]
        answer = queue.pop(0) if len(queue) > 1 else queue[0]
        if answer == "no answer":
            raise httpx.ConnectError("connection refused", request=request)
        status, body, headers = answer
        return httpx.Response(status, content=body, headers=headers)


@pytest.fixture
def daily(tmp_path):
    site, clock = Answers(), Clock()
    client = httpx.Client(transport=httpx.MockTransport(site.handler))
    crawler = Crawler(example_site(name="daily", sitemaps=(), max_bytes=10**6), tmp_path, client, sleep=clock.sleep,
                      now=lambda: clock.now)
    yield crawler, site, clock
    crawler.store.close()
    client.close()


def ask(crawler: Crawler, clock: Clock, *urls: str, days: float = 1) -> dict:
    """Queue held URLs, as the daily run does, and ask for them `days` later."""
    clock.now += timedelta(days=days)
    for url in urls:
        crawler.store.update(url, state="pending", attempts=0, next_attempt_at=None)
    crawler.store.commit()
    return crawler.crawl()


def asked(crawler: Crawler, url: str, *, days: float = 0) -> str:
    """When the URL was last asked for (its latest fetch row), `days` on."""
    latest = crawler.store.db.execute("select max(fetched_at) from fetches where url = ?", (url,)).fetchone()[0]
    return iso(parse_iso(latest) + timedelta(days=days))


def found(crawler: Crawler) -> list[tuple[str, str]]:
    return [(row["url"].removeprefix(HOST), row["change"]) for row in crawler.store.changes()]


def objects(crawler: Crawler) -> set[str]:
    return {path.name for path in crawler.objects.root.rglob("*") if path.is_file()}


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


# What the crawler records for a URL it holds --------------------------------------------------------------------------


def test_a_held_file_keeps_its_copy_through_every_kind_of_answer(daily):
    crawler, site, clock = daily
    url, first, second = f"{HOST}/doc.pdf", pdf_bytes(300), pdf_bytes(400)
    site.set("/doc.pdf", (200, first, PDF))
    crawler.add_known(url, source="sitemap", in_sitemap=True)
    crawler.crawl()
    row = crawler.store.get(url)
    assert (row["state"], row["detected_type"], row["content_sha256"]) == ("fetched", "pdf",
                                                                           hashlib.sha256(first).hexdigest())
    assert row["last_checked_at"] == asked(crawler, url) and row["next_check_at"] == asked(crawler, url, days=7)
    assert found(crawler) == [("/doc.pdf", "new")] and len(crawler.store.editions(url)) == 1

    # The same bytes again: nothing stored, nothing recorded but the check.
    stored = objects(crawler)
    ask(crawler, clock, url)
    row = crawler.store.get(url)
    assert (row["state"], row["last_checked_at"], objects(crawler)) == ("fetched", asked(crawler, url), stored)
    assert found(crawler) == [("/doc.pdf", "new")]

    # A new edition.
    site.set("/doc.pdf", (200, second, PDF))
    ask(crawler, clock, url)
    row = crawler.store.get(url)
    change = crawler.store.changes()[-1]
    assert (change["change"], change["previous_content_sha256"], change["content_sha256"]) == (
        "changed", hashlib.sha256(first).hexdigest(), hashlib.sha256(second).hexdigest())
    assert row["sha256"] == hashlib.sha256(second).hexdigest() and len(crawler.store.editions(url)) == 2
    assert len(objects(crawler)) == len(stored) + 1
    held_copy = {key: row[key] for key in ("sha256", "content_sha256", "content_length", "http_status", "fetched_at")}

    # Refusals, limits, errors and silence never demote the copy held, and no retry state is entered.
    stored = objects(crawler)
    for answer in [(401, b"", {}), (403, b"Access Denied", {}), (429, b"", {"retry-after": "60"}), (451, b"", {}),
                   (500, b"", {}), (503, b"", {}), (400, b"", {}), "no answer", (200, SOFT_ERROR, PDF),
                   (200, b"Service unavailable", {"content-type": "text/plain"})]:
        site.set("/doc.pdf", answer)
        ask(crawler, clock, url)
        row = crawler.store.get(url)
        assert row["state"] == "fetched" and row["next_attempt_at"] is None, answer
        assert {key: row[key] for key in held_copy} == held_copy, answer
        assert "the copy held stays current" in row["error"] and row["next_check_at"] == asked(crawler, url, days=1)
    assert objects(crawler) == stored
    refused = [(row["reason"], row["sha256"]) for row in crawler.store.changes() if row["change"] == "rejected"]
    assert refused == [("soft_error", hashlib.sha256(SOFT_ERROR).hexdigest()),
                       ("type_mismatch", hashlib.sha256(b"Service unavailable").hexdigest())]

    # Gone: retired at once, once; still held until it is purged.
    site.set("/doc.pdf", (404, b"", {}))
    ask(crawler, clock, url)
    gone_at = parse_iso(asked(crawler, url))
    ask(crawler, clock, url)
    row = crawler.store.get(url)
    assert (row["state"], row["gone_since"], row["purge_after"]) == ("not_found", iso(gone_at),
                                                                      iso(gone_at + timedelta(days=30)))
    assert row["sha256"] == held_copy["sha256"] and [change for _, change in found(crawler)].count("gone") == 1
    site.set("/doc.pdf", (503, b"", {}))
    ask(crawler, clock, url)
    assert crawler.store.get(url)["state"] == "not_found"  # a failed check of a gone URL changes nothing

    # Restored: the same content answers again.
    site.set("/doc.pdf", (200, second, PDF))
    ask(crawler, clock, url)
    row = crawler.store.get(url)
    assert (row["state"], row["gone_since"], row["purge_after"], row["http_status"]) == ("fetched", None, None, 200)
    assert found(crawler)[-1] == ("/doc.pdf", "restored") and objects(crawler) == stored

    # Moved: the target is queued and fetched as a URL of its own, and a second redirect there is no news.
    site.set("/doc.pdf", (301, b"", {"location": "/files/doc-2026.pdf"}))
    site.set("/files/doc-2026.pdf", (200, second, PDF))
    ask(crawler, clock, url)
    ask(crawler, clock, url)
    row = crawler.store.get(url)
    assert (row["state"], row["location"], row["content_sha256"]) == ("redirect", f"{HOST}/files/doc-2026.pdf",
                                                                       held_copy["content_sha256"])
    moved = [change for change in crawler.store.changes() if change["change"] == "moved"]
    assert [change["target"] for change in moved] == [f"{HOST}/files/doc-2026.pdf"]
    assert found(crawler)[-2:] == [("/doc.pdf", "moved"), ("/files/doc-2026.pdf", "new")]
    site.set("/doc.pdf", (200, second, PDF))
    ask(crawler, clock, url)
    assert crawler.store.get(url)["state"] == "fetched"  # back at its address with the same content: no news
    assert found(crawler)[-1] == ("/files/doc-2026.pdf", "new")


def test_a_held_page_changes_only_when_its_text_does(daily):
    crawler, site, clock = daily
    url = f"{HOST}/hazcom"
    site.set("/hazcom", (200, drupal_page(MAIN), PAGE))
    crawler.add_known(url, source="sitemap", in_sitemap=True)
    crawler.crawl()
    first = crawler.store.get(url)
    assert first["content_sha256"] == page_hash(main_text(drupal_page(MAIN)))
    assert first["next_check_at"] == asked(crawler, url, days=30)
    stored = objects(crawler)

    # Every request's own values differ, and the header gained a link: no edition, no object, the link followed.
    site.set("/hazcom", (200, drupal_page(MAIN, view="beef01", build="form-Q", token="tok9", version="1794700000",
                                          menu='About OSHA</a><a href="/new-topic">New topic'), PAGE))
    site.set("/new-topic", (200, b"<html><body><main>New topic</main></body></html>", PAGE))
    ask(crawler, clock, url)
    row = crawler.store.get(url)
    assert (row["sha256"], row["content_sha256"]) == (first["sha256"], first["content_sha256"])
    assert row["last_checked_at"] == asked(crawler, url) and row["last_checked_at"] != first["last_checked_at"]
    assert objects(crawler) - stored == {crawler.store.get(f"{HOST}/new-topic")["sha256"]}
    assert found(crawler) == [("/hazcom", "new"), ("/new-topic", "new")]

    # An edit of the text is a new edition.
    site.set("/hazcom", (200, drupal_page(MAIN.replace("label", "mark")), PAGE))
    ask(crawler, clock, url)
    assert found(crawler)[-1] == ("/hazcom", "changed") and len(crawler.store.editions(url)) == 2


def test_a_held_url_queued_for_a_check_is_checked_whatever_rules_came_since(daily):
    crawler, site, clock = daily
    old = ("/grants/fy2016/page", "/index.php/news")  # downloaded before the rules that now cover them
    for path in (*old, "/grants/fy2017/page"):
        site.set(path, (200, b"<html><body><main>A page</main></body></html>", PAGE))
    for path in old:
        crawler.add_known(f"{HOST}{path}", source="sitemap", in_sitemap=True)
    crawler.crawl()
    crawler.site = example_site(name="daily", sitemaps=(), max_bytes=10**6, unneeded_paths=("/grants/",),
                                alias_prefixes=("/index.php",))
    crawler.store.insert_url(f"{HOST}/grants/fy2017/page", source="discovered", state="pending", now=FETCHED_AT)
    site.requests.clear()
    ask(crawler, clock, *(f"{HOST}{path}" for path in old))
    assert [request.url.path for request in site.requests if request.url.path != "/robots.txt"] == list(old)
    assert [crawler.store.get(f"{HOST}{path}")["state"] for path in old] == ["fetched", "fetched"]
    assert crawler.store.get(f"{HOST}/grants/fy2017/page")["skip_reason"] == "not_needed"  # never fetched: skipped


def test_a_first_download_keeps_its_retries_and_a_refused_body_is_never_kept(daily):
    crawler, site, clock = daily
    crawler.site = example_site(name="daily", sitemaps=(), max_bytes=1000)
    site.set("/flaky.pdf", (503, b"", {}), (200, pdf_bytes(300), PDF))
    site.set("/soft.pdf", (200, SOFT_ERROR, PDF))
    site.set("/big.pdf", (200, pdf_bytes(5000), PDF))
    for path in ("/flaky.pdf", "/soft.pdf", "/big.pdf"):
        crawler.add_known(f"{HOST}{path}", source="sitemap", in_sitemap=True)
    crawler.store.commit()
    assert crawler.crawl()["stopped"] == "done"
    flaky, soft, big = (crawler.store.get(f"{HOST}{path}") for path in ("/flaky.pdf", "/soft.pdf", "/big.pdf"))
    assert (flaky["state"], flaky["attempts"], flaky["detected_type"]) == ("fetched", 2, "pdf")
    assert (soft["state"], soft["attempts"], soft["sha256"], soft["content_sha256"]) == ("rejected", 3, None, None)
    assert soft["error"].startswith("rejected soft_error: an HTML page served as application/pdf")
    assert (big["state"], big["skip_reason"]) == ("skipped", "size_cap")
    assert sorted(found(crawler)) == [("/big.pdf", "rejected"), ("/flaky.pdf", "new"), ("/soft.pdf", "rejected")]
    assert {row["reason"] for row in crawler.store.changes() if row["change"] == "rejected"} == {"soft_error",
                                                                                                "too_large"}
    assert objects(crawler) == {flaky["sha256"]} and crawler.store.editions(f"{HOST}/soft.pdf") == []


def test_only_a_copy_held_sends_its_validators(daily):
    crawler, site, clock = daily
    site.set("/a.pdf", (200, pdf_bytes(300), {**PDF, "etag": '"v1"'}))
    crawler.add_known(f"{HOST}/a.pdf", source="sitemap", in_sitemap=True)
    crawler.add_known(f"{HOST}/b.pdf", source="sitemap", in_sitemap=True)
    crawler.store.update(f"{HOST}/b.pdf", state="rejected", etag='"x"', detected_type="html")  # refused, no copy
    crawler.store.commit()
    crawler.crawl()
    site.requests.clear()
    site.set("/a.pdf", (304, b"", {}))
    ask(crawler, clock, f"{HOST}/a.pdf", f"{HOST}/b.pdf")
    sent = {request.url.path: request.headers.get("if-none-match") for request in site.requests}
    assert sent == {"/robots.txt": None, "/a.pdf": '"v1"', "/b.pdf": None}
    assert crawler.store.get(f"{HOST}/a.pdf")["state"] == "unchanged"
    # A 304 for a gone URL means the copy held is back.
    site.set("/a.pdf", (410, b"", {}))
    ask(crawler, clock, f"{HOST}/a.pdf")
    site.set("/a.pdf", (304, b"", {}))
    ask(crawler, clock, f"{HOST}/a.pdf")
    row = crawler.store.get(f"{HOST}/a.pdf")
    assert (row["state"], row["gone_since"]) == ("unchanged", None)
    assert found(crawler)[-2:] == [("/a.pdf", "gone"), ("/a.pdf", "restored")]


def test_every_crawl_and_fetch_is_a_run_that_its_changes_name(daily):
    crawler, site, clock = daily
    site.set("/a.pdf", (200, pdf_bytes(300), PDF))
    site.set("/b.pdf", (200, pdf_bytes(310), PDF))
    crawler.add_known(f"{HOST}/a.pdf", source="sitemap", in_sitemap=True)
    result = crawler.crawl()
    crawler.fetch_now([f"{HOST}/b.pdf"], max_bytes=10_000, max_seconds=60)
    runs = crawler.store.db.execute("select * from runs order by started_at").fetchall()
    assert [(run["site"], run["stop_reason"], run["requests"]) for run in runs] == [("daily", "done", 2),
                                                                                    ("daily", "done", 1)]
    assert runs[0]["run_id"] == result["run_id"] and all(run["ended_at"] for run in runs)
    assert [change["run_id"] for change in crawler.store.changes()] == [runs[0]["run_id"], runs[1]["run_id"]]

    def broken(request):
        raise RuntimeError("the disk is full")

    crawler.client = httpx.Client(transport=httpx.MockTransport(broken))
    crawler.add_known(f"{HOST}/c.pdf", source="sitemap", in_sitemap=True)
    with pytest.raises(RuntimeError):
        crawler.crawl()
    last = crawler.store.db.execute("select * from runs order by started_at desc limit 1").fetchone()
    assert last["stop_reason"] == "interrupted" and crawler.run_id is None
    crawler.client.close()


def test_the_same_bytes_again_bring_their_current_validators(daily):
    crawler, site, clock = daily
    url, body = f"{HOST}/doc.pdf", pdf_bytes(300)
    site.set("/doc.pdf", (200, body, {**PDF, "etag": '"v1"'}))
    crawler.add_known(url, source="sitemap", in_sitemap=True)
    crawler.crawl()
    site.set("/doc.pdf", (200, body, {**PDF, "etag": '"v2"', "last-modified": "Mon, 23 Nov 2026 10:00:00 GMT"}))
    ask(crawler, clock, url)  # put back unchanged: a new ETag for the same bytes
    row = crawler.store.get(url)
    assert (row["etag"], row["last_modified"], row["sha256"]) == ('"v2"', "Mon, 23 Nov 2026 10:00:00 GMT",
                                                                  hashlib.sha256(body).hexdigest())
    site.requests.clear()
    site.set("/doc.pdf", (304, b"", {}))
    ask(crawler, clock, url)
    assert [request.headers.get("if-none-match") for request in site.requests if request.url.path == "/doc.pdf"] == [
        '"v2"']
    assert crawler.store.get(url)["state"] == "unchanged" and found(crawler) == [("/doc.pdf", "new")]


def test_a_held_url_robots_txt_now_disallows_keeps_its_copy(daily):
    crawler, site, clock = daily
    url = f"{HOST}/doc.pdf"
    site.set("/doc.pdf", (200, pdf_bytes(300), PDF))
    crawler.add_known(url, source="sitemap", in_sitemap=True)
    crawler.crawl()
    held_copy = dict(crawler.store.get(url))
    site.set("/robots.txt", (200, b"User-agent: *\nDisallow: /doc.pdf\nDisallow: /new.pdf\n", {}))
    crawler.robots = None  # read again, as it is once a day
    crawler.add_known(f"{HOST}/new.pdf", source="sitemap", in_sitemap=True)
    site.requests.clear()
    ask(crawler, clock, url)
    row = crawler.store.get(url)
    assert [request.url.path for request in site.requests] == ["/robots.txt"]  # never asked
    assert (row["state"], row["sha256"], row["content_sha256"], row["gone_since"]) == (
        "fetched", held_copy["sha256"], held_copy["content_sha256"], None)
    assert "disallowed by robots.txt" in row["error"] and row["next_check_at"] == iso(clock.now + timedelta(days=1))
    assert (crawler.store.get(f"{HOST}/new.pdf")["state"], crawler.store.get(f"{HOST}/new.pdf")["skip_reason"]) == (
        "skipped", "robots")  # a URL not held is skipped, as before
    [result] = crawler.fetch_now([url], max_bytes=10_000, max_seconds=60)
    assert (result["state"], result["reason"]) == ("fetched", "robots") and found(crawler) == [("/doc.pdf", "new")]


def test_a_time_limit_is_a_deadline_for_every_wait(daily):
    crawler, site, clock = daily
    url = f"{HOST}/doc.pdf"
    site.set("/doc.pdf", (200, pdf_bytes(300), PDF))
    crawler.add_known(url, source="sitemap", in_sitemap=True)
    crawler.crawl()
    # A Retry-After of an hour near the deadline: the crawl waits until the deadline, not past it, and stops.
    site.set("/doc.pdf", (429, b"", {"retry-after": "3600"}))
    clock.now += timedelta(days=1)
    crawler.store.update(url, state="pending", attempts=0, next_attempt_at=None)
    until = clock.now + timedelta(minutes=5)
    assert crawler.crawl(until=until)["stopped"] == "time limit" and clock.now == until
    # robots.txt that cannot be read: asked again every 10 minutes until the deadline, then the crawl stops and says
    # why, without asking for anything else.
    site.set("/robots.txt", (503, b"", {}))
    crawler.robots = None
    crawler.store.update(url, state="pending", attempts=0, next_attempt_at=None)
    site.requests.clear()
    until = clock.now + timedelta(hours=1)
    result = crawler.crawl(until=until)
    assert (result["stopped"], clock.now) == ("robots.txt unreachable", until)
    assert [request.url.path for request in site.requests] == ["/robots.txt"] * 6
    assert crawler.store.get_run(result["run_id"])["stop_reason"] == "robots.txt unreachable"
    assert crawler.store.get(url)["state"] == "pending"  # asked on the next run


def test_an_interrupted_run_keeps_no_answer_half_recorded(daily, monkeypatch):
    crawler, site, clock = daily
    url = f"{HOST}/doc.pdf"
    site.set("/doc.pdf", (200, pdf_bytes(300), PDF))
    crawler.add_known(url, source="sitemap", in_sitemap=True)
    crawler.store.commit()

    def interrupted(*args, **kwargs):
        raise KeyboardInterrupt  # as a cancelled flow is stopped, between the row's update and its change row

    monkeypatch.setattr(crawler.store, "add_change", interrupted)
    with pytest.raises(KeyboardInterrupt):
        crawler.crawl()
    crawler.store.close()  # as the command does on its way out, committing whatever is left
    crawler.store = Store(crawler.store.path)
    row = crawler.store.get(url)
    assert (row["state"], row["sha256"], row["content_sha256"]) == ("pending", None, None)
    assert crawler.store.editions(url) == [] and crawler.store.changes() == []
    assert crawler.store.db.execute("select stop_reason from runs").fetchone()[0] == "interrupted"
    crawler.crawl()  # asked again, and recorded whole
    assert found(crawler) == [("/doc.pdf", "new")] and len(crawler.store.editions(url)) == 1


def test_a_copy_whose_object_is_not_here_is_examined_from_the_same_bytes(daily):
    """On the VM the raw archive is not kept: a copy recorded before content hashes is examined from an answer with
    the very same bytes, and nothing else is taken as its baseline."""
    crawler, site, clock = daily
    page, report = drupal_page(MAIN), pdf_bytes(300)
    copies = [held(f"{HOST}/hazcom", page, "text/html; charset=utf-8", etag='"p1"'),
              held(f"{HOST}/report.pdf", report, "application/pdf", etag='"r1"'),
              held(f"{HOST}/fr/1997-02-07.pdf", SOFT_ERROR, "application/pdf", etag='"s1"'),
              held(f"{HOST}/old-page", b"<html><body><main>Old text</main></body></html>", "text/html", etag='"o1"')]
    for copy in copies:  # rows as the first crawls wrote them, without their objects
        crawler.store.insert_url(copy["url"], source="sitemap", state="pending", now=FETCHED_AT, in_sitemap=True)
        crawler.store.update(copy["url"], **{key: copy[key] for key in ("http_status", "content_type", "content_length",
                                                                         "sha256", "fetched_at", "etag")})
    crawler.store.commit()
    site.set("/hazcom", (200, page, PAGE))
    site.set("/report.pdf", (200, report, PDF))
    site.set("/fr/1997-02-07.pdf", (200, SOFT_ERROR, PDF))
    site.set("/old-page", (200, b"<html><body><main>New text</main></body></html>", PAGE))
    crawler.crawl()
    assert all(request.headers.get("if-none-match") is None for request in site.requests)  # asked whole, once
    hazcom, report_row = crawler.store.get(f"{HOST}/hazcom"), crawler.store.get(f"{HOST}/report.pdf")
    assert (hazcom["detected_type"], hazcom["content_sha256"]) == ("html", page_hash(main_text(page)))
    assert (report_row["detected_type"], report_row["content_sha256"]) == ("pdf", hashlib.sha256(report).hexdigest())
    assert [edition["first_seen_at"] for edition in crawler.store.editions(f"{HOST}/hazcom")] == [FETCHED_AT]
    soft = crawler.store.get(f"{HOST}/fr/1997-02-07.pdf")
    assert (soft["state"], soft["content_sha256"]) == ("rejected", None) and not crawler.store.editions(soft["url"])
    # Other bytes cannot be compared with a copy never examined: a change, said to be one.
    assert sorted(found(crawler)) == [("/fr/1997-02-07.pdf", "rejected"), ("/old-page", "changed")]
    [changed] = [row for row in crawler.store.changes() if row["change"] == "changed"]
    assert (changed["previous_content_sha256"], changed["note"]) == (None, "the copy held before had no content hash")
    old = crawler.store.get(f"{HOST}/old-page")
    assert objects(crawler) == {old["sha256"]}  # only the new edition is stored: the same bytes are not kept again


def test_a_copy_recorded_before_content_hashes_is_examined_before_its_validators_are_sent(tmp_path):
    clock, asked = Clock(), []

    def handler(request):  # a server that confirms the copy held when asked conditionally
        asked.append(request)
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        if request.headers.get("if-none-match") == '"s1"':
            return httpx.Response(304)
        return httpx.Response(200, content=SOFT_ERROR, headers=PDF)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    crawler = Crawler(example_site(name="daily", sitemaps=()), tmp_path, client, sleep=clock.sleep,
                      now=lambda: clock.now)
    copy = held(f"{HOST}/fr/1997-02-07.pdf", SOFT_ERROR, "application/pdf", archive=crawler.objects.root, etag='"s1"')
    crawler.store.insert_url(copy["url"], source="sitemap", state="pending", now=FETCHED_AT, in_sitemap=True)
    crawler.store.update(copy["url"], **{key: copy[key] for key in ("http_status", "content_type", "content_length",
                                                                     "sha256", "fetched_at", "etag")})
    crawler.store.commit()
    crawler.crawl()
    soft = crawler.store.get(copy["url"])
    assert all("if-none-match" not in request.headers for request in asked)  # a soft error is not a copy to confirm
    assert (soft["state"], soft["content_sha256"], soft["detected_type"]) == ("rejected", None, "html")
    crawler.store.close()
    client.close()


def test_a_copy_recorded_before_content_hashes_is_examined_when_asked_again(daily):
    crawler, site, clock = daily
    body = drupal_page(MAIN)
    row = held(f"{HOST}/hazcom", body, "text/html; charset=utf-8", archive=crawler.objects.root, etag='"1"')
    crawler.store.insert_url(row["url"], source="sitemap", state="pending", now=FETCHED_AT, in_sitemap=True)
    crawler.store.update(row["url"], **{key: row[key] for key in ("http_status", "content_type", "content_length",
                                                                   "sha256", "fetched_at", "etag")})
    crawler.store.commit()
    site.set("/hazcom", (200, drupal_page(MAIN, view="feed42", version="1794800000"), PAGE))
    crawler.crawl()
    row = crawler.store.get(f"{HOST}/hazcom")
    assert row["content_sha256"] == page_hash(main_text(body)) and row["sha256"] == hashlib.sha256(body).hexdigest()
    assert [edition["first_seen_at"] for edition in crawler.store.editions(row["url"])] == [FETCHED_AT]
    assert found(crawler) == []  # the same text: no change


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
