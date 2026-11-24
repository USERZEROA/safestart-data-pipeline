from __future__ import annotations

import codecs
import gzip
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from pathlib import Path

import httpx
import pytest

from safestart_pipeline.config import USER_AGENT
from safestart_pipeline.crawl.locks import SiteLock
from safestart_pipeline.crawl.sites import Site
from safestart_pipeline.crawl.store import Store
from safestart_pipeline.stream.events import Event
from safestart_pipeline.watch import sitemaps
from safestart_pipeline.watch.checks import Check
from safestart_pipeline.watch.publish import ListPublisher, publish_new
from safestart_pipeline.watch.sitemaps import check_sitemaps, modified_after
from safestart_pipeline.watch.state import WatchState

HOST = "https://www.example.test"
NOW = datetime(2026, 11, 23, 18, 0, tzinfo=UTC)
ROBOTS = "User-agent: *\nCrawl-delay: 10\nDisallow: /private/\n"
INDEX = f"""<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>{HOST}/pages-1.xml</loc></sitemap>
  <sitemap><loc>{HOST}/pages-2.xml</loc></sitemap>
</sitemapindex>"""
# /a was edited before its fetch and /b after it, once both times are in UTC; /d's bare date is the day of its fetch.
PAGES_1 = f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>{HOST}/a</loc><lastmod>2026-11-20T09:00:00+05:00</lastmod></url>
  <url><loc>{HOST}/b</loc><lastmod>2026-11-20T09:00:00-05:00</lastmod></url>
  <url><loc>{HOST}/d</loc><lastmod>2026-11-20</lastmod></url>
</urlset>"""
PAGES_2 = f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>{HOST}/new</loc><lastmod>2026-11-21T10:00:00Z</lastmod></url>
  <url><loc>{HOST}/linked</loc></url>
  <url><loc>https://other.test/elsewhere</loc></url>
</urlset>"""
# A page served in a sitemap's place. Its unclosed <meta> makes it malformed XML, as is the page MSHA's site serves the
# cloud VM ("Opening and ending tag mismatch: meta line 5 and head").
PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<title>Request blocked</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
</head>
<body><p>This request was blocked.</p></body>
</html>"""
INVENTORY = {  # url -> (listed in a sitemap, sitemap lastmod then, fetched at)
    "/a": (True, "2026-11-01", "2026-11-20T06:00:00Z"),
    "/b": (True, "2026-11-01", "2026-11-20T12:00:00Z"),
    "/c": (True, "2026-10-01T00:00:00Z", "2026-11-18T00:00:00Z"),
    "/d": (True, "2026-11-20", "2026-11-20T08:00:00Z"),
    "/linked": (False, None, "2026-11-19T00:00:00Z"),
}
SITE = Site(name="example", canonical_host="www.example.test", hosts=frozenset({"www.example.test"}),
            sitemaps=(f"{HOST}/sitemap.xml",), min_delay=1.0, byte_budget=10**9)


class Clock:
    def __init__(self):
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class FakeSite:
    def __init__(self, clock: Clock):
        self.clock = clock
        self.requests: list[tuple[str, float]] = []
        self.statuses: dict[str, int] = {}  # path -> the status it answers with instead of its body
        self.types: dict[str, str] = {}  # path -> the Content-Type its body is served with, when not XML
        self.bodies: dict[str, str | bytes] = {"/sitemap-index.xml": INDEX, "/pages-1.xml": PAGES_1,
                                               "/pages-2.xml": PAGES_2}

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.requests.append((path, self.clock.now))
        if path in self.statuses:
            return httpx.Response(self.statuses[path])
        if path == "/robots.txt":
            return httpx.Response(200, text=ROBOTS)
        if path == "/sitemap.xml":
            return httpx.Response(301, headers={"location": "/sitemap-index.xml"})
        if path in self.bodies:
            body = self.bodies[path]
            return httpx.Response(200, content=body.encode() if isinstance(body, str) else body,
                                  headers={"content-type": self.types.get(path, "application/xml")})
        return httpx.Response(404)

    def paths(self) -> list[str]:
        return [path for path, _ in self.requests]


@dataclass
class Bench:
    root: Path
    state: WatchState
    fake: FakeSite
    clock: Clock
    client: httpx.Client

    def check(self, publisher: ListPublisher | None = None, now: datetime = NOW) -> Check:
        return check_sitemaps(SITE, self.root, self.state, publisher or ListPublisher(), self.client,
                              sleep=self.clock.sleep, clock=self.clock.monotonic, now=lambda: now)


def by_id(publisher: ListPublisher) -> dict:
    return {event.event_id: event for event in publisher.events}


@pytest.fixture
def bench(tmp_path):
    inventory = Store(tmp_path / "state" / "crawl-example.sqlite")
    for path, (listed, lastmod, fetched_at) in INVENTORY.items():
        inventory.insert_url(HOST + path, source="sitemap" if listed else "discovered", state="pending",
                             now="2026-11-17T00:00:00Z", in_sitemap=listed, lastmod=lastmod)
        inventory.update(HOST + path, state="fetched", fetched_at=fetched_at, http_status=200)
    inventory.close()
    clock = Clock()
    fake = FakeSite(clock)
    client = httpx.Client(transport=httpx.MockTransport(fake.handler), headers={"User-Agent": USER_AGENT})
    bench = Bench(tmp_path, WatchState(tmp_path / "state" / "watch.sqlite"), fake, clock, client)
    yield bench
    bench.state.close()
    client.close()


def test_pages_added_changed_or_dropped_since_the_crawl_are_published_once(bench):
    publisher = ListPublisher()
    result = bench.check(publisher)

    events = {event.event_id: event for event in publisher.events}
    assert sorted(events) == [
        f"page_change:{HOST}/b|changed|2026-11-20T09:00:00-05:00",
        f"page_change:{HOST}/c|dropped|2026-10-01T00:00:00Z",
        f"page_change:{HOST}/new|added|2026-11-21T10:00:00Z",
    ]
    changed = events[f"page_change:{HOST}/b|changed|2026-11-20T09:00:00-05:00"]
    assert changed.details == {"site": "example", "url": f"{HOST}/b", "lastmod": "2026-11-20T09:00:00-05:00",
                               "change": "changed"}
    assert changed.occurred_at == "2026-11-20T14:00:00Z"
    assert changed.source == {"site": "example", "sitemap": f"{HOST}/pages-1.xml"}
    assert changed.title == f"EXAMPLE page changed: {HOST}/b" and changed.topic == "safety.sources"
    dropped = events[f"page_change:{HOST}/c|dropped|2026-10-01T00:00:00Z"]
    assert dropped.occurred_at == "2026-11-23T18:00:00Z" and dropped.source == {"site": "example"}
    assert (result.status, result.found) == ("ok", 3)
    assert result.note == "5 URLs listed; 1 added, 1 changed, 1 dropped"
    # robots.txt first, the redirect followed as its own request, and every request 10 s (the Crawl-delay) apart.
    assert bench.fake.paths() == ["/robots.txt", "/sitemap.xml", "/sitemap-index.xml", "/pages-1.xml", "/pages-2.xml"]
    times = [moment for _, moment in bench.fake.requests]
    assert min(later - earlier for earlier, later in pairwise(times)) >= 10.0

    again = bench.check(now=NOW + timedelta(hours=6))
    assert (again.status, again.found) == ("unchanged", 0)


def test_dropped_pages_are_not_judged_when_a_sitemap_cannot_be_read(bench):
    bench.fake.statuses["/pages-2.xml"] = 500
    publisher = ListPublisher()
    result = bench.check(publisher)
    assert [event.event_id for event in publisher.events] == [f"page_change:{HOST}/b|changed|2026-11-20T09:00:00-05:00"]
    assert result.status == "error"
    assert result.note == ("3 URLs listed; 0 added, 1 changed, dropped pages not judged; 1 sitemap not read: "
                           f"{HOST}/pages-2.xml: HTTP 500")


def test_a_damaged_gzip_sitemap_is_one_unreadable_sitemap_and_the_others_are_still_read(bench):
    bench.fake.bodies["/pages-1.xml"] = gzip.compress(PAGES_1.encode())[:-10]  # truncated
    publisher = ListPublisher()
    result = bench.check(publisher)
    assert [event.event_id for event in publisher.events] == [f"page_change:{HOST}/new|added|2026-11-21T10:00:00Z"]
    assert result.status == "error" and "pages-1.xml: not a readable gzip sitemap" in result.note


def test_a_malformed_sitemap_is_an_error_not_a_refusal(bench):
    bench.fake.bodies["/pages-1.xml"] = PAGES_1.removesuffix("</urlset>")  # cut short
    result = bench.check()
    assert result.status == "error" and f"{HOST}/pages-1.xml: not well-formed XML" in result.note
    assert "refused" not in result.note


@pytest.mark.parametrize("body, content_type", [
    (PAGE, "text/html; charset=UTF-8"),
    (codecs.BOM_UTF8 + b"\r\n  " + PAGE.encode(), "application/xml"),  # an HTML page by its first bytes alone
    ("<HTML><BODY>Access denied</BODY></HTML>", "text/xml"),  # well-formed, but not a sitemap
    ("<!-- edge -->\n<p>Request blocked.</p>", "text/html"),  # an HTML page by its type alone
])
def test_an_html_page_served_for_a_sitemap_is_a_refusal_and_not_asked_again(bench, body, content_type):
    bench.fake.bodies["/sitemap-index.xml"] = body
    bench.fake.types["/sitemap-index.xml"] = content_type
    publisher = ListPublisher()
    result = bench.check(publisher)
    assert (result.status, result.found, publisher.events) == ("refused", 0, [])
    assert result.note == ("0 URLs listed; 0 added, 0 changed, dropped pages not judged; 1 sitemap not read: "
                           f"{HOST}/sitemap.xml: refused, the host served an HTML page instead of the sitemap")
    assert bench.fake.paths() == ["/robots.txt", "/sitemap.xml", "/sitemap-index.xml"]


@pytest.mark.parametrize("status, reason", [(401, "Unauthorized"), (403, "Forbidden"), (429, "Too Many Requests"),
                                            (451, "Unavailable For Legal Reasons")])
def test_a_refused_sitemap_beside_a_readable_one_leaves_a_partial_listing(bench, status, reason):
    bench.fake.statuses["/pages-2.xml"] = status
    publisher = ListPublisher()
    result = bench.check(publisher)
    # /b changed in the sitemap that was read; /c, which the refused one may still list, is not judged dropped.
    assert [event.event_id for event in publisher.events] == [f"page_change:{HOST}/b|changed|2026-11-20T09:00:00-05:00"]
    assert (result.status, result.found) == ("refused", 1)
    assert result.note == ("3 URLs listed; 0 added, 1 changed, dropped pages not judged; 1 sitemap not read: "
                           f"{HOST}/pages-2.xml: refused, the host answered HTTP {status} ({reason})")
    assert bench.fake.paths().count("/pages-2.xml") == 1


def test_a_sitemap_that_cannot_be_read_makes_an_error_of_a_check_with_refusals_too(bench):
    bench.fake.statuses |= {"/pages-1.xml": 403, "/pages-2.xml": 503}
    result = bench.check()
    assert result.status == "error"
    assert result.note == ("0 URLs listed; 0 added, 0 changed, dropped pages not judged; 2 sitemaps not read: "
                           f"{HOST}/pages-2.xml: HTTP 503; "
                           f"{HOST}/pages-1.xml: refused, the host answered HTTP 403 (Forbidden)")


def test_a_sitemap_is_read_whatever_type_it_is_served_as(bench):
    bench.fake.types["/pages-1.xml"] = "text/html"
    result = bench.check()
    assert (result.status, result.note) == ("ok", "5 URLs listed; 1 added, 1 changed, 1 dropped")


def test_a_page_the_crawl_never_recorded_is_added_once_and_changed_after(bench):
    """The inventory is only read, so /new stays unknown to it; a later lastmod of it is an edit, not a new page."""
    publisher = ListPublisher()
    bench.check(publisher)
    assert f"page_change:{HOST}/new|added|2026-11-21T10:00:00Z" in by_id(publisher)
    assert bench.check(publisher).found == 0  # listed again as it was: nothing new

    bench.fake.bodies["/pages-2.xml"] = PAGES_2.replace("2026-11-21T10:00:00Z", "2026-11-27T08:00:00Z")
    edited = bench.check(publisher)
    assert (edited.found, edited.note) == (1, "5 URLs listed; 0 added, 2 changed, 1 dropped")  # /b and /c as before
    event = publisher.events[-1]
    assert event.event_id == f"page_change:{HOST}/new|changed|2026-11-27T08:00:00Z"
    assert (event.title, event.details["change"]) == (f"EXAMPLE page changed: {HOST}/new", "changed")
    assert bench.check(publisher).found == 0


def test_a_site_is_not_asked_while_its_crawl_runs_or_without_an_inventory(bench):
    with SiteLock(bench.root / "state" / "crawl-example.lock"):
        result = bench.check()
    assert result.status == "skipped" and "crawl is running" in result.note and bench.fake.requests == []
    assert result.retry_in == sitemaps.BUSY_RETRY  # asked again in an hour, not a whole interval later
    assert bench.check().status == "ok"  # the skipped check did not count as a read

    (bench.root / "state" / "crawl-example.sqlite").unlink()
    assert bench.check(now=NOW + timedelta(hours=6)).note == "no example crawl inventory to compare with"


def test_a_page_the_inventory_recorded_after_the_last_read_is_added_all_the_same(bench):
    """The daily run of the official files reads the sitemap too, and records a new page before the watcher sees it."""
    publisher = ListPublisher()
    assert bench.check(publisher).status == "ok"  # the first read, at NOW
    inventory = Store(bench.root / "state" / "crawl-example.sqlite")
    inventory.insert_url(f"{HOST}/fresh", source="sitemap", state="fetched", now="2026-11-24T17:00:00Z",
                         in_sitemap=True)
    inventory.close()
    bench.fake.bodies["/pages-2.xml"] = PAGES_2.replace("</urlset>", f"<url><loc>{HOST}/fresh</loc></url></urlset>")
    assert bench.check(publisher, now=NOW + timedelta(days=1)).found == 1
    assert publisher.events[-1].event_id == f"page_change:{HOST}/fresh|added|"
    assert not any(event.event_id.startswith(f"page_change:{HOST}/linked|") for event in publisher.events)


def test_a_page_the_official_feeds_check_announced_as_new_is_not_added_again(bench):
    """The daily run downloaded /new after reading the sitemap, and the feed's check announced it: one page, one
    event. A later edit of it is still announced."""
    announced_new = Event.new(kind="page_change", key=f"official|{HOST}/new|new|{'a' * 64}|2026-11-23T17:00:00Z",
                              mode="live", occurred_at="2026-11-23T17:00:00Z", title=f"New EXAMPLE page: {HOST}/new")
    publish_new([announced_new], ListPublisher(), bench.state)
    publisher = ListPublisher()
    result = bench.check(publisher)
    assert sorted(by_id(publisher)) == [f"page_change:{HOST}/b|changed|2026-11-20T09:00:00-05:00",
                                        f"page_change:{HOST}/c|dropped|2026-10-01T00:00:00Z"]
    assert result.note == ("5 URLs listed; 0 added, 1 changed, 1 dropped; 1 added page left out, announced as new by"
                           " the official feed's check already")


def touch(path: Path, moment: datetime) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()
    os.utime(path, (moment.timestamp(), moment.timestamp()))


@pytest.fixture
def read_only_state(bench, monkeypatch):
    """The data root as the containers mount it: its state folder cannot be written, and a crawl on the host may be
    writing it all the same (its lock does not reach into a container). The inventory was last written an hour ago."""
    state, access = bench.root / "state", os.access
    monkeypatch.setattr(sitemaps.os, "access",
                        lambda path, mode, *args, **kwargs: (Path(path) != state or mode != os.W_OK)
                        and access(path, mode, *args, **kwargs))
    touch(state / "crawl-example.sqlite", NOW - timedelta(hours=1))
    return state


@pytest.mark.parametrize("written", ["state/crawl-example.sqlite", "state/crawl-example.sqlite-wal",
                                     "raw/example/objects/tmp/0f3e9a"])
def test_where_the_lock_cannot_be_seen_a_crawl_still_writing_keeps_its_site_from_being_asked(bench, read_only_state,
                                                                                         written):
    """The inventory's -wal changes at every commit, and a long download grows its temporary file the whole time."""
    touch(bench.root / written, NOW - sitemaps.CRAWL_QUIET + timedelta(minutes=1))
    result = bench.check()
    assert result.status == "skipped" and "crawl is running" in result.note and Path(written).name in result.note
    assert bench.fake.requests == [] and not (read_only_state / "crawl-example.lock").exists()


def test_where_the_lock_cannot_be_seen_a_quiet_crawl_lets_its_site_be_asked(bench, read_only_state):
    touch(read_only_state / "crawl-example.sqlite-wal", NOW - sitemaps.CRAWL_QUIET - timedelta(minutes=1))
    touch(bench.root / "raw" / "example" / "objects" / "tmp" / "left-by-a-stopped-crawl", NOW - timedelta(days=2))
    assert bench.check().status == "ok"
    assert not (read_only_state / "crawl-example.lock").exists()  # no lock file is made on a read-only mount


def test_a_site_with_a_minimum_interval_is_read_at_most_that_often(bench, monkeypatch):
    assert sitemaps.MIN_INTERVAL["osha"] == timedelta(days=1)  # OSHA asks for 10 s between requests
    monkeypatch.setitem(sitemaps.MIN_INTERVAL, "example", timedelta(days=1))
    assert bench.check().status == "ok"
    requests = len(bench.fake.requests)
    early = bench.check(now=NOW + timedelta(hours=23))
    assert early.status == "skipped" and "at most once every 1 day" in early.note
    assert len(bench.fake.requests) == requests
    assert bench.check(now=NOW + timedelta(hours=25)).status == "unchanged"


@pytest.mark.parametrize("lastmod, fetched_at, after", [
    ("2026-11-20T09:00:00-05:00", "2026-11-20T12:00:00Z", True),
    ("2026-11-20T09:00:00+05:00", "2026-11-20T06:00:00Z", False),
    ("2026-11-20T13:00:00", "2026-11-20T12:00:00Z", True),  # no offset: UTC
    ("2026-11-21", "2026-11-20T23:59:59Z", True),
    ("2026-11-20", "2026-11-20T08:00:00Z", False),
    ("2026-11", "2026-10-01T00:00:00Z", False),  # too coarse to judge
    (None, "2026-11-20T08:00:00Z", False),
    ("2026-11-20", None, False),
])
def test_a_lastmod_is_compared_with_the_fetch_in_utc(lastmod, fetched_at, after):
    assert modified_after(lastmod, fetched_at) is after
