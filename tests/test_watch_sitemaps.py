from __future__ import annotations

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
from safestart_pipeline.watch import sitemaps
from safestart_pipeline.watch.checks import Check
from safestart_pipeline.watch.publish import ListPublisher
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
        self.broken: set[str] = set()

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.requests.append((path, self.clock.now))
        if path in self.broken:
            return httpx.Response(500)
        if path == "/robots.txt":
            return httpx.Response(200, text=ROBOTS)
        if path == "/sitemap.xml":
            return httpx.Response(301, headers={"location": "/sitemap-index.xml"})
        bodies = {"/sitemap-index.xml": INDEX, "/pages-1.xml": PAGES_1, "/pages-2.xml": PAGES_2}
        if path in bodies:
            return httpx.Response(200, text=bodies[path], headers={"content-type": "application/xml"})
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
    bench.fake.broken.add("/pages-2.xml")
    publisher = ListPublisher()
    result = bench.check(publisher)
    assert [event.event_id for event in publisher.events] == [f"page_change:{HOST}/b|changed|2026-11-20T09:00:00-05:00"]
    assert result.status == "error" and "dropped pages not judged" in result.note and "HTTP 500" in result.note


def test_a_site_is_not_asked_while_its_crawl_runs_or_without_an_inventory(bench):
    with SiteLock(bench.root / "state" / "crawl-example.lock"):
        result = bench.check()
    assert result.status == "skipped" and "crawl is running" in result.note and bench.fake.requests == []
    assert bench.check().status == "ok"  # the skipped check did not count as a read

    (bench.root / "state" / "crawl-example.sqlite").unlink()
    assert bench.check(now=NOW + timedelta(hours=6)).note == "no example crawl inventory to compare with"


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
