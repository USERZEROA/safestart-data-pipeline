from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest

from safestart_pipeline.config import USER_AGENT
from safestart_pipeline.crawl.crawler import BLOCK_PAUSE_SECONDS, MAX_BLOCK_PAUSES, Crawler
from safestart_pipeline.crawl.locks import SiteLock
from safestart_pipeline.crawl.sites import Site
from safestart_pipeline.crawl.store import FINAL_STATES

HOST = "https://www.example.test"
ROBOTS = """User-agent: *
Crawl-delay: 10
Disallow: /private/
Disallow: /*?
"""
INDEX = """<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>https://www.example.test/sitemap-pages.xml</loc></sitemap>
</sitemapindex>"""
PAGES = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://www.example.test/</loc></url>
  <url><loc>https://www.example.test/a</loc><lastmod>2026-11-01</lastmod></url>
  <url><loc>https://www.example.test/big.pdf</loc></url>
  <url><loc>https://www.example.test/gone</loc></url>
  <url><loc>https://www.example.test/flaky</loc></url>
  <url><loc>https://www.example.test/moved</loc></url>
  <url><loc>https://other.test/not-ours</loc></url>
</urlset>"""
HOME = """<html><body>
<a href="/a">A</a> <a href="/b">B</a> <a href="/private/x">private</a> <a href="/logo.png">logo</a>
<a href="/search?q=1">search</a> <a href="https://other.test/x">elsewhere</a> <a href="http://example.test/c">C</a>
</body></html>"""
HTML = {"content-type": "text/html; charset=utf-8"}


class FakeSite:
    def __init__(self):
        self.requests: list[httpx.Request] = []
        self.flaky_calls = 0

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        if request.url.host != "www.example.test":
            return httpx.Response(500)
        if path == "/robots.txt":
            return httpx.Response(200, text=ROBOTS)
        if path == "/sitemap.xml":
            return httpx.Response(200, text=INDEX, headers={"content-type": "application/xml"})
        if path == "/sitemap-pages.xml":
            return httpx.Response(200, text=PAGES, headers={"content-type": "application/xml"})
        if path == "/":
            return httpx.Response(200, text=HOME, headers=HTML)
        if path == "/a":
            if request.headers.get("if-none-match") == '"a1"':
                return httpx.Response(304, headers={"etag": '"a1"'})
            return httpx.Response(200, text='<html><body><a href="/">home</a></body></html>', headers={**HTML, "etag": '"a1"'})
        if path == "/b":
            return httpx.Response(200, text="<html><body>B</body></html>", headers=HTML)
        if path == "/c":
            return httpx.Response(200, text="plain text", headers={"content-type": "text/plain"})
        if path == "/big.pdf":
            return httpx.Response(200, content=b"%PDF" + b"0" * 5000, headers={"content-type": "application/pdf"})
        if path == "/gone":
            return httpx.Response(404, text="missing")
        if path == "/flaky":
            self.flaky_calls += 1
            if self.flaky_calls == 1:
                return httpx.Response(503, text="busy")
            return httpx.Response(200, text="ok", headers={"content-type": "text/plain"})
        if path == "/moved":
            return httpx.Response(301, headers={"location": "/b"})
        return httpx.Response(404)


class Clock:
    def __init__(self):
        self.now = datetime(2026, 11, 17, 12, 0, tzinfo=UTC)
        self.sleeps: list[float] = []

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += timedelta(seconds=seconds)


def example_site(**overrides) -> Site:
    values = dict(
        name="example",
        canonical_host="www.example.test",
        hosts=frozenset({"www.example.test", "example.test"}),
        sitemaps=(f"{HOST}/sitemap.xml",),
        min_delay=1.0,
        byte_budget=10**9,
        max_bytes=1000,
    )
    values.update(overrides)
    return Site(**values)


@pytest.fixture
def setup(tmp_path):
    fake, clock = FakeSite(), Clock()
    client = httpx.Client(transport=httpx.MockTransport(fake.handler), headers={"User-Agent": USER_AGENT})
    crawler = Crawler(example_site(), tmp_path, client, sleep=clock.sleep, now=lambda: clock.now)
    yield crawler, fake, clock
    crawler.store.close()
    client.close()


def states(crawler: Crawler) -> dict[str, tuple[str, str | None]]:
    rows = crawler.store.db.execute("select url, state, skip_reason from urls").fetchall()
    return {row["url"]: (row["state"], row["skip_reason"]) for row in rows}


def test_every_url_ends_in_a_final_state(setup):
    crawler, fake, clock = setup
    inventory = crawler.seed_from_sitemaps()
    assert inventory["listed"] == 6 and inventory["offsite_entries"] == 1 and inventory["sitemaps"] == 2
    result = crawler.crawl()
    assert result["stopped"] == "done"
    assert states(crawler) == {
        f"{HOST}/": ("fetched", None),
        f"{HOST}/a": ("fetched", None),
        f"{HOST}/b": ("fetched", None),
        f"{HOST}/c": ("fetched", None),
        f"{HOST}/big.pdf": ("skipped", "size_cap"),
        f"{HOST}/gone": ("not_found", None),
        f"{HOST}/flaky": ("fetched", None),
        f"{HOST}/moved": ("redirect", None),
        f"{HOST}/private/x": ("skipped", "robots"),
        f"{HOST}/logo.png": ("skipped", "media"),
        f"{HOST}/search?q=1": ("skipped", "robots"),
    }
    assert all(state in FINAL_STATES for state, _ in states(crawler).values())
    assert crawler.store.get(f"{HOST}/flaky")["attempts"] == 2
    assert crawler.store.get(f"{HOST}/moved")["location"] == f"{HOST}/b"
    assert crawler.store.get(f"{HOST}/a")["sha256"] and crawler.store.get(f"{HOST}/a")["sitemap_lastmod"] == "2026-11-01"


def test_politeness_rules_hold(setup):
    crawler, fake, clock = setup
    crawler.seed_from_sitemaps()
    crawler.crawl()
    requested = [str(request.url) for request in fake.requests]
    assert not [url for url in requested if "/private/" in url or "?" in url or url.endswith(".png") or "other.test" in url]
    assert all(request.headers["user-agent"] == USER_AGENT for request in fake.requests)
    # robots.txt asks for 10 s between requests: no pause is shorter, and each request is followed by one.
    assert min(clock.sleeps) >= 10.0
    assert len(clock.sleeps) >= len(fake.requests)


def test_link_graph_keeps_external_edges_and_canonical_internal_ones(setup):
    crawler, fake, clock = setup
    crawler.seed_from_sitemaps()
    crawler.crawl()
    edges = {tuple(row) for row in crawler.store.db.execute("select from_url, to_url from links")}
    assert (f"{HOST}/", "https://other.test/x") in edges
    assert (f"{HOST}/", f"{HOST}/c") in edges  # http://example.test/c is the same page
    assert (f"{HOST}/moved", f"{HOST}/b") in edges
    assert crawler.store.get("https://other.test/x") is None


def test_recheck_is_conditional(setup):
    crawler, fake, clock = setup
    crawler.seed_from_sitemaps()
    crawler.crawl()
    assert crawler.store.recheck() == 5
    fake.requests.clear()
    crawler.crawl()
    sent = [request for request in fake.requests if request.url.path == "/a"]
    assert sent and sent[0].headers["if-none-match"] == '"a1"'
    page = crawler.store.get(f"{HOST}/a")
    assert page["state"] == "unchanged" and page["sha256"]


def test_request_limit_stops_and_resumes(setup):
    crawler, fake, clock = setup
    crawler.seed_from_sitemaps()
    before = crawler.requests
    first = crawler.crawl(max_requests=before + 2)
    assert first["stopped"] == "request limit"
    second = crawler.crawl()
    assert second["stopped"] == "done"
    assert "pending" not in crawler.store.counts() and "retry" not in crawler.store.counts()


def test_a_sustained_block_stops_the_crawl(tmp_path):
    clock = Clock()

    def handler(request):
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        return httpx.Response(403, text="Access Denied")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    crawler = Crawler(example_site(name="blocked", sitemaps=()), tmp_path, client, sleep=clock.sleep, now=lambda: clock.now)
    for number in range(60):
        crawler.add_known(f"{HOST}/p{number}", source="sitemap", in_sitemap=True)
    crawler.store.commit()
    result = crawler.crawl()
    assert result["stopped"] == "possible block"
    assert clock.sleeps.count(BLOCK_PAUSE_SECONDS) == MAX_BLOCK_PAUSES
    assert "fetched" not in crawler.store.counts()
    crawler.store.close()
    client.close()


def test_unreachable_robots_waits_instead_of_skipping(tmp_path):
    clock, calls = Clock(), {"robots": 0}

    def handler(request):
        if request.url.path == "/robots.txt":
            calls["robots"] += 1
            return httpx.Response(503) if calls["robots"] == 1 else httpx.Response(200, text="User-agent: *\nDisallow:\n")
        return httpx.Response(200, text="ok", headers={"content-type": "text/plain"})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    crawler = Crawler(example_site(name="flaky-robots", sitemaps=()), tmp_path, client, sleep=clock.sleep, now=lambda: clock.now)
    crawler.add_known(f"{HOST}/page", source="sitemap", in_sitemap=True)
    crawler.store.commit()
    crawler.crawl()
    assert calls["robots"] == 2
    assert crawler.store.get(f"{HOST}/page")["state"] == "fetched"
    crawler.store.close()
    client.close()


def test_one_crawl_per_site(tmp_path):
    lock_path = tmp_path / "crawl-example.lock"
    with SiteLock(lock_path):
        with pytest.raises(RuntimeError):
            with SiteLock(lock_path):
                pass
    with SiteLock(lock_path):
        pass
