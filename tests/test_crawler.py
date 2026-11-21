from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest

from safestart_pipeline.config import USER_AGENT
from safestart_pipeline.crawl.crawler import BLOCK_PAUSE_SECONDS, MAX_BLOCK_PAUSES, Crawler
from safestart_pipeline.crawl.locks import SiteLock
from safestart_pipeline.crawl.sites import Site
from safestart_pipeline.crawl.store import FINAL_STATES, Store
from safestart_pipeline.crawl.urls import normalize

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
<a href="/a">A</a> <a href="/b">B</a> <a href="/private/x">private</a> <a href="/logo.png">logo</a> <a href="/site.css">style</a>
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
        if path == "/logo.png":
            return httpx.Response(200, content=b"PNG" + b"0" * 100, headers={"content-type": "image/png"})
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
        f"{HOST}/logo.png": ("fetched", None),
        f"{HOST}/site.css": ("skipped", "asset"),
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
    assert not [url for url in requested if "/private/" in url or "?" in url or url.endswith(".css") or "other.test" in url]
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
    assert crawler.store.recheck() == 6
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


def test_query_links_are_recorded_but_not_followed(tmp_path):
    clock = Clock()

    def handler(request):
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        if request.url.path == "/list":
            body = '<html><body><a href="/list?f%5B0%5D=type%3A1">facet</a> <a href="/doc">doc</a></body></html>'
            return httpx.Response(200, text=body, headers=HTML)
        return httpx.Response(200, text="ok", headers={"content-type": "text/plain"})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    crawler = Crawler(example_site(name="facets", sitemaps=()), tmp_path, client, sleep=clock.sleep, now=lambda: clock.now)
    crawler.add_known(f"{HOST}/list", source="sitemap", in_sitemap=True)
    crawler.add_known(f"{HOST}/listed?page=2", source="sitemap", in_sitemap=True)
    crawler.store.commit()
    crawler.crawl()
    assert states(crawler) == {
        f"{HOST}/list": ("fetched", None),
        f"{HOST}/listed?page=2": ("fetched", None),
        f"{HOST}/list?f%5B0%5D=type%3A1": ("skipped", "query"),
        f"{HOST}/doc": ("fetched", None),
    }
    crawler.store.close()
    client.close()


def test_an_approved_large_file_is_fetched_on_request(setup):
    crawler, fake, clock = setup
    crawler.seed_from_sitemaps()
    crawler.crawl()
    assert crawler.store.get(f"{HOST}/big.pdf")["skip_reason"] == "size_cap"
    results = crawler.fetch_now([f"{HOST}/big.pdf", "https://other.test/x"], max_bytes=10_000, max_seconds=60)
    row = crawler.store.get(f"{HOST}/big.pdf")
    assert (row["state"], row["skip_reason"], row["content_length"]) == ("fetched", None, 5004) and row["sha256"]
    assert [result["state"] for result in results] == ["fetched", "not on this site"]
    assert clock.sleeps[-1] >= 10.0  # the site's crawl delay still follows the request


def test_files_an_earlier_rule_skipped_are_queued_again(setup):
    crawler, fake, clock = setup
    rows = {
        f"{HOST}/logo.png": ("media", None),  # an image: downloaded now
        f"{HOST}/site.css": ("media", None),  # a page asset: stays skipped, under its own reason
        f"{HOST}/big.pdf": ("size_cap", 5004),  # still over this site's 1,000-byte limit
        f"{HOST}/b": ("size_cap", 800),  # under it now
        f"{HOST}/c": ("size_cap", None),  # size never declared
    }
    for url, (reason, size) in rows.items():
        crawler.store.insert_url(url, source="sitemap", state="skipped", skip_reason=reason, in_sitemap=True, lastmod=None,
                                 discovered_from=None, now="2026-11-17T12:00:00Z")
        crawler.store.update(url, content_length=size)
    crawler.store.commit()
    assert crawler.requeue_policy_skips() == {"media": 1, "size_cap": 2}
    assert {url: states(crawler)[url] for url in rows} == {
        f"{HOST}/logo.png": ("pending", None),
        f"{HOST}/site.css": ("skipped", "asset"),
        f"{HOST}/big.pdf": ("skipped", "size_cap"),
        f"{HOST}/b": ("pending", None),
        f"{HOST}/c": ("pending", None),
    }
    crawler.crawl()
    assert crawler.store.get(f"{HOST}/logo.png")["state"] == "fetched"


def test_a_host_without_sitemaps_is_seeded_from_another_crawls_links(tmp_path):
    clock = Clock()
    first = Store(tmp_path / "state" / "crawl-example.sqlite")
    first.add_links(f"{HOST}/", [
        "https://legacy.example.test/report.pdf", "http://legacy.example.test/old.htm",
        "https://legacy.example.test/data/sets/x.zip", "https://legacy.example.test/clip.wav", "https://elsewhere.test/a",
    ])
    first.close()

    def handler(request):
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        return httpx.Response(200, text="ok", headers={"content-type": "text/plain"})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    legacy = Site(name="legacy", canonical_host="legacy.example.test", hosts=frozenset({"legacy.example.test"}), sitemaps=(),
                  min_delay=1.0, byte_budget=10**9, seed_from=("example",), excluded_paths=("/DATA/Sets/",))
    crawler = Crawler(legacy, tmp_path, client, sleep=clock.sleep, now=lambda: clock.now)
    assert crawler.seed_from_links(tmp_path)["seeded_from_links"] == 4
    assert states(crawler) == {
        "https://legacy.example.test/report.pdf": ("pending", None),
        "https://legacy.example.test/old.htm": ("pending", None),
        "https://legacy.example.test/data/sets/x.zip": ("skipped", "excluded"),
        "https://legacy.example.test/clip.wav": ("pending", None),
    }
    assert crawler.store.get("https://legacy.example.test/old.htm")["discovered_from"] == f"{HOST}/"
    assert crawler.crawl()["stopped"] == "done"
    assert crawler.store.counts() == {"fetched": 3, "skipped": 1}
    crawler.store.close()
    client.close()


def test_an_alias_prefix_maps_to_the_canonical_path():
    site = example_site(alias_prefixes=("/index.php",))
    assert site.in_scope(normalize("http://example.test/index%2Ephp/news?page=2")) == f"{HOST}/news?page=2"
    assert site.in_scope(f"{HOST}/index.php") == f"{HOST}/"
    assert site.in_scope(f"{HOST}/index.phpx/a") == f"{HOST}/index.phpx/a"
    assert site.in_scope(f"{HOST}/docs/index.php/a") == f"{HOST}/docs/index.php/a"
    assert example_site().in_scope(f"{HOST}/index.php/news") == f"{HOST}/index.php/news"


def test_alias_spellings_are_crawled_once_under_the_canonical_url(tmp_path):
    clock = Clock()
    # Drupal answers /index.php/<path> as /<path>, and a page reached that way repeats the prefix in its links.
    page = ('<html><body><a href="/index%2Ephp/news">a</a> <a href="/index%2ephp/news">b</a>'
            ' <a href="/index.php/about">c</a> <a href="/news">d</a></body></html>')

    def handler(request):
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        if request.url.path in ("/", "/news"):
            return httpx.Response(200, text=page, headers=HTML)
        return httpx.Response(200, text="ok", headers={"content-type": "text/plain"})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    crawler = Crawler(example_site(name="drupal", sitemaps=(), alias_prefixes=("/index.php",)), tmp_path, client,
                      sleep=clock.sleep, now=lambda: clock.now)
    crawler.add_known(f"{HOST}/", source="sitemap", in_sitemap=True)
    crawler.store.commit()
    assert crawler.crawl()["stopped"] == "done"
    assert states(crawler) == {
        f"{HOST}/": ("fetched", None),
        f"{HOST}/news": ("fetched", None),
        f"{HOST}/about": ("fetched", None),
    }
    crawler.store.close()
    client.close()


def test_waiting_urls_recorded_under_an_alias_are_folded_into_their_canonical_url(tmp_path):
    client = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(404)))
    crawler = Crawler(example_site(name="drupal", sitemaps=(), alias_prefixes=("/index.php",)), tmp_path, client)
    for url, state in ((f"{HOST}/index%2Ephp/news", "pending"), (f"{HOST}/index%2ephp/about", "retry"),
                       (f"{HOST}/index.php/old", "fetched"), (f"{HOST}/news", "fetched")):
        crawler.store.insert_url(url, source="discovered", state=state, now="2026-11-21T12:00:00Z",
                                 discovered_from=f"{HOST}/")
    crawler.store.commit()
    assert crawler.fold_aliases() == {"alias": 2, "queued": 1}
    assert states(crawler) == {
        f"{HOST}/index%2Ephp/news": ("skipped", "alias"),
        f"{HOST}/index%2ephp/about": ("skipped", "alias"),
        f"{HOST}/index.php/old": ("fetched", None),  # downloaded earlier: its record stays
        f"{HOST}/news": ("fetched", None),
        f"{HOST}/about": ("pending", None),
    }
    assert crawler.store.get(f"{HOST}/index%2ephp/about")["location"] == f"{HOST}/about"
    assert crawler.store.get(f"{HOST}/about")["discovered_from"] == f"{HOST}/"
    assert crawler.fold_aliases() == {"alias": 0, "queued": 0}
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
