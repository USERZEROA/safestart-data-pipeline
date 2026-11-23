from __future__ import annotations

import httpx
import pytest

from safestart_pipeline.watch.polite import MAX_REDIRECTS, PoliteClient

ROBOTS = "User-agent: *\nCrawl-delay: 5\nDisallow: /private/\n"


class Clock:
    def __init__(self):
        self.now = 0.0
        self.waits: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.waits.append(seconds)
        self.now += seconds


def polite(handler, clock: Clock, **options) -> PoliteClient:
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return PoliteClient(client, min_delay=1.0, sleep=clock.sleep, clock=clock.monotonic, **options)


def site(request: httpx.Request) -> httpx.Response:
    path = request.url.path
    if path == "/robots.txt":
        return httpx.Response(200, text=ROBOTS)
    if path == "/old":
        return httpx.Response(302, headers={"location": "/new"})
    if path == "/leak":
        return httpx.Response(301, headers={"location": "/private/x"})
    if path == "/loop":
        return httpx.Response(301, headers={"location": "/loop"})
    if path == "/new":
        return httpx.Response(200, text="page")
    return httpx.Response(404)


def test_each_redirect_hop_waits_the_crawl_delay():
    clock = Clock()
    answer = polite(site, clock).get("https://www.example.test/old")
    assert (answer.url, answer.status, answer.content) == ("https://www.example.test/new", 200, b"page")
    assert clock.waits == [5.0, 5.0]  # after robots.txt, and again before the redirect's target


def test_a_hop_robots_txt_disallows_is_refused():
    clock, seen = Clock(), []

    def handler(request):
        seen.append(request.url.path)
        return site(request)

    with pytest.raises(PermissionError, match="disallows https://www.example.test/private/x"):
        polite(handler, clock).get("https://www.example.test/leak")
    assert seen == ["/robots.txt", "/leak"]


def test_an_unreachable_robots_txt_allows_nothing_and_a_missing_one_everything():
    down = polite(lambda request: httpx.Response(503), Clock())
    with pytest.raises(PermissionError, match="unreachable"):
        down.get("https://www.example.test/page")
    missing = polite(lambda request: httpx.Response(404) if request.url.path == "/robots.txt" else httpx.Response(200),
                     Clock())
    assert missing.get("https://www.example.test/page").status == 200


def test_redirect_loops_and_oversized_answers_are_refused():
    with pytest.raises(httpx.TooManyRedirects):
        polite(site, Clock()).get("https://www.example.test/loop")
    big = polite(lambda request: httpx.Response(200, content=b"x" * 2048), Clock(), max_bytes=1024)
    with pytest.raises(ValueError, match="more than 1024 bytes"):
        big.get("https://www.example.test/big")
    assert MAX_REDIRECTS == 5
