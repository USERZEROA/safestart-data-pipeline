"""Paced GETs that obey robots.txt, for the watchers' few requests to websites and APIs.

Each request must be allowed by its host's robots.txt (read once per check, with crawl/robots.py) and waits at least the
caller's delay, or the host's Crawl-delay when that is longer, after the previous request to the same host. Redirects
are followed one hop at a time under the same rules, so a redirect never skips the wait: OSHA's sitemap.xml redirects,
and OSHA asks for 10 seconds between requests. A response is read up to a size limit.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import urljoin

import httpx

from ..config import USER_AGENT
from ..crawl.robots import Robots

MAX_REDIRECTS = 5
MAX_BYTES = 100 * 1024 * 1024


@dataclass(frozen=True)
class Answer:
    url: str  # the URL that answered, after any redirects
    status: int
    content: bytes


class PoliteClient:
    def __init__(self, client: httpx.Client, *, min_delay: float, sleep: Callable[[float], None] = time.sleep,
                 clock: Callable[[], float] = time.monotonic, max_bytes: int = MAX_BYTES):
        self.client, self.min_delay, self.sleep, self.clock, self.max_bytes = client, min_delay, sleep, clock, max_bytes
        self.rules: dict[str, Robots] = {}
        self.last: dict[str, float] = {}
        self.requests = 0

    def robots(self, host: str) -> Robots:
        if host not in self.rules:
            try:
                answer = self.client.get(f"https://{host}/robots.txt", follow_redirects=True)
                self.rules[host] = Robots(answer.status_code, answer.text if answer.is_success else "", USER_AGENT)
            except httpx.HTTPError:
                self.rules[host] = Robots(None, "", USER_AGENT)
            self._sent(host)
        return self.rules[host]

    def get(self, url: str) -> Answer:
        """GET with robots.txt and pacing on every hop. PermissionError when robots.txt does not allow a hop."""
        for _ in range(MAX_REDIRECTS + 1):
            host = httpx.URL(url).host
            rules = self.robots(host)
            if not rules.allowed(url):
                reason = "is unreachable, so it cannot allow" if rules.unreachable else "disallows"
                raise PermissionError(f"robots.txt {reason} {url}")
            wait = self.last[host] + max(self.min_delay, rules.crawl_delay() or 0.0) - self.clock()
            if wait > 0:
                self.sleep(wait)
            with self.client.stream("GET", url, follow_redirects=False) as response:
                content = self._read(response, url)
            self._sent(host)
            if not response.is_redirect:
                return Answer(url, response.status_code, content)
            url = urljoin(url, response.headers["location"])
        raise httpx.TooManyRedirects(f"more than {MAX_REDIRECTS} redirects, the last to {url}")

    def _read(self, response: httpx.Response, url: str) -> bytes:
        body = bytearray()
        for chunk in response.iter_bytes():
            body += chunk
            if len(body) > self.max_bytes:
                raise ValueError(f"{url} answered with more than {self.max_bytes} bytes")
        return bytes(body)

    def _sent(self, host: str) -> None:
        self.last[host] = self.clock()
        self.requests += 1
