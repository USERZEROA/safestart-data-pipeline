"""robots.txt rules (RFC 9309), parsed by Protego, which supports the `*` and `$` patterns these sites use."""

from __future__ import annotations

from protego import Protego


class Robots:
    """Answers for one host.

    A missing robots.txt (any 4xx) allows everything. An unreachable one (5xx or a network error) counts as a full
    disallow until it is fetched again (RFC 9309, section 2.3.1.4); callers wait instead of marking URLs skipped.
    """

    def __init__(self, status: int | None, text: str, user_agent: str):
        self.status = status
        self.user_agent = user_agent
        self.unreachable = status is None or status >= 500
        ok = status is not None and 200 <= status < 300
        self._parser = Protego.parse(text) if ok else None

    def allowed(self, url: str) -> bool:
        if self.unreachable:
            return False
        if self._parser is None:
            return True
        return bool(self._parser.can_fetch(url, self.user_agent))

    def crawl_delay(self) -> float | None:
        if self._parser is None:
            return None
        value = self._parser.crawl_delay(self.user_agent)
        return float(value) if value is not None else None

    def sitemaps(self) -> list[str]:
        return list(self._parser.sitemaps) if self._parser is not None else []
