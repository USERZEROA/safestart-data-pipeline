"""The websites the crawler covers."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit

GB = 1024**3


@dataclass(frozen=True)
class Site:
    name: str
    canonical_host: str
    hosts: frozenset[str]
    sitemaps: tuple[str, ...]
    min_delay: float  # seconds between requests; robots.txt Crawl-delay raises it, never lowers it
    byte_budget: int  # stop and report when downloads reach this many bytes
    discovered_limit: int = 100_000  # URLs found only through links; beyond it they are recorded as skipped
    max_bytes: int = 50 * 1024 * 1024  # per file

    def in_scope(self, url: str | None) -> str | None:
        """The canonical https form of a URL on one of this site's hosts, or None for any other host."""
        if not url:
            return None
        parts = urlsplit(url)
        if (parts.hostname or "") not in self.hosts:
            return None
        return urlunsplit(("https", self.canonical_host, parts.path or "/", parts.query, ""))


SITES = {
    "osha": Site(
        name="osha",
        canonical_host="www.osha.gov",
        hosts=frozenset({"www.osha.gov", "osha.gov"}),
        sitemaps=("https://www.osha.gov/sitemap.xml",),
        min_delay=10.0,
        byte_budget=20 * GB,
    ),
    "msha": Site(
        name="msha",
        canonical_host="www.msha.gov",
        hosts=frozenset({"www.msha.gov", "msha.gov"}),
        sitemaps=("https://www.msha.gov/sitemap.xml",),
        min_delay=1.0,
        byte_budget=15 * GB,
    ),
}
