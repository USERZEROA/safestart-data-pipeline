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
    # Per file. Copies are complete (owner, 2026-11-18): these limits only stop a runaway response, and a file that
    # passes them is never left out for its size.
    max_bytes: int = 2 * GB
    max_seconds: float = 3600.0
    # Links with a query string (filters, facets, search, calendars) multiply without end. They are recorded as skipped
    # unless the sitemap lists them; sitemap URLs are always fetched when robots.txt allows them.
    follow_query_links: bool = False
    # A host without sitemaps is seeded from the links other crawls recorded to it (`ssp inventory` reads them).
    seed_from: tuple[str, ...] = ()
    # Path prefixes (compared without case) that another source already downloads; recorded as skipped "excluded".
    excluded_paths: tuple[str, ...] = ()
    # Leading path segments the site's software ignores: Drupal answers /index.php/news exactly as /news, and a page
    # reached that way repeats the prefix in every link, so following them would copy the site once more. Removed from
    # every URL on the site's hosts.
    alias_prefixes: tuple[str, ...] = ()

    def excluded(self, url: str) -> bool:
        path = urlsplit(url).path.lower()
        return any(path.startswith(prefix.lower()) for prefix in self.excluded_paths)

    def in_scope(self, url: str | None) -> str | None:
        """The canonical https form of a URL on one of this site's hosts, or None for any other host."""
        if not url:
            return None
        parts = urlsplit(url)
        if (parts.hostname or "") not in self.hosts:
            return None
        path = parts.path or "/"
        for prefix in self.alias_prefixes:
            if path == prefix or path.startswith(prefix + "/"):
                path = path[len(prefix):] or "/"
                break
        return urlunsplit(("https", self.canonical_host, path, parts.query, ""))


SITES = {
    "osha": Site(
        name="osha",
        canonical_host="www.osha.gov",
        hosts=frozenset({"www.osha.gov", "osha.gov"}),
        sitemaps=("https://www.osha.gov/sitemap.xml",),
        min_delay=10.0,
        byte_budget=20 * GB,
        # The home page links a few pages as /index%2Ephp/<path>; by 2026-11-21 that had grown to 375 URLs in three
        # spellings, each a copy of /<path> with the prefix in its links.
        alias_prefixes=("/index.php",),
    ),
    "msha": Site(
        name="msha",
        canonical_host="www.msha.gov",
        hosts=frozenset({"www.msha.gov", "msha.gov"}),
        sitemaps=("https://www.msha.gov/sitemap.xml",),
        min_delay=1.0,
        byte_budget=15 * GB,
    ),
    # MSHA's legacy host: no home page (it redirects to www.msha.gov), no sitemap and no directory listings. Most old
    # pages now redirect to www.msha.gov, while older documents (investigation reports, guides, alerts) are still served
    # here, reachable only through links, so the www.msha.gov crawl's links seed it. Its open-data directory belongs to
    # the bulk source (acquire/bulk.py), which keeps every version of those files.
    "arlweb": Site(
        name="arlweb",
        canonical_host="arlweb.msha.gov",
        hosts=frozenset({"arlweb.msha.gov"}),
        sitemaps=(),
        min_delay=1.0,
        # Complete copy (owner, 2026-11-18): about 12 GB of pages and reports plus 13 GB of large exhibits.
        byte_budget=40 * GB,
        seed_from=("msha",),
        excluded_paths=("/OpenGovernmentData/DataSets/",),
    ),
}
