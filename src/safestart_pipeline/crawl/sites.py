"""The websites the crawler covers."""

from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import unquote, urlsplit, urlunsplit

from .urls import extension, is_page

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
    # Content the project does not use: never downloaded, recorded as skipped "not_needed", and queued again only if
    # these rules change. Matched without case: file types, path prefixes, and words in a file's name (pages are left
    # alone, since an English page may name a language). A waiting file every known link to which comes from a page
    # these rules name is not needed either, unless it lies under `kept_paths`.
    unneeded_types: frozenset[str] = frozenset()
    unneeded_paths: tuple[str, ...] = ()
    unneeded_patterns: tuple[str, ...] = ()  # regular expressions searched in the decoded, lower-case path
    unneeded_words: frozenset[str] = frozenset()
    kept_paths: tuple[str, ...] = ()
    # Listings (path prefixes) whose entries, pages and files alike, are not needed when nothing else links to them,
    # such as an archive of cancelled documents that share their path with current ones.
    unneeded_listings: tuple[str, ...] = ()

    def excluded(self, url: str) -> bool:
        path = urlsplit(url).path.lower()
        return any(path.startswith(prefix.lower()) for prefix in self.excluded_paths)

    def unneeded(self, url: str) -> bool:
        path = unquote(urlsplit(url).path).lower()
        if any(path.startswith(prefix.lower()) for prefix in self.unneeded_paths):
            return True
        if any(re.search(pattern, path) for pattern in self.unneeded_patterns):
            return True
        if is_page(url):
            return False
        if extension(url) in self.unneeded_types:
            return True
        name = path.rsplit("/", 1)[-1]
        return any(word in self.unneeded_words for word in re.findall(r"[^\W\d_]+", name))

    def kept(self, url: str) -> bool:
        path = unquote(urlsplit(url).path).lower()
        return any(path.startswith(prefix.lower()) for prefix in self.kept_paths)

    def unneeded_listing(self, url: str) -> bool:
        path = unquote(urlsplit(url).path).lower()
        return any(path.startswith(prefix.lower()) for prefix in self.unneeded_listings)

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
        # Not needed (owner, 2026-11-21): pictures, sound, video and e-book copies of publications; partnership,
        # success-story and grant-recipient pages; advisory-committee records; contact, order-cart and by-language
        # pages; copies in other languages; OSHA's web copy of CFR text (/laws-regs/regulations/standardnumber/),
        # since the bulk source holds the complete eCFR titles 29 and 30, verified section by section (the part 2205
        # page alone links 899 section numbers, 2205.101 to 2205.999). Interpretations, directives, Federal Register
        # notices, the interlinking pages that lead to them, publications, topic pages, news and data files stay.
        unneeded_types=frozenset(
            ".jpg .jpeg .png .gif .bmp .tif .tiff .svg .webp .ico .mp3 .wav .m4a .wma .aac .ogg .mp4 .m4v .mov .wmv"
            " .avi .flv .webm .mpg .mpeg .mobi .epub".split()
        ),
        unneeded_paths=(
            "/alliances/", "/successstories/", "/partnerships/", "/harwoodgrants/", "/advisorycommittee/",
            "/doc/accsh/", "/contactus/", "/publications/add/", "/publications/bylanguage/",
            "/laws-regs/regulations/standardnumber/", "/enforcement/directives/archive/",
            # Month pages of past press releases (77 months, 2013 to 2025). Of the 1,248 releases the sitemap listed,
            # 1,146 are gone (404); current releases are linked from live pages and stay.
            "/news/newsreleases/archive/",
            # Federal Annual Monitoring Evaluation (FAME) reports: OSHA's yearly review of how each of the 29 State Plans
            # runs its program, with the state's formal response, about 17 documents per state. They evaluate program
            # administration, not workplace hazards or rules; the State Plan pages themselves stay.
            "/stateplans/famereport/",
        ),
        # Interlinking pages exist for every part, section and paragraph of a standard, each with sub-pages per kind
        # of document. By 2026-11-22 about 9,500 paragraph-level pages had been read and had led to 254 documents not
        # already known, and the "regulations", "all" and "settlement_agreement" sub-pages to almost none; the part and
        # section pages for Federal Register notices, interpretations and directives led to 3,300. Paragraph pages and
        # those three sub-pages are not needed.
        unneeded_patterns=(
            r"^/laws-regs/interlinking/standards/[^/]*\(",
            r"^/laws-regs/interlinking/standards/.+/(regulations|all|settlement_agreement)/?$",
        ),
        unneeded_words=frozenset(
            "spanish espanol español chinese vietnamese korean portuguese russian haitian creole tagalog arabic hmong"
            " somali nepali japanese".split()
        ),
        # OSHA's own publications, Federal Register notices and directives are needed whichever page links to them.
        kept_paths=(
            "/sites/default/files/publications/", "/sites/default/files/laws-regs/", "/sites/default/files/enforcement/",
        ),
        # The directive archive lists cancelled and expired directives (by year since 1978, and expired local emphasis
        # programs) under the same paths as current ones. On 2026-11-22 it had put 1,165 URLs in the queue, 46 of them
        # also linked from a current page. Entries only the archive links to are not needed.
        unneeded_listings=("/enforcement/directives/archive/",),
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
