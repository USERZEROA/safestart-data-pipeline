"""Polite, resumable crawl of one site.

Seeds from the site's sitemaps, follows in-scope links and redirects, and gives every URL it learns of a final state:
fetched (with SHA-256), unchanged (304), redirect, not_found, error (after retries) or skipped (robots, page asset,
query string, excluded, size limit, discovered-URL limit, alias of another URL, not needed by the project). Requests
go one at a time, at least `Crawl-delay` apart, and stop on a sustained block.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from ..config import USER_AGENT
from .fetcher import FetchResult, ObjectStore, fetch
from .links import extract_links
from .robots import Robots
from .sitemaps import parse_sitemap
from .sites import Site
from .store import Store
from .urls import is_asset, is_page, normalize

RETRY_DELAYS = (timedelta(minutes=5), timedelta(minutes=30))  # a third failure is final
RETRY_STATUSES = frozenset({403, 408, 425, 429, 500, 502, 503, 504})
BLOCK_STATUSES = frozenset({403, 429})
BLOCK_THRESHOLD = 10  # responses in a row that look like a block
BLOCK_PAUSE_SECONDS = 1800.0
MAX_BLOCK_PAUSES = 3
ROBOTS_MAX_AGE = timedelta(hours=24)
ROBOTS_RETRY_SECONDS = 600.0
MAX_RETRY_AFTER_SECONDS = 3600.0
MAX_SITEMAP_BYTES = 100 * 1024 * 1024
MAX_HTML_PARSE_BYTES = 20 * 1024 * 1024
PROGRESS_EVERY = 100
HTML_TYPES = ("text/html", "application/xhtml+xml")
CHALLENGE_MARKERS = (b"captcha", b"are you a robot", b"request unsuccessful", b"challenge-platform")
TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime(TIME_FORMAT)


def parse_iso(value: str) -> datetime:
    return datetime.strptime(value, TIME_FORMAT).replace(tzinfo=UTC)


def make_client() -> httpx.Client:
    return httpx.Client(
        headers={"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
        timeout=httpx.Timeout(60.0, connect=30.0),
        follow_redirects=False,
    )


def is_html(content_type: str | None) -> bool:
    return bool(content_type) and content_type.split(";", 1)[0].strip().lower() in HTML_TYPES


def looks_blocked(result: FetchResult) -> bool:
    if result.status in BLOCK_STATUSES:
        return True
    sample = result.sample.lower()
    return result.status is not None and result.status >= 400 and any(marker in sample for marker in CHALLENGE_MARKERS)


class Crawler:
    @staticmethod
    def open_store(site: Site, data_root: Path) -> Store:
        return Store(data_root / "state" / f"crawl-{site.name}.sqlite")

    def __init__(
        self,
        site: Site,
        data_root: Path,
        client: httpx.Client,
        *,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], datetime] = utc_now,
        log: logging.Logger | None = None,
    ):
        self.site = site
        self.store = self.open_store(site, data_root)
        self.objects = ObjectStore(data_root / "raw" / site.name / "objects")
        self.client = client
        self.sleep = sleep
        self.now = now
        self.log = log or logging.getLogger(f"crawl.{site.name}")
        self.robots: Robots | None = None
        self.robots_at: datetime | None = None
        self.requests = 0
        self.discovered = self.store.discovered_count()

    # robots.txt and pacing ---------------------------------------------------------------------------------------------

    def refresh_robots(self) -> Robots:
        url = f"https://{self.site.canonical_host}/robots.txt"
        status: int | None
        try:
            response = self.client.get(url, follow_redirects=True)
            status = response.status_code
            text = response.text if 200 <= status < 300 else ""
        except httpx.HTTPError as problem:
            status, text = None, ""
            self.log.warning("robots.txt unreachable: %s", problem)
        self.requests += 1
        self.robots = Robots(status, text, USER_AGENT)
        self.robots_at = self.now()
        self.store.set_meta("robots_status", str(status))
        self.store.set_meta("robots_text", text)
        self.store.set_meta("robots_fetched_at", iso(self.robots_at))
        self.store.commit()
        return self.robots

    def ensure_robots(self) -> Robots:
        if self.robots is None or self.robots_at is None or self.now() - self.robots_at > ROBOTS_MAX_AGE:
            self.refresh_robots()
            self.pause()
        while self.robots is not None and self.robots.unreachable:
            self.log.warning("robots.txt unreachable; waiting %.0f s before asking again", ROBOTS_RETRY_SECONDS)
            self.sleep(ROBOTS_RETRY_SECONDS)
            self.refresh_robots()
            self.pause()
        assert self.robots is not None
        return self.robots

    def delay(self) -> float:
        robots_delay = self.robots.crawl_delay() if self.robots else None
        return max(self.site.min_delay, robots_delay or 0.0)

    def pause(self, at_least: float | None = None) -> None:
        """Wait the crawl delay, or longer when the server asked for it with Retry-After (capped at one hour)."""
        self.sleep(max(self.delay(), min(at_least or 0.0, MAX_RETRY_AFTER_SECONDS)))

    # Known URLs --------------------------------------------------------------------------------------------------------

    def add_known(
        self,
        url: str,
        *,
        source: str,
        discovered_from: str | None = None,
        in_sitemap: bool = False,
        lastmod: str | None = None,
    ) -> None:
        existing = self.store.get(url)
        if existing is not None:
            if in_sitemap:
                self.store.mark_in_sitemap(url, lastmod)
            # A file skipped because only unneeded pages linked to it is wanted once a needed page links to it.
            if existing["skip_reason"] == "not_needed" and not self.why_unneeded(url):
                self.store.update(url, state="pending", skip_reason=None, attempts=0, error=None, next_attempt_at=None)
            return
        state, reason = "pending", None
        if self.robots is not None and not self.robots.allowed(url):
            state, reason = "skipped", "robots"
        elif self.site.excluded(url):
            state, reason = "skipped", "excluded"
        elif is_asset(url):
            state, reason = "skipped", "asset"
        elif self.site.unneeded(url):
            state, reason = "skipped", "not_needed"
        elif not in_sitemap and not self.site.follow_query_links and urlsplit(url).query:
            state, reason = "skipped", "query"
        elif not in_sitemap and self.discovered >= self.site.discovered_limit:
            state, reason = "skipped", "discovered_limit"
        self.store.insert_url(
            url,
            source=source,
            state=state,
            skip_reason=reason,
            in_sitemap=in_sitemap,
            lastmod=lastmod,
            discovered_from=discovered_from,
            now=iso(self.now()),
        )
        if not in_sitemap:
            self.discovered += 1

    # Inventory ---------------------------------------------------------------------------------------------------------

    def seed_from_sitemaps(self) -> dict[str, int]:
        """Read robots.txt and every sitemap (following indexes) and record each listed URL."""
        robots = self.ensure_robots()
        queue: list[tuple[str, str | None]] = [(url, None) for url in self.site.sitemaps]
        queue += [(url, "robots.txt") for url in robots.sitemaps() if self.site.in_scope(normalize(url))]
        seen: set[str] = set()
        listed = offsite = 0
        while queue:
            url, parent = queue.pop(0)
            if url in seen:
                continue
            seen.add(url)
            fetched_at = iso(self.now())
            if not robots.allowed(url):
                self.store.record_sitemap(
                    url, parent=parent, fetched_at=fetched_at, status=None, kind=None, entries=None, sha256=None,
                    error="disallowed by robots.txt",
                )
                continue
            result = fetch(self.client, url, self.objects, max_bytes=MAX_SITEMAP_BYTES)
            self.requests += 1
            kind = entries = error = None
            if result.path is not None:
                try:
                    sitemap = parse_sitemap(result.path.read_bytes())
                    kind, entries = sitemap.kind, len(sitemap.entries)
                    for entry in sitemap.entries:
                        target = self.site.in_scope(normalize(entry.loc, url))
                        if target is None:
                            offsite += 1
                        elif sitemap.kind == "index":
                            queue.append((target, url))
                        else:
                            listed += 1
                            self.add_known(target, source="sitemap", in_sitemap=True, lastmod=entry.lastmod,
                                           discovered_from=url)
                except ValueError as problem:
                    error = str(problem)[:500]
            elif result.status is not None and 300 <= result.status < 400:
                target = self.site.in_scope(normalize(result.location or "", url))
                error = f"redirect to {result.location}"
                if target:
                    queue.append((target, url))
            else:
                error = result.error or f"HTTP {result.status}" + (" (over size cap)" if result.too_large else "")
            self.store.record_sitemap(
                url, parent=parent, fetched_at=fetched_at, status=result.status, kind=kind, entries=entries,
                sha256=result.sha256, error=error,
            )
            self.store.commit()
            self.log.info("sitemap %s -> %s %s entries %s", url, result.status, kind or error, entries)
            self.pause(result.retry_after)
        self.store.set_meta("inventory_at", iso(self.now()))
        self.store.commit()
        return {"sitemaps": len(seen), "listed": listed, "offsite_entries": offsite, **self.store.counts()}

    def seed_from_links(self, data_root: Path) -> dict[str, int]:
        """Record the links other crawls found to this site's hosts: the inventory of a host without sitemaps. Each
        other crawl's state is only read."""
        self.ensure_robots()
        seeded = 0
        patterns = [f"{scheme}://{host}/%" for host in sorted(self.site.hosts) for scheme in ("http", "https")]
        for other in self.site.seed_from:
            path = data_root / "state" / f"crawl-{other}.sqlite"
            if not path.exists():
                self.log.warning("no %s crawl state to seed from", other)
                continue
            source = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
            try:
                where = " or ".join("to_url like ?" for _ in patterns)
                rows = source.execute(f"select to_url, min(from_url) from links where {where} group by to_url", patterns)
                for link, page in rows.fetchall():
                    url = self.site.in_scope(normalize(link))
                    if url and self.store.get(url) is None:
                        self.add_known(url, source=f"links:{other}", discovered_from=page)
                        seeded += 1
            finally:
                source.close()
        self.store.set_meta("seeded_from_links_at", iso(self.now()))
        self.store.commit()
        return {"seeded_from_links": seeded, **self.store.counts()}

    def fetch_now(self, urls: list[str], *, max_bytes: int, max_seconds: float) -> list[dict[str, object]]:
        """Download chosen URLs at once with their own size and time limits (files over the site's cap that were
        approved one by one), whatever their current state, with the usual robots.txt check and pacing."""
        results = []
        for url in urls:
            robots = self.ensure_robots()
            canonical = self.site.in_scope(normalize(url))
            if canonical is None:
                results.append({"url": url, "state": "not on this site"})
                continue
            if self.store.get(canonical) is None:
                self.add_known(canonical, source="manual")
            if not robots.allowed(canonical):
                self.store.update(canonical, state="skipped", skip_reason="robots")
                self.store.commit()
                results.append({"url": canonical, "state": "skipped", "reason": "robots"})
                continue
            # Back to pending, as if newly found, so the outcome is recorded exactly as the crawl would record it.
            self.store.update(canonical, state="pending", skip_reason=None, error=None, attempts=0, next_attempt_at=None)
            row = self.store.get(canonical)
            result = fetch(self.client, canonical, self.objects, max_bytes=max_bytes, max_seconds=max_seconds)
            self.requests += 1
            self.apply(row, result)
            self.store.commit()
            row = self.store.get(canonical)
            results.append({"url": canonical, "state": row["state"], "reason": row["skip_reason"], "status": row["http_status"],
                            "bytes": row["content_length"], "sha256": row["sha256"], "error": row["error"]})
            self.log.info("fetched on request %s -> %s %s bytes", canonical, row["state"], row["content_length"])
            self.pause(result.retry_after)
        return results

    def requeue_policy_skips(self) -> dict[str, int]:
        """Queue again what an earlier, stricter rule skipped: images, audio and video (once skipped as "media") and
        files under the site's current size limit (once over a smaller one). Page assets stay skipped."""
        counts = {"media": 0, "size_cap": 0}
        for row in self.store.skipped(("media", "size_cap")):
            url, reason = row["url"], row["skip_reason"]
            if reason == "media" and is_asset(url):
                self.store.update(url, skip_reason="asset")
                continue
            if reason == "size_cap" and (row["content_length"] or 0) > self.site.max_bytes:
                continue
            self.store.update(url, state="pending", skip_reason=None, error=None, attempts=0, next_attempt_at=None)
            counts[reason] += 1
        self.store.commit()
        return counts

    def fold_aliases(self) -> dict[str, int]:
        """Waiting URLs recorded in a form the current URL rules no longer produce (an alias such as /index%2Ephp/news)
        are recorded as skipped "alias" with their canonical URL as location, and that URL is queued in their place
        unless already known. URLs that already have an outcome keep their records."""
        counts = {"alias": 0, "queued": 0}
        for row in self.store.waiting():
            url = row["url"]
            canonical = self.site.in_scope(normalize(url))
            if canonical is None or canonical == url:
                continue
            self.store.update(url, state="skipped", skip_reason="alias", location=canonical, next_attempt_at=None)
            counts["alias"] += 1
            if self.store.get(canonical) is None:
                self.add_known(canonical, source=row["source"], discovered_from=row["discovered_from"],
                               in_sitemap=bool(row["in_sitemap"]), lastmod=row["sitemap_lastmod"])
                counts["queued"] += 1
        self.store.commit()
        return counts

    def why_unneeded(self, url: str) -> str | None:
        """'by_rule' when the site's rules name the URL; 'linked_only_from_unneeded' for a file every known link to
        which comes from a page the rules name (unless it lies under the kept paths); None when it is needed. Pages are
        never judged by their links alone."""
        if self.site.unneeded(url):
            return "by_rule"
        if is_page(url) or self.site.kept(url):
            return None
        pages = [self.site.in_scope(normalize(page)) or page for page in self.store.linking_pages(url)]
        if pages and all(self.site.unneeded(page) for page in pages):
            return "linked_only_from_unneeded"
        return None

    def skip_unneeded(self) -> dict[str, int]:
        """Bring the queue in line with the site's current not-needed rules: waiting URLs they cover are skipped as
        "not_needed", and URLs skipped so that the rules no longer cover are queued again. Nothing downloaded changes."""
        counts = {"by_rule": 0, "linked_only_from_unneeded": 0, "queued_again": 0}
        for row in self.store.waiting():
            reason = self.why_unneeded(row["url"])
            if reason:
                self.store.update(row["url"], state="skipped", skip_reason="not_needed", next_attempt_at=None)
                counts[reason] += 1
        for row in self.store.skipped(("not_needed",)):
            if not self.why_unneeded(row["url"]):
                self.store.update(row["url"], state="pending", skip_reason=None, attempts=0, error=None, next_attempt_at=None)
                counts["queued_again"] += 1
        self.store.commit()
        return counts

    # Crawl -------------------------------------------------------------------------------------------------------------

    def crawl(self, max_requests: int | None = None, until: datetime | None = None) -> dict[str, object]:
        """Work through due URLs until none are left, the request or time limit is reached, the byte budget is used,
        or the site keeps answering like a block."""
        self.ensure_robots()
        folded = self.fold_aliases()
        if any(folded.values()):
            self.log.info("waiting URLs recorded under an alias, folded into their canonical URL: %s", folded)
        unneeded = self.skip_unneeded()
        if any(unneeded.values()):
            self.log.info("waiting URLs the project does not need, skipped: %s", unneeded)
        requeued = self.requeue_policy_skips()
        if any(requeued.values()):
            self.log.info("queued again under the current download rules: %s", requeued)
        blocked_in_a_row = block_pauses = 0
        stopped = "done"
        while True:
            if max_requests is not None and self.requests >= max_requests:
                stopped = "request limit"
                break
            if until is not None and self.now() >= until:
                stopped = "time limit"
                break
            robots = self.ensure_robots()
            row = self.store.next_due(iso(self.now()))
            if row is None:
                earliest = self.store.earliest_retry()
                if earliest is None:
                    break
                wait = (parse_iso(earliest) - self.now()).total_seconds()
                self.sleep(min(max(wait, 1.0), 600.0))
                continue
            url = row["url"]
            if not robots.allowed(url):
                self.store.update(url, state="skipped", skip_reason="robots")
                self.store.commit()
                continue
            result = fetch(
                self.client, url, self.objects, etag=row["etag"], last_modified=row["last_modified"],
                max_bytes=self.site.max_bytes, max_seconds=self.site.max_seconds,
            )
            self.requests += 1
            self.apply(row, result)
            self.store.commit()
            if self.requests % PROGRESS_EVERY == 0:
                self.log.info("requests %d states %s", self.requests, self.store.counts())
                if self.store.fetched_bytes() >= self.site.byte_budget:
                    stopped = "byte budget"
                    break
            blocked_in_a_row = blocked_in_a_row + 1 if looks_blocked(result) else 0
            if blocked_in_a_row >= BLOCK_THRESHOLD:
                block_pauses += 1
                if block_pauses > MAX_BLOCK_PAUSES:
                    stopped = "possible block"
                    break
                self.log.warning("%d blocked responses in a row; pausing %.0f s", blocked_in_a_row, BLOCK_PAUSE_SECONDS)
                self.sleep(BLOCK_PAUSE_SECONDS)
                blocked_in_a_row = 0
            self.pause(result.retry_after)
        self.store.set_meta("last_stop", f"{iso(self.now())} {stopped}")
        self.store.commit()
        self.log.info("stopped (%s) after %d requests; states %s", stopped, self.requests, self.store.counts())
        return {"stopped": stopped, "requests": self.requests, "states": self.store.counts()}

    def apply(self, row, result: FetchResult) -> None:
        url, now = row["url"], iso(self.now())
        attempts = row["attempts"] + 1
        status = result.status
        self.store.add_fetch(
            url, now, status=status, sha256=result.sha256, size=result.size, etag=result.etag,
            last_modified=result.last_modified, error=result.error,
        )
        common = {"http_status": status, "fetched_at": now, "attempts": attempts, "next_attempt_at": None}
        if status is None or (result.error and not result.too_large):
            self.retry_or_fail(url, attempts, status, result.error or "no response")
        elif 200 <= status < 300 and result.too_large:
            self.store.update(url, state="skipped", skip_reason="size_cap", content_type=result.content_type,
                              content_length=result.size, error=None, **common)
        elif 200 <= status < 300:
            self.store.update(url, state="fetched", content_type=result.content_type, content_length=result.size,
                              sha256=result.sha256, etag=result.etag, last_modified=result.last_modified, error=None,
                              **common)
            if is_html(result.content_type) and result.path is not None and (result.size or 0) <= MAX_HTML_PARSE_BYTES:
                self.discover(url, result.path.read_bytes())
        elif status == 304:
            self.store.update(url, state="unchanged", error=None, **common)
        elif 300 <= status < 400:
            target = normalize(result.location or "", url)
            self.store.update(url, state="redirect", location=target or result.location, error=None, **common)
            if target:
                canonical = self.site.in_scope(target)
                self.store.add_links(url, [canonical or target])
                if canonical and canonical != url:
                    self.add_known(canonical, source="redirect", discovered_from=url)
        elif status in (404, 410):
            self.store.update(url, state="not_found", error=None, **common)
        elif status in RETRY_STATUSES:
            self.retry_or_fail(url, attempts, status, f"HTTP {status}")
        else:
            self.store.update(url, state="error", error=f"HTTP {status}", **common)

    def retry_or_fail(self, url: str, attempts: int, status: int | None, error: str) -> None:
        now = self.now()
        if attempts <= len(RETRY_DELAYS):
            self.store.update(url, state="retry", http_status=status, attempts=attempts, error=error,
                              next_attempt_at=iso(now + RETRY_DELAYS[attempts - 1]), fetched_at=iso(now))
        else:
            self.store.update(url, state="error", http_status=status, attempts=attempts, error=error,
                              next_attempt_at=None, fetched_at=iso(now))

    def discover(self, page_url: str, content: bytes) -> None:
        found = [(self.site.in_scope(link), link) for link in extract_links(content, page_url)]
        # The edges go in first, so that a target's needs are judged with this page's link already known.
        self.store.add_links(page_url, [canonical or link for canonical, link in found])
        for canonical, _ in found:
            if canonical and canonical != page_url:
                self.add_known(canonical, source="discovered", discovered_from=page_url)
