"""Polite, resumable crawl of one site.

Seeds from the site's sitemaps, follows in-scope links and redirects, and gives every URL it learns of a final state:
fetched (with SHA-256), unchanged (304), redirect, not_found, error (after retries), rejected (a body that failed its
check, after retries) or skipped (robots, page asset, query string, excluded, size limit, discovered-URL limit, alias of
another URL, not needed by the project). Requests go one at a time, at least `Crawl-delay` apart, and stop on a
sustained block.

Every 2xx body is examined (content.py) before it is kept. A usable body of a URL the store does not hold is "new": its
object, an edition and a change row are recorded. A refused one is never kept: it is tried again like a failed request
and ends "rejected". A URL the store holds (store.py) is asked again conditionally and keeps its copy whatever happens,
except as follows:
- a usable body whose content hash differs from the held copy's is a new edition ("changed"; "restored" for a URL that
  was gone); one with the same content hash stores nothing new, and only last_checked_at and next_check_at move;
- 404 or 410 retires it at once ("gone": gone_since, purge_after 30 days on), the copy kept until then;
- 3xx is "moved", the target queued;
- 401, 403, 429, 451, 5xx, another error, no answer, a refused body, one over the size limit, or a robots.txt that now
  disallows the URL (it is then not asked) leave the held copy current: no retry state, no demotion, the reason in
  `error` (refused bodies also as a "rejected" change row), and the URL due again a day later.
A URL's first download keeps its retry rules. Each crawl or fetch_now call is a run (the runs table), and every change
names its run. Each answer is committed whole, its row, edition and change together: an interrupted run rolls back the
answer it was recording, and that URL is asked again.

`ask` makes one request under all of these rules, and the crawl's loop and the daily run of the official files
(official/daily.py) both ask through it, so robots.txt, the pause, Retry-After and the stop on a sustained block are the
same for both. While a daily run listens (`unlinked`), a page asked again has its links stored as they stand today, and
the daily run is told the targets the page no longer links to; a crawl only ever adds links.
"""

from __future__ import annotations

import logging
import sqlite3
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from ..config import USER_AGENT
from .backfill import due_after, examine_held
from .content import HASH_FORMAT, Verdict, examine
from .fetcher import FetchResult, ObjectStore, fetch
from .links import extract_links
from .robots import Robots
from .sitemaps import REFUSED_STATUSES, parse_sitemap, served_as_html
from .sites import Site
from .store import Store, holds, unexamined
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
PURGE_AFTER = timedelta(days=30)  # a gone URL's copy is kept this long
RECHECK_GONE = timedelta(days=7)  # a gone or moved URL is asked about again at this pace, so a return is noticed
RECHECK_FAILED = timedelta(days=1)  # a held URL whose check failed is due again this soon
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


class RobotsUnreachable(Exception):
    """robots.txt could still not be read when the crawl's time limit came."""


@dataclass
class SitemapRead:
    """One reading of a site's sitemaps: every in-scope URL they list (with its lastmod), the URLs recorded for the
    first time, how many sitemaps were asked for, the URL-set entries in scope (repeats included) and off the site, and
    the sitemaps the host refused or that could not be read otherwise, each as "<sitemap>: <why>"."""

    listed: dict[str, str | None] = field(default_factory=dict)
    added: list[str] = field(default_factory=list)
    sitemaps: int = 0
    entries: int = 0
    offsite: int = 0
    refused: list[str] = field(default_factory=list)
    unread: list[str] = field(default_factory=list)

    @property
    def complete(self) -> bool:
        """Whether every sitemap was read: only then does a URL they do not list tell anything."""
        return not self.refused and not self.unread


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
        self.run_id: str | None = None
        self.run_requests = 0
        self.blocked_in_a_row = self.block_pauses = 0
        self.asked_for: str | None = None  # why the URL being asked is asked, noted on the change it makes
        # Set by a daily run: a page asked again keeps today's links only, and this is told the targets it dropped.
        self.unlinked: Callable[[str, set[str]], None] | None = None
        recorded = self.store.get_meta("content_hash_format")
        if recorded is None:
            self.store.set_meta("content_hash_format", HASH_FORMAT)
            self.store.commit()
        elif recorded != HASH_FORMAT:
            self.log.error("content hashes in this store were made under %s, not %s: pages will look changed",
                           recorded, HASH_FORMAT)

    # Runs -------------------------------------------------------------------------------------------------------------

    def begin_run(self) -> str:
        """Start a run: the changes found until end_run name it, and the runs table records its start, end, stop
        reason and requests."""
        started = self.now()
        self.run_id = f"{self.site.name}-{started:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"
        self.run_requests = self.requests
        self.store.start_run(self.run_id, self.site.name, iso(started))
        self.store.commit()
        return self.run_id

    def end_run(self, stop_reason: str) -> None:
        if self.run_id is None:
            return
        self.store.end_run(self.run_id, ended_at=iso(self.now()), stop_reason=stop_reason,
                           requests=self.requests - self.run_requests)
        self.store.commit()
        self.run_id = None

    @contextmanager
    def run(self) -> Iterator[dict[str, str]]:
        """The work inside as a run, unless one is open already. The work names its stop reason in the dict it is
        given; a run whose work raises ends "interrupted", and what it had not committed is rolled back first: each
        answer is committed whole (its row, edition and change together) or not at all, and a URL whose answer was
        not committed is asked again."""
        own = self.run_id is None
        if own:
            self.begin_run()
        outcome = {"stopped": "interrupted"}
        try:
            yield outcome
        except BaseException:
            with suppress(sqlite3.Error):
                self.store.db.rollback()
            raise
        finally:
            if own:
                self.end_run(outcome["stopped"])

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

    def ensure_robots(self, until: datetime | None = None) -> Robots:
        """The host's robots.txt, read again once a day. While it cannot be read the crawler waits and asks again;
        RobotsUnreachable when `until` comes first."""
        if self.robots is None or self.robots_at is None or self.now() - self.robots_at > ROBOTS_MAX_AGE:
            self.refresh_robots()
            self.pause(until=until)
        while self.robots is not None and self.robots.unreachable:
            self.log.warning("robots.txt unreachable; waiting %.0f s before asking again", ROBOTS_RETRY_SECONDS)
            self.wait(ROBOTS_RETRY_SECONDS, until)
            if until is not None and self.now() >= until:
                raise RobotsUnreachable(f"robots.txt answered {self.robots.status or 'nothing'} until the time limit")
            self.refresh_robots()
            self.pause(until=until)
        assert self.robots is not None
        return self.robots

    def delay(self) -> float:
        robots_delay = self.robots.crawl_delay() if self.robots else None
        return max(self.site.min_delay, robots_delay or 0.0)

    def pause(self, at_least: float | None = None, *, until: datetime | None = None) -> None:
        """Wait the crawl delay, or longer when the server asked for it with Retry-After (capped at one hour)."""
        self.wait(max(self.delay(), min(at_least or 0.0, MAX_RETRY_AFTER_SECONDS)), until)

    def wait(self, seconds: float, until: datetime | None = None) -> None:
        """Sleep, but never past `until`: a crawl whose time limit comes during a wait stops without asking again."""
        if until is not None:
            seconds = min(seconds, max((until - self.now()).total_seconds(), 0.0))
        self.sleep(seconds)

    # Known URLs --------------------------------------------------------------------------------------------------------

    def add_known(
        self,
        url: str,
        *,
        source: str,
        discovered_from: str | None = None,
        in_sitemap: bool = False,
        lastmod: str | None = None,
    ) -> bool:
        """Record a URL the crawl learned of, queued or skipped by the site's rules; True when it was not known."""
        existing = self.store.get(url)
        if existing is not None:
            if in_sitemap:
                self.store.mark_in_sitemap(url, lastmod)
            # A file skipped because only unneeded pages linked to it is wanted once a needed page links to it.
            if existing["skip_reason"] == "not_needed" and not self.why_unneeded(url):
                self.store.update(url, state="pending", skip_reason=None, attempts=0, error=None, next_attempt_at=None)
            return False
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
        return True

    # Inventory ---------------------------------------------------------------------------------------------------------

    def seed_from_sitemaps(self) -> dict[str, int]:
        """Read robots.txt and every sitemap (following indexes) and record each listed URL."""
        read = self.read_sitemaps()
        self.store.set_meta("inventory_at", iso(self.now()))
        self.store.commit()
        return {"sitemaps": read.sitemaps, "listed": read.entries, "offsite_entries": read.offsite,
                **self.store.counts()}

    def read_sitemaps(self, until: datetime | None = None, *, keep: bool = True,
                      max_requests: int | None = None) -> SitemapRead:
        """Read robots.txt and every sitemap (following indexes and redirects), record each listed URL (add_known) and
        each sitemap's answer (the sitemaps table), and return what was read. A sitemap is refused when the host
        answers 401, 403, 429 or 451 or serves an HTML page in its place (crawl/sitemaps.py). The sitemaps left when
        `until` comes, or once the crawler has sent `max_requests`, are not asked. With keep=False a sitemap's body is
        not kept in the object store (the daily run reads OSHA's every day), only its SHA-256 in the sitemaps table."""
        robots = self.ensure_robots(until)
        queue: list[tuple[str, str | None]] = [(url, None) for url in self.site.sitemaps]
        queue += [(url, "robots.txt") for url in robots.sitemaps() if self.site.in_scope(normalize(url))]
        seen: set[str] = set()
        read = SitemapRead()
        while queue:
            url, parent = queue.pop(0)
            if url in seen:
                continue
            late = until is not None and self.now() >= until
            if late or (max_requests is not None and self.requests >= max_requests):
                why = "not asked before the time limit" if late else "not asked within the request budget"
                left = dict.fromkeys(other for other, _ in [(url, parent), *queue] if other not in seen)
                read.unread += [f"{other}: {why}" for other in left]
                break
            seen.add(url)
            fetched_at = iso(self.now())
            if not robots.allowed(url):
                self.store.record_sitemap(
                    url, parent=parent, fetched_at=fetched_at, status=None, kind=None, entries=None, sha256=None,
                    error="disallowed by robots.txt",
                )
                read.unread.append(f"{url}: disallowed by robots.txt")
                continue
            result = fetch(self.client, url, self.objects, max_bytes=MAX_SITEMAP_BYTES, keep=keep)
            self.requests += 1
            kind = entries = error = None
            try:
                if result.path is not None:
                    content = result.path.read_bytes()
                    try:
                        sitemap = parse_sitemap(content)
                    except ValueError as problem:
                        error = str(problem)[:500]
                        if served_as_html(content, result.content_type):
                            read.refused.append(f"{url}: refused, the host served an HTML page instead of the sitemap")
                        else:
                            read.unread.append(f"{url}: {error}"[:300])
                    else:
                        kind, entries = sitemap.kind, len(sitemap.entries)
                        for entry in sitemap.entries:
                            target = self.site.in_scope(normalize(entry.loc, url))
                            if target is None:
                                read.offsite += 1
                            elif sitemap.kind == "index":
                                queue.append((target, url))
                            else:
                                read.entries += 1
                                read.listed[target] = entry.lastmod
                                if self.add_known(target, source="sitemap", in_sitemap=True, lastmod=entry.lastmod,
                                                  discovered_from=url):
                                    read.added.append(target)
                elif result.status is not None and 300 <= result.status < 400:
                    target = self.site.in_scope(normalize(result.location or "", url))
                    error = f"redirect to {result.location}"
                    if target:
                        queue.append((target, url))
                    else:
                        read.unread.append(f"{url}: {error}, off the site"[:300])
                else:
                    error = result.error or f"HTTP {result.status}" + (" (over size cap)" if result.too_large else "")
                    if result.status in REFUSED_STATUSES:
                        read.refused.append(f"{url}: refused, the host answered HTTP {result.status}")
                    else:
                        read.unread.append(f"{url}: {error}"[:300])
            finally:
                self.objects.drop(result)  # a body read without keeping it (keep=False); a kept one stays
            self.store.record_sitemap(
                url, parent=parent, fetched_at=fetched_at, status=result.status, kind=kind, entries=entries,
                sha256=result.sha256, error=error,
            )
            self.store.commit()
            self.log.info("sitemap %s -> %s %s entries %s", url, result.status, kind or error, entries)
            self.pause(result.retry_after, until=until)
        read.sitemaps = len(seen)
        return read

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
        with self.run() as outcome:
            for url in urls:
                robots = self.ensure_robots()
                canonical = self.site.in_scope(normalize(url))
                if canonical is None:
                    results.append({"url": url, "state": "not on this site"})
                    continue
                if self.store.get(canonical) is None:
                    self.add_known(canonical, source="manual")
                if not robots.allowed(canonical):
                    state = self.disallowed(self.store.get(canonical))
                    self.store.commit()
                    results.append({"url": canonical, "state": state, "reason": "robots"})
                    continue
                # Back to pending, as if newly found, so the outcome is recorded exactly as the crawl would record it
                # (a held URL keeps its copy and is checked against it).
                self.store.update(canonical, state="pending", skip_reason=None, error=None, attempts=0,
                                  next_attempt_at=None)
                row = self.store.get(canonical)
                result = fetch(self.client, canonical, self.objects, max_bytes=max_bytes, max_seconds=max_seconds,
                               keep=False)
                self.requests += 1
                self.apply(row, result)
                self.store.commit()
                row = self.store.get(canonical)
                results.append({"url": canonical, "state": row["state"], "reason": row["skip_reason"],
                                "status": row["http_status"], "bytes": row["content_length"], "sha256": row["sha256"],
                                "error": row["error"]})
                self.log.info("fetched on request %s -> %s %s bytes", canonical, row["state"], row["content_length"])
                self.pause(result.retry_after)
            outcome["stopped"] = "done"
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
        unless already known. URLs that already have an outcome keep their records, a held URL queued for a check
        among them."""
        counts = {"alias": 0, "queued": 0}
        for row in self.store.waiting():
            url = row["url"]
            canonical = self.site.in_scope(normalize(url))
            if canonical is None or canonical == url or holds(row):
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
        """'by_rule' when the site's rules name the URL; 'listed_only_in_unneeded_listing' for a page or file that only
        such a listing links to; 'linked_only_from_unneeded' for a file every known link to which comes from a page the
        rules name (unless it lies under the kept paths); None when it is needed. Other pages are never judged by their
        links alone."""
        if self.site.unneeded(url):
            return "by_rule"
        pages = [self.site.in_scope(normalize(page)) or page for page in self.store.linking_pages(url)]
        if pages and self.site.unneeded_listings and all(self.site.unneeded_listing(page) for page in pages):
            return "listed_only_in_unneeded_listing"
        if is_page(url) or self.site.kept(url):
            return None
        if pages and all(self.site.unneeded(page) for page in pages):
            return "linked_only_from_unneeded"
        return None

    def skip_unneeded(self) -> dict[str, int]:
        """Bring the queue in line with the site's current not-needed rules: waiting URLs they cover are skipped as
        "not_needed", and URLs skipped so that the rules no longer cover are queued again. Nothing downloaded changes:
        a held URL queued for a check is checked."""
        counts = {"by_rule": 0, "listed_only_in_unneeded_listing": 0, "linked_only_from_unneeded": 0, "queued_again": 0}
        for row in self.store.waiting():
            reason = None if holds(row) else self.why_unneeded(row["url"])
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
        or the site keeps answering like a block. `until` is a deadline: no wait runs past it, an unreachable
        robots.txt included ("robots.txt unreachable"); only a download already under way can."""
        with self.run() as outcome:
            run_id = self.run_id
            try:
                stopped = self.work(max_requests, until)
            except RobotsUnreachable as problem:
                stopped = "robots.txt unreachable"
                self.log.warning("stopping: %s", problem)
            outcome["stopped"] = stopped
            self.store.set_meta("last_stop", f"{iso(self.now())} {stopped}")
            self.store.commit()
            self.log.info("stopped (%s) after %d requests; states %s", stopped, self.requests, self.store.counts())
            return {"stopped": stopped, "requests": self.requests, "states": self.store.counts(), "run_id": run_id}

    def work(self, max_requests: int | None, until: datetime | None) -> str:
        """The crawl's queue worked through; returns why it stopped."""
        self.ensure_robots(until)
        self.tidy_queue()
        self.blocked_in_a_row = self.block_pauses = 0
        while True:
            if max_requests is not None and self.requests >= max_requests:
                return "request limit"
            if until is not None and self.now() >= until:
                return "time limit"
            robots = self.ensure_robots(until)
            row = self.store.next_due(iso(self.now()))
            if row is None:
                earliest = self.store.earliest_retry()
                if earliest is None:
                    return "done"
                self.wait(min(max((parse_iso(earliest) - self.now()).total_seconds(), 1.0), 600.0), until)
                continue
            _, stopped = self.ask(row, robots, until)
            if stopped:
                return stopped

    def tidy_queue(self) -> None:
        """The waiting URLs brought in line with the site's current rules (aliases, not needed, size limits)."""
        folded = self.fold_aliases()
        if any(folded.values()):
            self.log.info("waiting URLs recorded under an alias, folded into their canonical URL: %s", folded)
        unneeded = self.skip_unneeded()
        if any(unneeded.values()):
            self.log.info("waiting URLs the project does not need, skipped: %s", unneeded)
        requeued = self.requeue_policy_skips()
        if any(requeued.values()):
            self.log.info("queued again under the current download rules: %s", requeued)

    def ask(self, row: sqlite3.Row, robots: Robots, until: datetime | None = None, *,
            why: str | None = None) -> tuple[FetchResult | None, str | None]:
        """One request for the URL of `row`, under every rule the crawl keeps: not asked when robots.txt disallows it
        (recorded as such), the validators of a verified copy sent, the answer recorded and committed whole, then a
        stop at the byte budget or on a sustained block, or the pause before the next request (the crawl delay, or a
        longer Retry-After, never past `until`). `why`, when given, is noted on the change the answer records. Returns
        the answer (None when the URL was not asked) and the reason to stop, if there is one."""
        if not robots.allowed(row["url"]):
            self.disallowed(row)
            self.store.commit()
            return None, None
        if unexamined(row) and self.objects.path_for(row["sha256"]).exists():
            # A copy recorded before content hashes, examined before it is asked about, so that only a verified copy
            # sends validators. One whose object is not kept (the VM) is asked for whole, and examined from the answer
            # when that is the same bytes (see apply).
            examine_held(self.store, self.objects.root, row)
            row = self.store.get(row["url"])
        conditional = row["content_sha256"] is not None
        result = fetch(
            self.client, row["url"], self.objects, etag=row["etag"] if conditional else None,
            last_modified=row["last_modified"] if conditional else None, max_bytes=self.site.max_bytes,
            max_seconds=self.site.max_seconds, keep=False,
        )
        self.requests += 1
        self.asked_for = why
        try:
            self.apply(row, result)
        finally:
            self.asked_for = None
        self.store.commit()
        if self.requests % PROGRESS_EVERY == 0:
            self.log.info("requests %d states %s", self.requests, self.store.counts())
            if self.store.fetched_bytes() >= self.site.byte_budget:
                return result, "byte budget"
        self.blocked_in_a_row = self.blocked_in_a_row + 1 if looks_blocked(result) else 0
        if self.blocked_in_a_row >= BLOCK_THRESHOLD:
            self.block_pauses += 1
            if self.block_pauses > MAX_BLOCK_PAUSES:
                return result, "possible block"
            self.log.warning("%d blocked responses in a row; pausing %.0f s", self.blocked_in_a_row,
                             BLOCK_PAUSE_SECONDS)
            self.wait(BLOCK_PAUSE_SECONDS, until)
            self.blocked_in_a_row = 0
        self.pause(result.retry_after, until=until)
        return result, None

    def disallowed(self, row: sqlite3.Row) -> str:
        """A URL that robots.txt disallows, recorded without asking: skipped "robots", or, when the store holds a copy,
        that copy kept current with the reason noted, as for any refusal. Returns the state it is left in."""
        if holds(row):
            self.check_failed(row, self.now(), "disallowed by robots.txt")
            return self.held_state(row)
        self.store.update(row["url"], state="skipped", skip_reason="robots")
        return "skipped"

    def apply(self, row: sqlite3.Row, result: FetchResult) -> None:
        """Record the answer to a request for the URL of `row` (its row as it was when asked). A 2xx body waits staged
        in the object store until this keeps or drops it."""
        url, moment = row["url"], self.now()
        self.store.add_fetch(
            url, iso(moment), status=result.status, sha256=result.sha256, size=result.size, etag=result.etag,
            last_modified=result.last_modified, error=result.error,
        )
        try:
            if unexamined(row):  # a copy recorded before content hashes existed: examined now, as the backfill does,
                # from its object or, where that is not kept (the VM), from this answer when it is the same bytes;
                # with neither it stays unexamined, and other bytes are a change that cannot be compared
                same = result.path if result.staged and result.sha256 == row["sha256"] else None
                examine_held(self.store, self.objects.root, row, same_bytes=same)
                row = self.store.get(url)
            if holds(row):
                self.check(row, result, moment)
            else:
                self.first_answer(row, result, moment)
        finally:
            self.objects.drop(result)  # whatever was not kept

    def first_answer(self, row: sqlite3.Row, result: FetchResult, moment: datetime) -> None:
        """An answer for a URL the store holds no copy of, under the first download's rules."""
        url, now = row["url"], iso(moment)
        attempts = row["attempts"] + 1
        status = result.status
        common = {"http_status": status, "fetched_at": now, "attempts": attempts, "next_attempt_at": None}
        if status is None or (result.error and not result.too_large):
            self.retry_or_fail(url, attempts, status, result.error or "no response")
        elif 200 <= status < 300 and result.too_large:
            self.store.update(url, state="skipped", skip_reason="size_cap", content_type=result.content_type,
                              content_length=result.size, error=None, **common)
            self.refused(url, result, self.too_large(result), now)
        elif 200 <= status < 300:
            verdict = examine(result.path, url=url, content_type=result.content_type, sha256=result.sha256)
            if not verdict.usable:
                self.objects.drop(result)
                if self.retry_or_fail(url, attempts, status, f"rejected {verdict.reason}: {verdict.note}",
                                      final="rejected"):
                    self.refused(url, result, verdict, now)
                return
            content = self.page_content(result)
            self.objects.keep(result)
            self.store.update(url, state="fetched", content_type=result.content_type, content_length=result.size,
                              sha256=result.sha256, etag=result.etag, last_modified=result.last_modified, error=None,
                              content_sha256=verdict.content_sha256, detected_type=verdict.detected_type,
                              last_checked_at=now, next_check_at=due_after(now, verdict.detected_type), **common)
            self.new_edition(url, result, verdict, now, "new", previous=None)
            if content is not None:
                self.discover(url, content)
        elif status == 304:
            self.store.update(url, state="unchanged", error=None, **common)
        elif 300 <= status < 400:
            location, _ = self.follow_redirect(url, result)
            self.store.update(url, state="redirect", location=location, error=None, **common)
        elif status in (404, 410):
            self.store.update(url, state="not_found", error=None, **common)
        elif status in RETRY_STATUSES:
            self.retry_or_fail(url, attempts, status, f"HTTP {status}")
        else:
            self.store.update(url, state="error", error=f"HTTP {status}", **common)

    def check(self, row: sqlite3.Row, result: FetchResult, moment: datetime) -> None:
        """An answer for a URL the store holds a copy of: only a usable new content, a 404 or 410, or a redirect changes
        what is held (see the module docstring)."""
        url, now, status = row["url"], iso(moment), result.status
        answered = {"http_status": status, "fetched_at": now, "attempts": row["attempts"] + 1, "next_attempt_at": None,
                    "error": None, "last_checked_at": now}
        gone = row["gone_since"] is not None
        if status is None or (result.error and not result.too_large):
            self.check_failed(row, moment, result.error or "no response")
        elif 200 <= status < 300 and result.too_large:
            verdict = self.too_large(result)
            self.check_failed(row, moment, f"rejected too_large: {verdict.note}")
            self.refused(url, result, verdict, now)
        elif 200 <= status < 300:
            self.check_body(row, result, moment)
        elif status == 304:
            if gone:
                answered |= {"gone_since": None, "purge_after": None}
                self.record(url, "restored", now, **self.held_fields(row), http_status=status)
            self.store.update(url, state="unchanged", next_check_at=due_after(now, row["detected_type"]), **answered)
        elif 300 <= status < 400:
            location, target = self.follow_redirect(url, result)
            moved_before = (row["location"] == location and row["http_status"] is not None
                            and 300 <= row["http_status"] < 400 and row["http_status"] != 304)
            self.store.update(url, state="redirect", location=location, next_check_at=iso(moment + RECHECK_GONE),
                              **answered)
            if not moved_before:
                self.record(url, "moved", now, **self.held_fields(row), http_status=status, target=target)
        elif status in (404, 410):
            if not gone:
                answered |= {"gone_since": now, "purge_after": iso(moment + PURGE_AFTER)}
                self.record(url, "gone", now, **self.held_fields(row), http_status=status)
            self.store.update(url, state="not_found", next_check_at=iso(moment + RECHECK_GONE), **answered)
        else:  # a refusal, an error: nothing retired
            self.check_failed(row, moment, f"HTTP {status}")

    def check_body(self, row: sqlite3.Row, result: FetchResult, moment: datetime) -> None:
        """A 2xx body for a held URL: refused, the same content again, or a new edition."""
        url, now = row["url"], iso(moment)
        same_bytes = result.sha256 == row["sha256"]
        if same_bytes and row["content_sha256"] is not None and row["detected_type"] is not None:
            verdict = Verdict(row["detected_type"], row["content_sha256"])  # the very bytes held, examined before
        else:
            verdict = examine(result.path, url=url, content_type=result.content_type, sha256=result.sha256)
        if not verdict.usable:
            self.objects.drop(result)
            self.check_failed(row, moment, f"rejected {verdict.reason}: {verdict.note}")
            self.refused(url, result, verdict, now)
            return
        content = self.page_content(result)
        confirmed = {"attempts": row["attempts"] + 1, "next_attempt_at": None, "error": None, "last_checked_at": now,
                     "next_check_at": due_after(now, verdict.detected_type), "gone_since": None, "purge_after": None}
        gone = row["gone_since"] is not None
        if verdict.content_sha256 == row["content_sha256"]:
            self.objects.drop(result)  # nothing new to keep
            if same_bytes and (result.etag or result.last_modified):
                # The validators the server gives these very bytes now (a file put back unchanged gets a new ETag),
                # so that the next check can be answered 304 instead of the whole file again.
                confirmed |= {"etag": result.etag, "last_modified": result.last_modified}
            state = self.held_state(row)
            if state in ("redirect", "not_found"):  # it answers with its content again
                self.store.update(url, state="fetched", http_status=result.status, fetched_at=now, **confirmed)
                if gone:
                    self.record(url, "restored", now, **self.held_fields(row), http_status=result.status)
            else:
                self.store.update(url, state=state, **confirmed)
        else:
            self.objects.keep(result)
            self.store.update(url, state="fetched", content_type=result.content_type, content_length=result.size,
                              sha256=result.sha256, etag=result.etag, last_modified=result.last_modified,
                              http_status=result.status, fetched_at=now, content_sha256=verdict.content_sha256,
                              detected_type=verdict.detected_type, **confirmed)
            self.new_edition(url, result, verdict, now, "restored" if gone else "changed",
                             previous=row["content_sha256"])
        if content is not None:
            self.discover(url, content, replace=self.unlinked is not None)

    def check_failed(self, row: sqlite3.Row, moment: datetime, why: str) -> None:
        """A held URL whose answer gave nothing usable: its copy stays current and its record as it was, the reason is
        noted, and the URL is due again a day later."""
        self.store.update(row["url"], state=self.held_state(row), attempts=row["attempts"] + 1, next_attempt_at=None,
                          error=f"{why} at {iso(moment)}; the copy held stays current"[:500],
                          next_check_at=iso(moment + RECHECK_FAILED))

    @staticmethod
    def held_state(row: sqlite3.Row) -> str:
        """The state a held URL keeps when an answer leaves its record as it was: its own, or, while it is queued,
        the one its latest recorded answer gave it."""
        if row["state"] in ("fetched", "unchanged", "redirect", "not_found"):
            return row["state"]
        status = row["http_status"] or 200
        if row["gone_since"] or status in (404, 410):
            return "not_found"
        if status == 304:
            return "unchanged"
        return "redirect" if 300 <= status < 400 else "fetched"

    @staticmethod
    def held_fields(row: sqlite3.Row) -> dict[str, object]:
        """A change row's description of the copy held."""
        return {"sha256": row["sha256"], "content_sha256": row["content_sha256"], "bytes": row["content_length"],
                "content_type": row["content_type"], "detected_type": row["detected_type"]}

    def too_large(self, result: FetchResult) -> Verdict:
        return Verdict(None, None, "too_large",
                       f"{result.size:,} bytes or more, over the {self.site.max_bytes:,}-byte limit")

    @staticmethod
    def page_content(result: FetchResult) -> bytes | None:
        """A page's bytes, to find its links in (a page over MAX_HTML_PARSE_BYTES is not read)."""
        if is_html(result.content_type) and result.path is not None and (result.size or 0) <= MAX_HTML_PARSE_BYTES:
            return result.path.read_bytes()
        return None

    def follow_redirect(self, url: str, result: FetchResult) -> tuple[str | None, str | None]:
        """Record a redirect's target as a link of the URL and queue it when it lies on this site. Returns the location
        to record (the target as an absolute URL) and the target as a change row names it (its canonical form when it
        lies on this site)."""
        target = normalize(result.location or "", url)
        if not target:
            return result.location, result.location
        canonical = self.site.in_scope(target)
        self.store.add_links(url, [canonical or target])
        if canonical and canonical != url:
            self.add_known(canonical, source="redirect", discovered_from=url)
        return target, canonical or target

    def new_edition(self, url: str, result: FetchResult, verdict: Verdict, now: str, change: str, *,
                    previous: str | None) -> None:
        self.store.add_edition(url, content_sha256=verdict.content_sha256, sha256=result.sha256, size=result.size,
                               content_type=result.content_type, detected_type=verdict.detected_type,
                               first_seen_at=now)
        note = verdict.note or ("the copy held before had no content hash" if change == "changed" and not previous
                                else None)
        self.record(url, change, now, sha256=result.sha256, content_sha256=verdict.content_sha256,
                    previous_content_sha256=previous, bytes=result.size, content_type=result.content_type,
                    detected_type=verdict.detected_type, http_status=result.status, note=note)

    def refused(self, url: str, result: FetchResult, verdict: Verdict, now: str) -> None:
        self.record(url, "rejected", now, sha256=result.sha256, bytes=result.size, content_type=result.content_type,
                    detected_type=verdict.detected_type, http_status=result.status, reason=verdict.reason,
                    note=verdict.note)

    def record(self, url: str, change: str, now: str, **fields: object) -> None:
        if self.asked_for:
            fields["note"] = "; ".join(filter(None, (fields.get("note"), self.asked_for)))
        self.store.add_change(url, change, observed_at=now, run_id=self.run_id, **fields)

    def retry_or_fail(self, url: str, attempts: int, status: int | None, error: str, *, final: str = "error") -> bool:
        """Schedule another attempt, or after the last one record the final state; True when the outcome is final."""
        now = self.now()
        if attempts <= len(RETRY_DELAYS):
            self.store.update(url, state="retry", http_status=status, attempts=attempts, error=error,
                              next_attempt_at=iso(now + RETRY_DELAYS[attempts - 1]), fetched_at=iso(now))
            return False
        self.store.update(url, state=final, http_status=status, attempts=attempts, error=error,
                          next_attempt_at=None, fetched_at=iso(now))
        return True

    def discover(self, page_url: str, content: bytes, *, replace: bool = False) -> None:
        """Record a page's links and queue the targets on this site it is the first to name. With `replace` (a page
        asked again by a daily run) the page keeps today's links only, and the daily run is told the ones it dropped."""
        found = [(self.site.in_scope(link), link) for link in extract_links(content, page_url)]
        targets = [canonical or link for canonical, link in found]
        # The edges go in first, so that a target's needs are judged with this page's link already known.
        if replace:
            dropped = self.store.replace_links(page_url, targets)
            if dropped and self.unlinked is not None:
                self.unlinked(page_url, dropped)
        else:
            self.store.add_links(page_url, targets)
        for canonical, _ in found:
            if canonical and canonical != page_url:
                self.add_known(canonical, source="discovered", discovered_from=page_url)
