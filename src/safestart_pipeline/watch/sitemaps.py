"""Changed pages on the MSHA and OSHA websites, found by reading each site's sitemaps against its crawl's inventory.

A page is "added" when today's sitemaps list a URL the crawl never recorded, or one the crawl's inventory first
recorded after the watcher's previous read of the site (the daily run of the official files, official/daily.py, reads
OSHA's sitemap too and records its new URLs, often before the watcher sees them). It is "changed" when its sitemap
lastmod is after the crawl last fetched it, and "dropped" when the crawl's inventory has it from a sitemap and today's
sitemaps no longer list it. The inventory is only read, so a page the crawl never recorded stays unknown to it: once the
watcher has announced such a page as added, a later lastmod of it is a change. Dropped pages are judged only when every
sitemap could be read, since an unread sitemap would seem to drop all of its pages. Each finding becomes a page_change
event keyed by the URL, the change and the lastmod, so a change is reported once while a later edit of the same page is
reported again. A page the official feed's check (feed.py) announced as new, after the daily run downloaded it, is not
announced as added again: one new page, one event, whichever check sees it first.

A host that refuses a sitemap (crawl/sitemaps.py: HTTP 401, 403, 429 or 451, or an HTML page in the sitemap's place)
is recorded, never worked around: MSHA's site serves the cloud VM such a page, and a workstation the sitemap. The check
is "refused" and its note says which sitemaps were refused and why. The check is an "error" instead when any sitemap
could not be read for another reason (no answer, another HTTP status, malformed XML).

The watcher asks the way the crawl does: robots.txt through crawl/robots.py, the site's own delay or its Crawl-delay if
longer, and not while that site's crawl or daily run writes its inventory. Where it can write the data root's state
folder, it holds the crawl's lock while it asks. Where it cannot, as the containers mount the data root read-only, a
crawl may still run outside them: the workstation's crawls run on the host, into the folder the stack mounts, and their
file locks do not reach into a container. There the site is skipped while anything its crawl writes changed within
CRAWL_QUIET: the inventory or its -wal (a commit after every request) or a download in progress
(`raw/<site>/objects/tmp`). A crawl that has just stopped is waited for as long. A check skipped this way is due again
BUSY_RETRY later rather than a whole interval later, so a daily run that writes OSHA's inventory from 16:30 to 04:30 UTC
cannot push OSHA's check into its window day after day: the check comes once the run has ended, and its next time falls
outside the window. OSHA asks for 10 seconds between requests, so its sitemaps are read at most once a day whatever
the schedule; OSHA's sitemap gives no lastmod, so only added and dropped pages are found there.
"""

from __future__ import annotations

import os
import time
from collections import Counter
from collections.abc import Callable, Iterable
from contextlib import AbstractContextManager, ExitStack, nullcontext, suppress
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from ..crawl.locks import SiteLock
from ..crawl.sitemaps import REFUSED_STATUSES, Sitemap, parse_sitemap, served_as_html
from ..crawl.sites import Site
from ..crawl.urls import normalize
from ..stream.events import Event, as_utc
from ..verify import read_only
from .checks import Check
from .feed import KEY_PREFIX as FEED_KEY_PREFIX
from .polite import Answer, PoliteClient
from .publish import Publisher, publish_new
from .schedule import BUSY_RETRY
from .state import WatchState

# The least time between two reads of a site's sitemaps, whatever the schedule or a forced check asks.
MIN_INTERVAL = {"msha": timedelta(0), "osha": timedelta(days=1)}
# Where the crawl's lock cannot be seen, a crawl is taken to run until what it writes has been still this long. A crawl
# commits its inventory after every request and a long download grows its file all the while; only the crawl's own
# pauses are longer (a Retry-After or a suspected block, while it sends nothing).
CRAWL_QUIET = timedelta(minutes=15)
PARAMETERS = 500  # URLs per inventory query
TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"  # the crawl inventory's times
CHANGES = ("added", "changed", "dropped")

Listed = dict[str, tuple[str | None, str]]  # URL -> (lastmod, the sitemap listing it)


@dataclass(frozen=True)
class Change:
    url: str
    change: str  # added, changed or dropped
    lastmod: str | None
    sitemap: str | None  # the sitemap listing it today (none for a dropped page)


def utc_now() -> datetime:
    return datetime.now(UTC)


def modified_after(lastmod: str | None, fetched_at: str | None) -> bool:
    """Whether a sitemap lastmod (W3C datetime, any offset) is after a fetch time (UTC); a bare date counts by day."""
    if not lastmod or not fetched_at:
        return False
    try:
        fetched = in_utc(datetime.fromisoformat(fetched_at))
        if len(lastmod) == 10:
            return date.fromisoformat(lastmod) > fetched.date()
        return in_utc(datetime.fromisoformat(lastmod)) > fetched
    except ValueError:
        return False


def in_utc(moment: datetime) -> datetime:
    """A time without an offset is taken as UTC (the crawl writes UTC; W3C datetimes should carry their offset)."""
    return moment.astimezone(UTC) if moment.tzinfo else moment.replace(tzinfo=UTC)


def lastmod_time(lastmod: str | None) -> str | None:
    try:
        return as_utc(lastmod) if lastmod else None
    except ValueError:
        return None


class SitemapClient(PoliteClient):
    """A PoliteClient that also keeps the Content-Type each URL answered with, which its answers leave out."""

    def __init__(self, client: httpx.Client, **options: Any) -> None:
        super().__init__(client, **options)
        self.content_types: dict[str, str] = {}

    def _read(self, response: httpx.Response, url: str) -> bytes:
        self.content_types[url] = response.headers.get("content-type", "")
        return super()._read(response, url)


class Refused(Exception):
    """The host would not serve a sitemap to the watcher; the message says how it refused."""


def html_page(answer: Answer, content_type: str | None) -> bool:
    """Whether an answer is an HTML page, by its Content-Type or by its first bytes."""
    return served_as_html(answer.content, content_type)


def read_sitemap(polite: SitemapClient, url: str) -> tuple[Answer, Sitemap]:
    """One sitemap and the answer it came in. Refused when the host will not serve it; httpx.HTTPError,
    PermissionError (robots.txt) or ValueError when it cannot be read otherwise. A readable sitemap is read whatever
    its Content-Type."""
    answer = polite.get(url)
    if answer.status in REFUSED_STATUSES:
        raise Refused(f"the host answered HTTP {answer.status} ({httpx.codes.get_reason_phrase(answer.status)})")
    if not 200 <= answer.status < 300:
        raise ValueError(f"HTTP {answer.status}")
    try:
        return answer, parse_sitemap(answer.content)
    except ValueError as problem:
        if html_page(answer, polite.content_types.get(answer.url)):
            raise Refused("the host served an HTML page instead of the sitemap") from problem
        raise


def read_sitemaps(site: Site, polite: SitemapClient) -> tuple[Listed, list[str], list[str]]:
    """Every in-scope URL today's sitemaps list (following indexes), then the sitemaps the host refused and those that
    could not be read for another reason, each as "<sitemap>: <why>"."""
    robots = polite.robots(site.canonical_host)
    queue = [*site.sitemaps, *filter(None, (site.in_scope(normalize(url)) for url in robots.sitemaps()))]
    seen: set[str] = set()
    listed: Listed = {}
    refused: list[str] = []
    problems: list[str] = []
    while queue:
        url = queue.pop(0)
        if url in seen:
            continue
        seen.add(url)
        try:
            answer, sitemap = read_sitemap(polite, url)
        except Refused as refusal:
            refused.append(f"{url}: refused, {refusal}"[:300])
            continue
        except (httpx.HTTPError, PermissionError, ValueError) as problem:
            problems.append(f"{url}: {problem}"[:300])
            continue
        for entry in sitemap.entries:
            target = site.in_scope(normalize(entry.loc, answer.url))
            if target is not None and sitemap.kind == "index":
                queue.append(target)
            elif target is not None:
                listed[target] = (entry.lastmod, url)
    return listed, refused, problems


def compare(inventory: Path, listed: Listed, *, judge_dropped: bool, since: datetime | None = None) -> list[Change]:
    """Today's sitemaps against the crawl's inventory, which is only read. `since` is the watcher's previous read of
    these sitemaps: a URL the inventory first recorded after it is added too."""
    changes: list[Change] = []
    recorded_after = since.astimezone(UTC).strftime(TIME_FORMAT) if since else None
    db = read_only(inventory)
    try:
        urls = list(listed)
        for start in range(0, len(urls), PARAMETERS):
            chunk = urls[start:start + PARAMETERS]
            marks = ", ".join("?" for _ in chunk)
            known = {row["url"]: row for row in
                     db.execute(f"select url, fetched_at, first_seen_at from urls where url in ({marks})", chunk)}
            for url in chunk:
                lastmod, sitemap = listed[url]
                row = known.get(url)
                if row is None or (recorded_after and row["first_seen_at"] > recorded_after):
                    changes.append(Change(url, "added", lastmod, sitemap))
                elif modified_after(lastmod, row["fetched_at"]):
                    changes.append(Change(url, "changed", lastmod, sitemap))
        if judge_dropped:
            for row in db.execute("select url, sitemap_lastmod from urls where in_sitemap = 1"):
                if row["url"] not in listed:
                    changes.append(Change(row["url"], "dropped", row["sitemap_lastmod"], None))
    finally:
        db.close()
    return changes


def page_key(change: Change) -> str:
    """A page_change event's source key: the URL, the change and the lastmod, so a change is reported once."""
    return f"{change.url}|{change.change}|{change.lastmod or ''}"


def page_event(site: Site, change: Change, checked_at: datetime) -> Event:
    """A page_change event; it happened at the page's own lastmod when the sitemap gives one, else at the check."""
    occurred = lastmod_time(change.lastmod) if change.change != "dropped" else None
    return Event.new(
        kind="page_change", key=page_key(change), mode="live", occurred_at=occurred or checked_at,
        title=f"{site.name.upper()} page {change.change}: {change.url}",
        details={"site": site.name, "url": change.url, "lastmod": change.lastmod, "change": change.change},
        source={"site": site.name, **({"sitemap": change.sitemap} if change.sitemap else {})})


def announced(changes: Iterable[Change], state: WatchState) -> list[Change]:
    """The changes as the stream reports them: a page the crawl never recorded that the watcher announced as added
    under another lastmod before has changed since, and a page the official feed's check announced as new is left out
    (it was announced once already)."""
    reported = []
    for change in changes:
        if change.change == "added" and not state.emitted([f"page_change:{page_key(change)}"]):
            # The ids of the page as added all start like the id of an added page without a lastmod.
            if state.any_emitted_starting(f"page_change:{page_key(replace(change, lastmod=None))}"):
                change = replace(change, change="changed")
            elif state.any_emitted_starting(f"page_change:{FEED_KEY_PREFIX}{change.url}|new|"):
                continue
        reported.append(change)
    return reported


def crawl_writes(data_root: Path, site: Site) -> list[Path]:
    """What the site's crawl writes while it runs: its inventory, the inventory's -wal, and downloads in progress."""
    inventory = data_root / "state" / f"crawl-{site.name}.sqlite"
    paths = [inventory, inventory.with_name(f"{inventory.name}-wal")]
    with suppress(OSError):  # no download folder: nothing being downloaded
        paths += (data_root / "raw" / site.name / "objects" / "tmp").iterdir()
    return paths


def crawl_lock(data_root: Path, site: Site, now: datetime) -> AbstractContextManager[object]:
    """The site's crawl lock where the state folder can be written. Where it cannot, RuntimeError, as from a lock
    another process holds, while anything the crawl writes changed within CRAWL_QUIET of `now`; else nothing to hold."""
    state = data_root / "state"
    if os.access(state, os.W_OK):
        return SiteLock(state / f"crawl-{site.name}.lock")
    for path in crawl_writes(data_root, site):
        with suppress(OSError):  # gone meanwhile (a finished download, a checkpointed -wal)
            changed = datetime.fromtimestamp(path.stat().st_mtime, UTC)
            if now - changed < CRAWL_QUIET:
                raise RuntimeError(f"{path.name} changed at {changed:%Y-%m-%d %H:%M} UTC, in the last"
                                   f" {CRAWL_QUIET.seconds // 60} minutes")
    return nullcontext()


def check_sitemaps(site: Site, data_root: Path, state: WatchState, publisher: Publisher, client: httpx.Client, *,
                   sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.monotonic,
                   now: Callable[[], datetime] = utc_now) -> Check:
    source, started = f"sitemaps-{site.name}", now()
    last_read, floor = state.time(f"{source}:read_at"), MIN_INTERVAL.get(site.name, timedelta(0))
    if last_read is not None and started - last_read < floor:
        return Check(source, "skipped", note=f"read at {last_read:%Y-%m-%d %H:%M} UTC; at most once every {floor}")
    inventory = data_root / "state" / f"crawl-{site.name}.sqlite"
    if not inventory.exists():
        return Check(source, "skipped", note=f"no {site.name} crawl inventory to compare with")
    with ExitStack() as stack:
        try:
            stack.enter_context(crawl_lock(data_root, site, started))
        except RuntimeError as busy:
            return Check(source, "skipped", note=f"the {site.name} crawl is running ({busy}), so its host was not"
                                                 f" asked; asked again in {BUSY_RETRY}", retry_in=BUSY_RETRY)
        state.set_time(f"{source}:read_at", started)
        state.commit()
        polite = SitemapClient(client, min_delay=site.min_delay, sleep=sleep, clock=clock)
        listed, refused, problems = read_sitemaps(site, polite)
    judged = not (refused or problems)  # dropped pages need every sitemap
    compared = compare(inventory, listed, judge_dropped=judged, since=last_read)
    changes = announced(compared, state)
    found = publish_new((page_event(site, change, started) for change in changes), publisher, state)
    counts = Counter(change.change for change in changes)
    note = f"{len(listed):,} URLs listed; " + ", ".join(f"{counts[name]:,} {name}" for name in CHANGES
                                                       if judged or name != "dropped")
    if already := len(compared) - len(changes):
        note += (f"; {already:,} added {'page' if already == 1 else 'pages'} left out, announced as new by the official"
                 " feed's check already")
    if not judged:
        unread = len(problems) + len(refused)
        note += (f", dropped pages not judged; {unread} sitemap{'' if unread == 1 else 's'} not read: "
                 + "; ".join([*problems[:3], *refused[:3]]))
    status = "error" if problems else "refused" if refused else "ok" if found else "unchanged"
    return Check(source, status, found, note)
