"""Changed pages on the MSHA and OSHA websites, found by reading each site's sitemaps against its crawl's inventory.

A page is "added" when today's sitemaps list a URL the crawl never recorded, "changed" when its sitemap lastmod is after
the crawl last fetched it, and "dropped" when the crawl's inventory has it from a sitemap and today's sitemaps no longer
list it. Dropped pages are judged only when every sitemap could be read, since an unreadable sitemap would seem to drop
all of its pages. Each finding becomes a page_change event keyed by the URL, the change and the lastmod, so a change is
reported once while a later edit of the same page is reported again.

The watcher asks the way the crawl does: robots.txt through crawl/robots.py, the site's own delay or its Crawl-delay if
longer, and never while that site's crawl runs (it holds the crawl's lock while it asks). A data root the watcher cannot
write, as the containers mount it, holds no running crawl, since a crawl writes its state there, so there it asks
without the lock. The inventory is only read.
OSHA asks for 10 seconds between requests, so its sitemaps are read at most once a day whatever the schedule; OSHA's
sitemap gives no lastmod, so only added and dropped pages are found there.
"""

from __future__ import annotations

import os
import time
from collections import Counter
from collections.abc import Callable
from contextlib import AbstractContextManager, ExitStack, nullcontext
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx

from ..crawl.locks import SiteLock
from ..crawl.sitemaps import parse_sitemap
from ..crawl.sites import Site
from ..crawl.urls import normalize
from ..stream.events import Event, as_utc
from ..verify import read_only
from .checks import Check
from .polite import PoliteClient
from .publish import Publisher, publish_new
from .state import WatchState

# The least time between two reads of a site's sitemaps, whatever the schedule or a forced check asks.
MIN_INTERVAL = {"msha": timedelta(0), "osha": timedelta(days=1)}
PARAMETERS = 500  # URLs per inventory query
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


def read_sitemaps(site: Site, polite: PoliteClient) -> tuple[Listed, list[str]]:
    """Every in-scope URL today's sitemaps list (following indexes), and the sitemaps that could not be read."""
    robots = polite.robots(site.canonical_host)
    queue = [*site.sitemaps, *filter(None, (site.in_scope(normalize(url)) for url in robots.sitemaps()))]
    seen: set[str] = set()
    listed: Listed = {}
    problems: list[str] = []
    while queue:
        url = queue.pop(0)
        if url in seen:
            continue
        seen.add(url)
        try:
            answer = polite.get(url)
            if not 200 <= answer.status < 300:
                raise ValueError(f"HTTP {answer.status}")
            sitemap = parse_sitemap(answer.content)
        except (httpx.HTTPError, PermissionError, ValueError) as problem:
            problems.append(f"{url}: {problem}"[:300])
            continue
        for entry in sitemap.entries:
            target = site.in_scope(normalize(entry.loc, answer.url))
            if target is not None and sitemap.kind == "index":
                queue.append(target)
            elif target is not None:
                listed[target] = (entry.lastmod, url)
    return listed, problems


def compare(inventory: Path, listed: Listed, *, judge_dropped: bool) -> list[Change]:
    """Today's sitemaps against the crawl's inventory, which is only read."""
    changes: list[Change] = []
    db = read_only(inventory)
    try:
        urls = list(listed)
        for start in range(0, len(urls), PARAMETERS):
            chunk = urls[start:start + PARAMETERS]
            marks = ", ".join("?" for _ in chunk)
            fetched = {row["url"]: row["fetched_at"] for row in
                       db.execute(f"select url, fetched_at from urls where url in ({marks})", chunk)}
            for url in chunk:
                lastmod, sitemap = listed[url]
                if url not in fetched:
                    changes.append(Change(url, "added", lastmod, sitemap))
                elif modified_after(lastmod, fetched[url]):
                    changes.append(Change(url, "changed", lastmod, sitemap))
        if judge_dropped:
            for row in db.execute("select url, sitemap_lastmod from urls where in_sitemap = 1"):
                if row["url"] not in listed:
                    changes.append(Change(row["url"], "dropped", row["sitemap_lastmod"], None))
    finally:
        db.close()
    return changes


def page_event(site: Site, change: Change, checked_at: datetime) -> Event:
    """A page_change event; it happened at the page's own lastmod when the sitemap gives one, else at the check."""
    occurred = lastmod_time(change.lastmod) if change.change != "dropped" else None
    return Event.new(
        kind="page_change", key=f"{change.url}|{change.change}|{change.lastmod or ''}", mode="live",
        occurred_at=occurred or checked_at, title=f"{site.name.upper()} page {change.change}: {change.url}",
        details={"site": site.name, "url": change.url, "lastmod": change.lastmod, "change": change.change},
        source={"site": site.name, **({"sitemap": change.sitemap} if change.sitemap else {})})


def crawl_lock(data_root: Path, site: Site) -> AbstractContextManager[object]:
    """The site's crawl lock, or nothing to hold where the state folder cannot be written (no crawl runs there)."""
    state = data_root / "state"
    if not os.access(state, os.W_OK):
        return nullcontext()
    return SiteLock(state / f"crawl-{site.name}.lock")


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
            stack.enter_context(crawl_lock(data_root, site))
        except RuntimeError:
            return Check(source, "skipped", note=f"the {site.name} crawl is running, so its host was not asked")
        state.set_time(f"{source}:read_at", started)
        state.commit()
        listed, problems = read_sitemaps(site, PoliteClient(client, min_delay=site.min_delay, sleep=sleep, clock=clock))
    changes = compare(inventory, listed, judge_dropped=not problems)
    found = publish_new((page_event(site, change, started) for change in changes), publisher, state)
    counts = Counter(change.change for change in changes)
    note = f"{len(listed):,} URLs listed; " + ", ".join(f"{counts[name]:,} {name}" for name in CHANGES)
    if problems:
        note += f"; dropped pages not judged, {len(problems)} sitemaps unreadable: " + "; ".join(problems[:3])
    return Check(source, "error" if problems else "ok" if found else "unchanged", found, note)
