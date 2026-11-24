"""A whole daily run of the official files (`ssp official run`, the official flow), and its status (`ssp official
status`).

The sources go one after another, the quick ones first so that OSHA's 10 s Crawl-delay takes what is left of the window:
www.msha.gov's probe, the eCFR editions, the Federal Register's rules, arlweb (at most its share of the window), then
OSHA until the deadline. Each source's outcome is recorded in state/official.sqlite (state.py) as it ends; one that
raises is recorded as "error" and raised again, and the others still run (the caller decides: the command line goes on,
the flow's task fails and the flow fails at the end). A run that was not interrupted then publishes the feed's next
manifest (feed.py), a heartbeat when it found nothing.

A crawl site is asked only under its crawl lock, state/crawl-<site>.lock, never under the data root's pipeline.lock,
so the run never waits for, or holds up, the batch and model flows, which write other files. The lock is waited for
up to LOCK_WAIT, which a watcher's check (it holds the lock while it reads a site's sitemaps, under a minute) never
outlasts; a crawl that holds it longer leaves the site "skipped" for the day. The probe of www.msha.gov takes the msha
crawl's lock the same way. The eCFR and Federal Register sources only read the bulk store and ask the API.

The deadline is the first 04:30 UTC after the run's scheduled start (its start when run by hand), so the day's findings
exist before SafeStart's 06:00 UTC import, but never later than 15 minutes before the next start of another flow (the
daily 14:00 batch, the Sunday 15:00 model), which a flow run would otherwise hold up, since the flows run one at a time:
a run started by hand at 05:00 stops at 13:45, one started on a Sunday at 14:30 at 14:45. A run that starts after its
deadline asks nothing and records every source as skipped.
"""

from __future__ import annotations

import logging
import sqlite3
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from contextlib import ExitStack
from datetime import UTC, datetime, timedelta
from datetime import time as clock_time
from pathlib import Path

import httpx

from ..crawl import locks
from ..crawl.crawler import Crawler, iso, utc_now
from ..crawl.locks import SiteLock
from ..crawl.sites import SITES, Site
from ..crawl.store import HELD_SQL
from ..verify import read_only
from . import feed
from .daily import NEEDED_SQL, PLANS, DailyCrawl, Plan, cycle, queue_counts
from .sources import ecfr_hints, federal_register_hints, outcome, probe_msha
from .state import HOSTS, OfficialState

SOURCES = ("msha", "ecfr", "federal_register", "arlweb", "osha")  # the order of a run
CRAWLED = ("arlweb", "osha")
FINISH_BY = clock_time(4, 30)  # UTC: before SafeStart's daily import at 06:00 UTC
# The other flows' starts (flows/__main__.py), as (weekday, Monday 0, or None for every day; time of day UTC).
KEEP_CLEAR = ((None, clock_time(14, 0)), (6, clock_time(15, 0)))
CLEAR_MARGIN = timedelta(minutes=15)
LOCK_WAIT = timedelta(minutes=10)
LOCK_RETRY_SECONDS = 30.0
log = logging.getLogger(__name__)


def next_at(moment: datetime, at: clock_time, weekday: int | None = None) -> datetime:
    """The first time of day `at` (UTC), on `weekday` if one is given, strictly after `moment`."""
    moment = moment.astimezone(UTC)
    candidate = moment.replace(hour=at.hour, minute=at.minute, second=0, microsecond=0)
    if weekday is not None:
        candidate += timedelta(days=(weekday - candidate.weekday()) % 7)
    return candidate if candidate > moment else candidate + timedelta(days=1 if weekday is None else 7)


def deadline(start: datetime) -> datetime:
    """When a run that started (or was scheduled to start) at `start` stops: see the module docstring."""
    return min(next_at(start, FINISH_BY), *(next_at(start, at, weekday) - CLEAR_MARGIN for weekday, at in KEEP_CLEAR))


def parse_until(value: str, now: datetime) -> datetime:
    """`--until`: an ISO 8601 time (UTC unless it says otherwise) or a time of day HH:MM, the next one in UTC."""
    if len(value) == 5 and value[2] == ":":
        hour, minute = int(value[:2]), int(value[3:])
        return next_at(now, clock_time(hour, minute))
    moment = datetime.fromisoformat(value)  # Python 3.11 on reads a trailing Z as UTC
    return moment.astimezone(UTC) if moment.tzinfo else moment.replace(tzinfo=UTC)


def ordered(sources: Iterable[str] | None) -> list[str]:
    chosen = set(sources or SOURCES)
    if unknown := sorted(chosen - set(SOURCES)):
        raise ValueError(f"unknown source {', '.join(unknown)}; the sources are {', '.join(SOURCES)}")
    return [source for source in SOURCES if source in chosen]


class OfficialRun:
    """One run: its record in state/official.sqlite, its deadline and request budget, and each source in turn."""

    def __init__(self, root: Path, client: httpx.Client, *, until: datetime, budget: int | None = None,
                 trigger: str = "cli", sites: Mapping[str, Site] = SITES, plans: Mapping[str, Plan] = PLANS,
                 sleep: Callable[[float], None] = time.sleep, now: Callable[[], datetime] = utc_now):
        self.root, self.client, self.until, self.budget = root, client, until, budget
        self.sites, self.plans, self.sleep, self.now = sites, plans, sleep, now
        self.state = OfficialState(root / "state" / "official.sqlite")
        started = now()
        self.run_id = f"official-{started:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"
        self.state.start_run(self.run_id, started_at=iso(started), until=iso(until), trigger=trigger)

    def clock(self) -> float:
        """Seconds, from the same clock as `now`, for the polite clients' pacing."""
        return self.now().timestamp()

    def close(self, stop_reason: str) -> None:
        self.state.end_run(self.run_id, ended_at=iso(self.now()), stop_reason=stop_reason)
        self.state.close()

    def source(self, name: str) -> dict:
        """Ask one source and record its outcome. An exception is recorded as status "error" and raised again."""
        started = self.now()
        self.state.start_source(self.run_id, name, host=HOSTS[name], started_at=iso(started))
        try:
            if started >= self.until:
                result = outcome("skipped", f"the run's deadline, {iso(self.until)}, came before this source",
                                 stop_reason="time limit")
            elif name in CRAWLED:
                result = self.crawl_site(name)
            elif name == "msha":
                result = self.probe(name)
            elif name == "ecfr":
                result = ecfr_hints(self.root, self.state, now=started, run_id=self.run_id)
            else:
                result = federal_register_hints(self.client, self.state, now=started, run_id=self.run_id,
                                                sleep=self.sleep, clock=self.clock)
        except BaseException as problem:
            self.state.end_source(self.run_id, name, ended_at=iso(self.now()), status="error",
                                  note=f"{type(problem).__name__}: {problem}", requests=0, stop_reason="interrupted")
            raise
        self.state.end_source(self.run_id, name, ended_at=iso(self.now()), **result)
        log.info("%s: %s; %s", name, result["status"], result["note"])
        return result

    def lock(self, stack: ExitStack, name: str, until: datetime) -> str | None:
        """Hold the site's crawl lock in `stack`, waiting up to LOCK_WAIT for a short holder such as a watcher's check;
        the reason it could not be held otherwise."""
        path = self.root / "state" / f"crawl-{name}.lock"
        give_up = min(self.now() + LOCK_WAIT, until)
        while True:
            try:
                stack.enter_context(SiteLock(path))
                return None
            except RuntimeError:
                if self.now() >= give_up:
                    return (f"another process held {path.name} for {LOCK_WAIT.seconds // 60} minutes (a crawl of"
                            f" {name}?), so its host was not asked")
                self.sleep(min(LOCK_RETRY_SECONDS, max((give_up - self.now()).total_seconds(), 0.0)))

    def crawl_site(self, name: str) -> dict:
        site, plan = self.sites[name], self.plans[name]
        if not (self.root / "state" / f"crawl-{name}.sqlite").exists():
            return outcome("skipped", f"no {name} crawl inventory (state/crawl-{name}.sqlite) on this host")
        until = self.until if plan.share is None else min(self.until, self.now() + plan.share)
        with ExitStack() as stack:
            if why := self.lock(stack, name, until):
                return outcome("skipped", why, stop_reason="lock held")
            crawler = Crawler(site, self.root, self.client, sleep=self.sleep, now=self.now,
                              log=logging.getLogger(f"safestart_pipeline.official.{name}"))
            try:
                return DailyCrawl(crawler, plan, until=until, budget=self.budget).run()
            finally:
                crawler.store.close()

    def probe(self, name: str) -> dict:
        with ExitStack() as stack:
            if why := self.lock(stack, name, self.until):
                return outcome("skipped", why, stop_reason="lock held")
            return probe_msha(self.sites[name], self.client, sleep=self.sleep, clock=self.clock)


def run_official(root: Path, client: httpx.Client, *, sources: Iterable[str] | None = None, until: datetime,
                 budget: int | None = None, trigger: str = "cli", publish: bool = True, **options) -> dict:
    """Every chosen source in turn (`ssp official run`), then, unless the run was interrupted, the feed's next
    manifest (feed.py). A source that raises is reported and the others still run; a feed that cannot be published is
    reported under "feed" with the outcome "error"."""
    run = OfficialRun(root, client, until=until, budget=budget, trigger=trigger, **options)
    results: dict[str, dict] = {}
    stop = "interrupted"
    try:
        for name in ordered(sources):
            try:
                results[name] = run.source(name)
            except Exception as problem:
                log.exception("%s failed", name)
                results[name] = {"status": "error", "note": f"{type(problem).__name__}: {problem}"}
        stop = "errors" if any(result["status"] == "error" for result in results.values()) else "done"
    finally:
        run.close(stop)
    report = {"run_id": run.run_id, "until": iso(until), "sources": results}
    if publish:
        try:
            report["feed"] = feed.publish(root, trigger=trigger, run_id=run.run_id, now=run.now, sleep=run.sleep)
        except Exception as problem:  # recorded by the feed as an error; the run's own record stands
            log.exception("the feed's manifest was not published")
            report["feed"] = {"outcome": "error", "note": f"{type(problem).__name__}: {problem}"}
    return report


# Status ---------------------------------------------------------------------------------------------------------------


def lock_state(root: Path, name: str) -> str:
    """"free" or "held" (a crawl or a daily run of the site is asking it now); "unknown" where it cannot be tried. No
    lock file is made: a holder makes its own."""
    return locks.lock_state(root / "state" / f"crawl-{name}.lock")


def status(root: Path, now: datetime) -> dict:
    """The latest run, each source's latest outcome, for each crawl site what waits, how far the re-check has come, how
    many held copies the project does not need (kept, never asked about) and what was found in the last day and week,
    and the feed (its newest manifest and the latest attempt to publish one). Every store is only read."""
    report: dict[str, object] = {"latest_run": None, "sources": {}, "feed": feed.feed_status(root, now)}
    path = root / "state" / "official.sqlite"
    latest: dict[str, dict] = {}
    hints: dict[str, int] = {}
    if path.exists():
        db = read_only(path)
        try:
            run = db.execute("select * from runs order by started_at desc limit 1").fetchone()
            report["latest_run"] = dict(run) if run else None
            for row in db.execute("select * from sources order by started_at"):
                latest[row["source"]] = {key: row[key] for key in ("run_id", "status", "note", "requests",
                                                                   "stop_reason", "started_at", "ended_at")}
            hints = {row[0]: row[1] for row in db.execute(
                "select source, count(*) from hints where observed_at >= ? group by source",
                (iso(now - timedelta(days=7)),))}
        finally:
            db.close()
    sources: dict[str, dict] = {}
    for name in SOURCES:
        entry: dict[str, object] = {"host": HOSTS[name], "last": latest.get(name)}
        if name in ("ecfr", "federal_register"):
            entry["hints_last_7_days"] = hints.get(name, 0)
        if name in (*CRAWLED, "msha"):
            entry["lock"] = lock_state(root, name)
        if name in CRAWLED:
            entry |= crawl_status(root / "state" / f"crawl-{name}.sqlite", now)
        sources[name] = entry
    report["sources"] = sources
    return report


def crawl_status(path: Path, now: datetime) -> dict:
    if not path.exists():
        return {"inventory": "none on this host"}
    db = read_only(path)
    try:
        try:
            found = {"queue": queue_counts(db, now), "cycle": cycle(db, now),
                     "held_not_needed": db.execute(f"select count(*) from urls where {HELD_SQL} and not {NEEDED_SQL}"
                                                   ).fetchone()[0]}
        except sqlite3.OperationalError as problem:  # a store of the first version, migrated when a run opens it
            return {"inventory": f"not migrated yet ({problem})"}
        for label, since in (("changes_last_day", timedelta(days=1)), ("changes_last_7_days", timedelta(days=7))):
            rows = db.execute("select change, count(*) from changes where observed_at >= ? group by change",
                              (iso(now - since),))
            found[label] = {change: count for change, count in rows}
        found["latest_crawl_run"] = dict(db.execute("select * from runs order by started_at desc limit 1").fetchone()
                                         or {}) or None
        return found
    finally:
        db.close()
