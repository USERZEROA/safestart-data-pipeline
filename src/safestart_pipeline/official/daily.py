"""One day's run of a crawl site for the daily update of the official files: OSHA (sitemaps, hub pass, queue) and
arlweb (queue only).

A run asks the site, within its deadline and its request budget, in this order:
1. robots.txt, as the crawl reads it (crawler.py): its rules, its Crawl-delay, and a wait while it cannot be read;
2. the sitemaps (OSHA), read whole. URLs listed for the first time are queued. When every sitemap was read, a URL they
   no longer list gets dropped_from_sitemap_at and, when it is held and answering, a confirmation; one listed again
   loses both and, when it is not held, is asked again. A read that would queue more than SUSPECT_DROPS confirmations
   at once is taken for a broken sitemap: the drops are marked without confirmations, and the re-check reaches those
   URLs in its own time. Its end, more than SUSPECT_DROPS URLs listed again at once, clears the marks without asking
   again the URLs that are not held (OSHA's sitemap lists 2,446, about seven hours of requests ahead of the re-check):
   they come round at the rate of URLs asked for their return;
3. the hub pass (OSHA): the pages whose links reach the most held files (choose_hubs), each asked conditionally. Their
   new links are queued, their links are stored as they stand today (crawler.py), and a held file that no sitemap
   lists and that no page checked within LINK_FRESH still links to is queued for confirmation;
4. the queue: due retries of first downloads, then new URLs (sitemap URLs first), then confirmations, then up to
   RETURNING_PER_RUN URLs that never gave a usable copy and were not asked for RETURNING_AFTER (a 404, or an error other
   than a refusal, at a URL the sitemap still lists; a copy refused for its bytes), then the rolling re-check: held URLs
   due by next_check_at, the one due longest first, and those whose last check failed after the others. A check
   confirms a file for a week and a page for a month, a gone or moved URL is asked again in a week and a failed check
   in a day (crawler.py, backfill.py), so files come round about weekly and pages about monthly; taking the one due
   longest, rather than the one checked longest ago, keeps the files weekly when a month's pages come due at once.
The run asks nothing the project does not need (the crawl's not-needed rules, Crawler.why_unneeded): a new URL or one
due to be asked for its return that the rules name is skipped as the crawl skips it ("not_needed"), and a held copy they
name (downloaded before the rule; on OSHA 13,855 of the 41,118 held, 2026-11-24) is kept but marked not needed when the
run starts (tidy_held: skip_reason "not_needed", its state as it was), so that it is neither re-checked, confirmed nor a
hub, and the re-check cycle counts only the copies the project needs. A held copy the rules no longer name is asked
about again in its turn.
Every request goes through Crawler.ask: robots.txt, the Crawl-delay, Retry-After, the validators of a verified copy, the
change rules of crawler.py and the stop on a sustained block ("possible block"), which retires nothing. Two rules of the
daily run keep that stop for real blocks: URLs that refused before are never what makes a streak (the run stops asking
them after FAILED_STREAK refusals in a row and leaves them for the next run, and a URL that refused is not asked for its
return), and a site that gives no answer or a 5xx OUTAGE_THRESHOLD times in a row is waited for as the crawl waits for
a block, then left until the next run ("site unavailable").

A run started again the same day goes on where the other stopped: the queue lives in the inventory, the sitemaps are not
read again within HUB_INTERVAL of a complete read, and a hub checked within HUB_INTERVAL is not asked again.

The outcome's status is the one the feed reports: refused when the run stopped on a sustained block or robots.txt
disallowed everything it was to ask; unavailable when robots.txt could not be read before the deadline, the site went on
giving no answer, or no request was answered; partial when the deadline or the budget came before the sitemaps, the
hubs, the new URLs and the confirmations were all done, or a sitemap could not be read; ok otherwise. The re-check is
open-ended: it goes on until no URL is due, the deadline comes or the budget is used. The URLs it counts as checked and
unchanged are those the feed lists as checked (CURRENT_SQL: current documents of the feed's types); a URL still gone or
moved, or a picture, a recording or another file, is counted apart.
"""

from __future__ import annotations

import heapq
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta

from ..crawl.content import FEED_TYPES
from ..crawl.crawler import (
    BLOCK_PAUSE_SECONDS,
    BLOCK_THRESHOLD,
    MAX_BLOCK_PAUSES,
    Crawler,
    RobotsUnreachable,
    iso,
    looks_blocked,
    parse_iso,
)
from ..crawl.fetcher import FetchResult
from ..crawl.robots import Robots
from ..crawl.sites import Site
from ..crawl.store import HELD_SQL, holds
from ..crawl.urls import is_page

HUB_INTERVAL = timedelta(hours=20)  # a sitemap read or a hub check this recent is not repeated the same day
LINK_FRESH = timedelta(days=2)  # a link from a page checked this recently still counts (hubs are checked daily)
RETURNING_AFTER = timedelta(days=30)
RETURNING_PER_RUN = 60  # OSHA's sitemap lists 1,756 URLs that answer 404: about 30 days for all of them
SUSPECT_DROPS = 1000  # more confirmations than this from one sitemap read: a broken sitemap, not removed pages
FAILED_STREAK = BLOCK_THRESHOLD - 1  # refusals in a row from URLs that refused before, never a block on their own
OUTAGE_THRESHOLD = 30  # requests in a row with no answer or a 5xx: the site is down (5 minutes at OSHA's delay)
CYCLE = timedelta(days=30)
ANSWERING = ("fetched", "unchanged")
REFUSALS = "(401, 403, 429, 451)"

RETRY_SQL = "select * from urls where state = 'retry' and next_attempt_at <= ? order by next_attempt_at, rowid limit 1"
NEW_WHERE = f"state = 'pending' and not {HELD_SQL}"
CONFIRM_WHERE = f"{HELD_SQL} and (confirm_reason is not null or state = 'pending')"
RETURNING_WHERE = (f"not {HELD_SQL} and ((in_sitemap = 1 and dropped_from_sitemap_at is null and (state = 'not_found'"
                   f" or (state = 'error' and coalesce(http_status, 0) not in {REFUSALS}))) or state = 'rejected')"
                   " and coalesce(fetched_at, first_seen_at) <= ?")
# A held copy the not-needed rules name is marked so (tidy_held) and left out of everything the run asks.
NEEDED_SQL = "coalesce(skip_reason, '') != 'not_needed'"
RECHECK_WHERE = (f"{HELD_SQL} and {NEEDED_SQL} and state in ('fetched', 'unchanged', 'redirect', 'not_found')"
                 " and confirm_reason is null and coalesce(next_check_at, '') <= ?")
# A held copy that is current (neither gone nor moved) and a document of the feed's types: what a check that leaves it
# unchanged confirms, and the feed lists among the checked URLs (feed.py); the run counts its checks the same way.
CURRENT_SQL = (f"{HELD_SQL} and gone_since is null and state in ('fetched', 'unchanged')"
               f" and detected_type in ({', '.join(repr(kind) for kind in FEED_TYPES)})")
# When a held URL is due; one never scheduled (no check or backfill dated it) is due since it was last confirmed.
DUE_AT = "coalesce(next_check_at, last_checked_at, fetched_at, first_seen_at)"
LINKED_FROM_FRESH = ("select 1 from links join urls on urls.url = links.from_url"
                     " where links.to_url = ? and urls.last_checked_at >= ? limit 1")


@dataclass(frozen=True)
class Plan:
    """What a site's daily run does: its request budget, the longest share of the run's window it may take (None: up to
    the deadline), whether it reads the sitemaps, and how many hub pages it asks."""

    budget: int
    share: timedelta | None = None
    sitemaps: bool = False
    hubs: int = 0


PLANS = {
    # OSHA's Crawl-delay is 10 s (10.6 s a request measured): 16:30 to 04:30 UTC holds about 4,000 requests. The sitemap
    # read takes 3, the 500 hubs reach 98.8% of the 5,594 held files a needed held page links to (567 reach all of
    # them; copy of the inventory, 2026-11-24), and the re-check needs about 1,000 files and 1,100 pages a day. The
    # budget only guards against a delay shorter than OSHA's.
    "osha": Plan(budget=5000, sitemaps=True, hubs=500),
    # arlweb answers the VM at a 1 s delay; its 12,394 held documents, weekly, are about 1,800 requests a day.
    "arlweb": Plan(budget=3000, share=timedelta(hours=2)),
}


def choose_hubs(db: sqlite3.Connection, site: Site, limit: int) -> list[str]:
    """Up to `limit` held pages whose links reach the most held files, picked greedily: each next page is the one whose
    links reach the most files the pages before it do not, the lower URL first between equals. Only held URLs that
    answer count (not gone, moved or queued), and only those the site's not-needed rules keep."""
    files, pages = set(), set()
    for url, detected in db.execute(f"select url, detected_type from urls where {HELD_SQL} and {NEEDED_SQL}"
                                    " and state in ('fetched', 'unchanged')"):
        if not site.unneeded(url):
            (pages if is_html_copy(url, detected) else files).add(url)
    reach: dict[str, set[str]] = defaultdict(set)
    for source, target in db.execute("select from_url, to_url from links"):
        if target in files and source in pages:
            reach[source].add(target)
    heap = [(-len(targets), page) for page, targets in reach.items()]
    heapq.heapify(heap)
    covered: set[str] = set()
    chosen: list[str] = []
    while heap and len(chosen) < limit:
        _, page = heapq.heappop(heap)
        gain = len(reach[page] - covered)
        if gain == 0:
            continue
        if heap and (-gain, page) > heap[0]:  # the bound it was queued with is out of date: queue it again
            heapq.heappush(heap, (-gain, page))
            continue
        chosen.append(page)
        covered |= reach[page]
    return chosen


def is_html_copy(url: str, detected_type: str | None) -> bool:
    """Whether a held copy is a page: what its bytes are, or, for a copy not examined yet, what its URL says."""
    return detected_type == "html" if detected_type else is_page(url)


def confirmable(row: sqlite3.Row) -> bool:
    """Whether a URL may be queued for confirmation: held, answering with its copy, needed by the project (not marked
    otherwise by tidy_held) and not queued already."""
    return (holds(row) and row["state"] in ANSWERING and row["skip_reason"] != "not_needed"
            and not row["confirm_reason"])


def cycle(db: sqlite3.Connection, now: datetime) -> dict[str, object]:
    """How far the re-check has come round: the held URLs not gone that the project needs (those it re-checks), how
    many of them were confirmed in the last 30 days, and the oldest confirmation (the feed's `cycle`). A copy never
    checked since its download (one recorded before content hashes) was last confirmed by that download."""
    confirmed = "coalesce(last_checked_at, fetched_at, first_seen_at)"
    held, recent, oldest = db.execute(
        f"select count(*), coalesce(sum({confirmed} >= ?), 0), min({confirmed}) from urls where {HELD_SQL}"
        f" and {NEEDED_SQL} and gone_since is null", (iso(now - CYCLE),)).fetchone()
    return {"held": held, "checkedLast30Days": recent, "oldestCheckAt": oldest}


def queue_counts(db: sqlite3.Connection, now: datetime) -> dict[str, int]:
    """What waits for the next run: retries of first downloads, new URLs, confirmations, URLs due to be asked again
    for their return, and held URLs due for their re-check."""

    def count(where: str, *values: object) -> int:
        return db.execute(f"select count(*) from urls where {where}", values).fetchone()[0]

    return {"retries": count("state = 'retry'"), "new": count(NEW_WHERE), "confirmations": count(CONFIRM_WHERE),
            "returning_due": count(RETURNING_WHERE, iso(now - RETURNING_AFTER)),
            "checks_due": count(RECHECK_WHERE, iso(now))}


class DailyCrawl:
    """One daily run of a crawl site through its Crawler (whose store and objects it writes, under the site's lock,
    which the caller holds)."""

    def __init__(self, crawler: Crawler, plan: Plan, *, until: datetime, budget: int | None = None):
        self.crawler, self.plan, self.until = crawler, plan, until
        self.store = crawler.store
        self.budget = plan.budget if budget is None else budget
        self.first_request = crawler.requests
        self.asked: Counter[str] = Counter()  # requests by purpose
        self.sent = self.answered = 0  # requests through the queue and the hub pass; those answered below 500
        self.confirmations: Counter[str] = Counter()  # confirmations queued, by why
        self.returning = 0
        self.failed_refusals = 0  # refusals in a row from held URLs whose last check had failed
        self.failed_asked = True  # whether such URLs are still asked in this run
        self.unanswered_in_a_row = self.outage_pauses = 0
        self.skipped: Counter[str] = Counter()  # URLs not asked because the project does not need them, by purpose
        self.held_not_needed: dict[str, int] = {}
        self.sitemaps: dict[str, object] = {}
        self.hubs: dict[str, int] = {}

    @property
    def requests(self) -> int:
        return self.crawler.requests - self.first_request

    def run(self) -> dict[str, object]:
        """The run, as one run of the crawl store; returns its outcome (status, note, requests, stop reason, the crawl
        store's run and the counts). An exception rolls back the answer it interrupted (crawler.py) and is raised."""
        crawler = self.crawler
        started = iso(crawler.now())
        crawler.unlinked = self.unlinked
        try:
            with crawler.run() as outcome:
                run_id = crawler.run_id
                try:
                    stopped = self.work()
                except RobotsUnreachable as problem:
                    crawler.log.warning("stopping: %s", problem)
                    stopped = "robots.txt unreachable"
                outcome["stopped"] = stopped
                self.store.set_meta("last_stop", f"{iso(crawler.now())} {stopped} (daily run)")
                self.store.commit()
        finally:
            crawler.unlinked = None
        return self.outcome(stopped, run_id, started)

    def work(self) -> str:
        crawler = self.crawler
        crawler.ensure_robots(self.until)
        crawler.tidy_queue()
        self.tidy_held()
        crawler.blocked_in_a_row = crawler.block_pauses = 0
        if self.plan.sitemaps:
            self.read_sitemaps()
        if self.plan.hubs and (stopped := self.hub_pass()):
            return stopped
        return self.work_queue()

    def tidy_held(self) -> None:
        """The held copies brought in line with the not-needed rules: one they now name is kept but marked not needed
        (skip_reason "not_needed", its state as it was, a confirmation queued for it dropped), so that the run never
        asks about it; one they no longer name loses the mark and is asked about again in its turn."""
        store, why_unneeded = self.store, self.crawler.why_unneeded
        marked = cleared = 0
        for row in store.db.execute(f"select url, skip_reason from urls where {HELD_SQL}").fetchall():
            unneeded = why_unneeded(row["url"]) is not None
            if unneeded and row["skip_reason"] != "not_needed":
                store.update(row["url"], skip_reason="not_needed", confirm_reason=None)
                marked += 1
            elif not unneeded and row["skip_reason"] == "not_needed":
                store.update(row["url"], skip_reason=None)
                cleared += 1
        store.commit()
        total = store.db.execute(f"select count(*) from urls where {HELD_SQL} and not {NEEDED_SQL}").fetchone()[0]
        self.held_not_needed = {"held": total, "marked": marked, "cleared": cleared}
        if marked or cleared:
            self.crawler.log.info("held copies the project does not need: %s", self.held_not_needed)

    def limit(self) -> str | None:
        if self.requests >= self.budget:
            return "request limit"
        if self.crawler.now() >= self.until:
            return "time limit"
        return None

    # The sitemaps -----------------------------------------------------------------------------------------------------

    def read_sitemaps(self) -> None:
        store, now = self.store, self.crawler.now()
        last = store.get_meta("daily_sitemaps_at")
        if last and parse_iso(last) > now - HUB_INTERVAL:
            self.sitemaps = {"read": False, "note": f"read completely at {last}"}
            return
        read = self.crawler.read_sitemaps(self.until, keep=False, max_requests=self.first_request + self.budget)
        stamp = iso(self.crawler.now())
        self.sitemaps = {"read": True, "complete": read.complete, "listed": len(read.listed), "added": len(read.added),
                         "refused": read.refused[:5], "unread": read.unread[:5]}
        if read.complete:
            self.sitemaps |= self.compare_listing(read.listed, stamp)
            store.set_meta("daily_sitemaps_at", stamp)
        store.commit()
        self.crawler.log.info("sitemaps: %s", self.sitemaps)

    def compare_listing(self, listed: dict[str, str | None], stamp: str) -> dict[str, object]:
        """Today's complete listing against the URLs the store has from a sitemap."""
        rows = self.store.db.execute(
            "select url, state, sha256, content_sha256, detected_type, dropped_from_sitemap_at, confirm_reason,"
            " skip_reason from urls where in_sitemap = 1").fetchall()
        confirm = [row["url"] for row in rows if row["url"] not in listed and row["dropped_from_sitemap_at"] is None
                   and confirmable(row)]
        suspect = len(confirm) > SUSPECT_DROPS
        # So many back at once is the end of a broken read, not URLs that came back: none of them is asked again here.
        recovered = sum(row["url"] in listed and row["dropped_from_sitemap_at"] is not None
                        for row in rows) > SUSPECT_DROPS
        dropped = back = asked_again = 0
        for row in rows:
            url = row["url"]
            if url not in listed and row["dropped_from_sitemap_at"] is None:
                fields: dict[str, object] = {"dropped_from_sitemap_at": stamp}
                if not suspect and confirmable(row):
                    fields["confirm_reason"] = f"dropped from the sitemap {stamp}"
                    self.confirmations["dropped from the sitemap"] += 1
                self.store.update(url, **fields)
                dropped += 1
            elif url in listed and row["dropped_from_sitemap_at"] is not None:
                fields = {"dropped_from_sitemap_at": None}
                if (row["confirm_reason"] or "").startswith("dropped from the sitemap"):
                    fields["confirm_reason"] = None
                if not recovered and not holds(row) and row["state"] in ("not_found", "error", "rejected", "redirect"):
                    fields |= {"state": "pending", "attempts": 0, "next_attempt_at": None, "error": None}
                    asked_again += 1
                self.store.update(url, **fields)
                back += 1
        found = {"dropped": dropped, "listed_again": back, "asked_again": asked_again}
        if suspect:
            found["suspect"] = (f"{len(confirm):,} held URLs dropped at once: marked, not queued for confirmation (a"
                                " broken sitemap?)")
        if recovered:
            found["recovered"] = (f"{back:,} URLs listed again at once: marks cleared, none asked again (the end of a"
                                  " broken sitemap?)")
        return found

    # The hub pass -----------------------------------------------------------------------------------------------------

    def hub_pass(self) -> str | None:
        crawler, store = self.crawler, self.store
        hubs = choose_hubs(store.db, crawler.site, self.plan.hubs)
        recent = iso(crawler.now() - HUB_INTERVAL)
        self.hubs = {"chosen": len(hubs), "asked": 0, "checked_already": 0, "left": 0}
        crawler.log.info("hub pass: %d pages", len(hubs))
        for index, url in enumerate(hubs):
            if stopped := self.limit():
                self.hubs["left"] = len(hubs) - index
                return stopped
            row = store.get(url)
            if (row["last_checked_at"] or "") > recent:
                self.hubs["checked_already"] += 1
                continue
            robots = crawler.ensure_robots(self.until)
            self.hubs["asked"] += 1
            if stopped := self.ask(row, robots, "hub"):
                self.hubs["left"] = len(hubs) - index - 1
                return stopped
        return None

    def unlinked(self, page: str, targets: set[str]) -> None:
        """A page asked again no longer links to `targets`: each held file among them that no sitemap lists and that no
        page checked within LINK_FRESH still links to is queued for confirmation."""
        now = self.crawler.now()
        fresh = iso(now - LINK_FRESH)
        for url in sorted(targets):
            row = self.store.get(url)
            if row is None or not confirmable(row) or is_html_copy(url, row["detected_type"]):
                continue
            if row["in_sitemap"] and row["dropped_from_sitemap_at"] is None:
                continue  # the sitemap lists it
            if self.store.db.execute(LINKED_FROM_FRESH, (url, fresh)).fetchone():
                continue
            self.store.update(url, confirm_reason=f"no longer linked from {page} ({iso(now)})"[:500])
            self.confirmations["no longer linked"] += 1

    # The queue --------------------------------------------------------------------------------------------------------

    def work_queue(self) -> str:
        crawler = self.crawler
        while True:
            if stopped := self.limit():
                return stopped
            robots = crawler.ensure_robots(self.until)
            row, purpose = self.next_item()
            if row is None:
                earliest = self.store.earliest_retry()
                if earliest is None:
                    return "done"
                crawler.wait(min(max((parse_iso(earliest) - crawler.now()).total_seconds(), 1.0), 600.0), self.until)
                continue
            if stopped := self.ask(row, robots, purpose):
                return stopped

    def next_item(self) -> tuple[sqlite3.Row | None, str | None]:
        db, now = self.store.db, self.crawler.now()
        if row := db.execute(RETRY_SQL, (iso(now),)).fetchone():
            return row, "retry"
        while row := db.execute(f"select * from urls where {NEW_WHERE} order by in_sitemap desc, rowid limit 1"
                                ).fetchone():
            # One found since the run began, through links of pages the project does not need, is skipped.
            if not self.not_needed(row, "new"):
                return row, "new"
        if row := db.execute(f"select * from urls where {CONFIRM_WHERE} order by rowid limit 1").fetchone():
            return row, "confirmation"
        while self.returning < RETURNING_PER_RUN and (row := db.execute(
                f"select * from urls where {RETURNING_WHERE} order by coalesce(fetched_at, first_seen_at), rowid"
                " limit 1", (iso(now - RETURNING_AFTER),)).fetchone()):
            # One the not-needed rules name (it answered 404 or an error before the rule existed) is never asked again:
            # if it came back it would be a new URL the project does not need.
            if not self.not_needed(row, "returning"):  # asked again under the first download's rules
                self.returning += 1
                self.store.update(row["url"], state="pending", attempts=0, next_attempt_at=None)
                return self.store.get(row["url"]), "returning"
        # The one due longest first, not the oldest confirmation: a file is due a week after its check and a page a
        # month after, so when a month's pages come due together (the crawl fetched 34,168 OSHA pages in six days, more
        # than six days of requests), files keep their weekly turn instead of waiting until every page older than
        # them is done. URLs whose last check failed (no confirmation since) go after the others, the one due longest
        # first, so that those a run leaves are the first the next run asks.
        failed = "" if self.failed_asked else " and error is null"
        row = db.execute(f"select * from urls where {RECHECK_WHERE}{failed} order by error is not null, {DUE_AT},"
                         " rowid limit 1", (iso(now),)).fetchone()
        return (row, "recheck") if row else (None, None)

    def not_needed(self, row: sqlite3.Row, purpose: str) -> bool:
        """Whether the not-needed rules name a URL the store holds no copy of. One they name is recorded as skipped
        "not_needed", as the crawl records it (it queues the URL again should the rules change), and counted."""
        if not self.crawler.why_unneeded(row["url"]):
            return False
        self.store.update(row["url"], state="skipped", skip_reason="not_needed", next_attempt_at=None)
        self.store.commit()
        self.skipped[purpose] += 1
        return True

    def ask(self, row: sqlite3.Row, robots: Robots, purpose: str) -> str | None:
        """One request through the crawler; a confirmation queued for the URL is answered by it (and noted on the change
        it makes), whatever the purpose. Returns the reason to stop, if there is one."""
        why = None
        if row["confirm_reason"]:
            why = f"asked to confirm: {row['confirm_reason']}"
            self.store.update(row["url"], confirm_reason=None)  # committed with the answer
        failed_before = purpose == "recheck" and row["error"] is not None
        result, stopped = self.crawler.ask(row, robots, self.until, why=why)
        if result is None:
            self.asked["disallowed by robots.txt"] += 1
            return stopped
        self.asked[purpose] += 1
        self.sent += 1
        self.answered += result.status is not None and result.status < 500
        if failed_before:
            self.failed_refusals = self.failed_refusals + 1 if looks_blocked(result) else 0
            if self.failed_refusals >= FAILED_STREAK:
                self.failed_asked = False  # they refuse still: left for the next run, before they look like a block
        return stopped or self.outage(result)

    def outage(self, result: FetchResult) -> str | None:
        """Stop, after waiting as the crawl waits for a block, when the site keeps giving no answer or a 5xx."""
        unanswered = result.status is None or result.status >= 500
        self.unanswered_in_a_row = self.unanswered_in_a_row + 1 if unanswered else 0
        if self.unanswered_in_a_row < OUTAGE_THRESHOLD:
            return None
        self.outage_pauses += 1
        if self.outage_pauses > MAX_BLOCK_PAUSES:
            return "site unavailable"
        self.crawler.log.warning("%d requests in a row unanswered; pausing %.0f s", self.unanswered_in_a_row,
                                 BLOCK_PAUSE_SECONDS)
        self.crawler.wait(BLOCK_PAUSE_SECONDS, self.until)
        self.unanswered_in_a_row = 0
        return None

    # The outcome ------------------------------------------------------------------------------------------------------

    def outcome(self, stopped: str, run_id: str, started: str) -> dict[str, object]:
        db, now = self.store.db, self.crawler.now()
        changes = Counter(row[0] for row in db.execute("select change from changes where run_id = ?", (run_id,)))
        # Held URLs this run confirmed, with no change but a refusal: the current documents among them are what the
        # feed lists as checked (CURRENT_SQL); the others (still gone or moved, or a picture, a recording or another
        # file the feed does not describe) are counted apart.
        checked, checked_other = db.execute(
            f"select coalesce(sum({CURRENT_SQL}), 0), coalesce(sum(not ({CURRENT_SQL})), 0) from urls u"
            f" where {HELD_SQL} and last_checked_at >= ? and not exists (select 1 from changes c"
            " where c.url = u.url and c.run_id = ? and c.change != 'rejected')", (started, run_id)).fetchone()
        left = queue_counts(db, now)
        unfinished = []
        if self.plan.sitemaps and self.sitemaps.get("read") and not self.sitemaps.get("complete"):
            unfinished.append("sitemaps not read whole: " + "; ".join([*self.sitemaps["refused"],
                                                                        *self.sitemaps["unread"]][:3]))
        if self.hubs.get("left"):
            unfinished.append(f"{self.hubs['left']:,} hubs not asked")
        unfinished += [f"{left[key]:,} {label} waiting" for key, label in (("new", "new URLs"),
                                                                            ("confirmations", "confirmations"))
                       if left[key]]
        disallowed = self.asked["disallowed by robots.txt"]
        if stopped == "possible block" or (disallowed and not self.sent):
            status = "refused"
        elif stopped in ("robots.txt unreachable", "site unavailable") or (self.sent and not self.answered):
            status = "unavailable"
        else:
            status = "partial" if unfinished else "ok"
        found = ", ".join(f"{changes[name]:,} {name}" for name in ("new", "changed", "gone", "restored", "moved",
                                                                    "rejected") if changes[name]) or "no changes"
        parts = [f"{self.requests:,} requests, stopped: {stopped}", found, f"{checked:,} checked and unchanged"]
        if checked_other:
            parts.append(f"{checked_other:,} other URLs checked (still gone or moved, or not documents)")
        if self.sitemaps.get("read"):
            parts.append(f"sitemaps: {self.sitemaps['listed']:,} URLs, {self.sitemaps['added']:,} new"
                         + (f", {self.sitemaps.get('dropped', 0):,} dropped" if self.sitemaps.get("complete") else ""))
        if self.hubs:
            parts.append(f"hubs: {self.hubs['asked'] + self.hubs['checked_already']:,} of {self.hubs['chosen']:,}")
        if stopped == "possible block":
            parts.append("the site kept refusing, so the run stopped as the crawl does after a sustained block; nothing"
                         " was retired")
        if stopped == "site unavailable":
            parts.append(f"no answer or a 5xx {OUTAGE_THRESHOLD} times in a row after {MAX_BLOCK_PAUSES} pauses;"
                         " nothing was retired")
        if disallowed:
            parts.append(f"{disallowed:,} URLs robots.txt disallows, not asked (their copies stay current)")
        if not self.failed_asked:
            parts.append(f"URLs that refused before still refused {FAILED_STREAK} times in a row: the rest of them wait"
                         " for the next run")
        if self.skipped:
            parts.append(f"{sum(self.skipped.values()):,} URLs the project does not need, skipped ("
                         + ", ".join(f"{count:,} {purpose}" for purpose, count in sorted(self.skipped.items())) + ")")
        if self.held_not_needed.get("marked") or self.held_not_needed.get("cleared"):
            parts.append(f"held copies the project does not need, kept but no longer asked about: "
                         f"{self.held_not_needed['held']:,} ({self.held_not_needed['marked']:,} newly,"
                         f" {self.held_not_needed['cleared']:,} needed again)")
        parts += unfinished
        return {"status": status, "note": "; ".join(parts), "requests": self.requests, "stop_reason": stopped,
                "crawl_run_id": run_id,
                "details": {"asked": dict(self.asked), "changes": dict(changes), "checked_unchanged": checked,
                            "checked_other": checked_other, "confirmations_queued": dict(self.confirmations),
                            "sitemaps": self.sitemaps, "hubs": self.hubs, "returning_asked": self.returning,
                            "left": left, "skipped_not_needed": dict(self.skipped),
                            "held_not_needed": self.held_not_needed, "cycle": cycle(db, now)}}
