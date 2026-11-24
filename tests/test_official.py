"""The daily run of the official files against mocked sites: a crawl site's run (new, changed, the same content in new
bytes, 304, gone by 404 and 410, restored, moved, a soft error, refusals by 403, 429 and a challenge page, a sustained
block), its sitemap pass (new, dropped and listed-again URLs, an incomplete or refused sitemap, a broken one), its hub
pass (the greedy cover, new links, files no longer linked), the order of its queue, URLs asked again for their return,
held copies and returning URLs the project does not need (never asked), its deadline and budget, a crash and the run
that resumes it; www.msha.gov's probe, the eCFR editions, the Federal Register's rules; a whole run, with a lock another
process holds or an inventory that was not backfilled; and `ssp official run|status`."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from safestart_pipeline import cli
from safestart_pipeline.acquire.bulk import BulkStore
from safestart_pipeline.crawl.backfill import backfill_hashes
from safestart_pipeline.crawl.crawler import BLOCK_PAUSE_SECONDS, Crawler, iso
from safestart_pipeline.crawl.locks import SiteLock
from safestart_pipeline.crawl.sites import Site
from safestart_pipeline.crawl.store import HELD_SQL, Store, holds
from safestart_pipeline.official import daily, run, sources
from safestart_pipeline.official.daily import DailyCrawl, Plan, choose_hubs
from safestart_pipeline.official.run import OfficialRun, run_official
from safestart_pipeline.official.state import OfficialState
from test_crawler import HOST, Clock, example_site, pdf_bytes
from test_editions import SOFT_ERROR, Answers, first_store, held

ROBOTS = b"User-agent: *\nCrawl-delay: 10\nDisallow: /private/\n"
PDF = {"content-type": "application/pdf"}
HTML = {"content-type": "text/html; charset=utf-8"}
XML = {"content-type": "application/xml"}
START = datetime(2026, 11, 24, 16, 30, tzinfo=UTC)
SITE = example_site(name="example", sitemaps=(f"{HOST}/sitemap.xml",), max_bytes=10**6)
PLAN = Plan(budget=1000, sitemaps=True, hubs=10)
FILES = {"a": 300, "b": 310, "c": 320, "e": 330}
CHALLENGE = b"<html><body><h1>Request unsuccessful</h1><p>Please complete the captcha.</p></body></html>"


def urlset(*paths: str) -> bytes:
    entries = "".join(f"<url><loc>{HOST}{path}</loc></url>" for path in paths)
    return (f'<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
            f"{entries}</urlset>").encode()


def page(title: str, *links: str, header: str = "") -> bytes:
    """A page whose main content is its title and its links; `header` changes the bytes outside the main content."""
    anchors = " ".join(f'<a href="{link}">{link.rsplit("/", 1)[-1]}</a>' for link in links)
    return (f'<html><body><header><a href="/">Home</a> {header}</header><main><h1>{title}</h1><p>{anchors}</p></main>'
            "</body></html>").encode()


def site_answers() -> Answers:
    """Three pages and five files: the sitemap lists the pages and the report; the other files are reached through
    links, c.pdf from both pages that link to files."""
    site = Answers()
    site.set("/robots.txt", (200, ROBOTS, {}))
    site.set("/sitemap.xml", (200, urlset("/", "/hazcom", "/publications", "/report.pdf"), XML))
    site.set("/", (200, page("Home", "/publications", "/hazcom"), {**HTML, "etag": '"h1"'}))
    site.set("/publications", (200, page("Publications", "/files/a.pdf", "/files/b.pdf", "/files/c.pdf"),
                               {**HTML, "etag": '"p1"'}))
    site.set("/hazcom", (200, page("Hazard communication", "/files/c.pdf", "/files/e.pdf"), {**HTML, "etag": '"z1"'}))
    for name, size in FILES.items():
        site.set(f"/files/{name}.pdf", (200, pdf_bytes(size), {**PDF, "etag": f'"{name}1"'}))
    site.set("/report.pdf", (200, pdf_bytes(400), {**PDF, "etag": '"r1"'}))
    return site


@dataclass
class Bench:
    root: Path
    site: Answers
    clock: Clock
    client: httpx.Client

    def run(self, *, days: float = 0, hours: float = 12, budget: int | None = None, plan: Plan = PLAN,
            site: Site = SITE) -> dict:
        """A daily run `days` after the last one, with a fresh crawler as each run has, until `hours` later."""
        self.clock.now += timedelta(days=days)
        crawler = Crawler(site, self.root, self.client, sleep=self.clock.sleep, now=lambda: self.clock.now)
        try:
            return DailyCrawl(crawler, plan, until=self.clock.now + timedelta(hours=hours), budget=budget).run()
        finally:
            crawler.store.close()

    @contextmanager
    def db(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.root / "state" / "crawl-example.sqlite")
        db.row_factory = sqlite3.Row
        try:
            yield db
        finally:
            db.close()

    def row(self, path: str) -> dict:
        with self.db() as db:
            found = db.execute("select * from urls where url = ?", (HOST + path,)).fetchone()
        return dict(found) if found else {}

    def found(self, result: dict | None = None) -> list[tuple[str, str]]:
        """The changes recorded, by path, in the order observed: all of them, or one run's."""
        with self.db() as db:
            rows = db.execute("select url, change from changes where run_id = coalesce(?, run_id) order by id",
                              (result["crawl_run_id"] if result else None,)).fetchall()
        return [(row["url"].removeprefix(HOST), row["change"]) for row in rows]

    def change(self, path: str, change: str) -> dict:
        with self.db() as db:
            return dict(db.execute("select * from changes where url = ? and change = ? order by id desc",
                                   (HOST + path, change)).fetchone())

    def held(self) -> dict[str, str]:
        with self.db() as db:
            return {row["url"]: row["sha256"] for row in db.execute(f"select url, sha256 from urls where {HELD_SQL}")}

    def paths(self) -> list[str]:
        """The paths asked since the requests were last cleared, in order."""
        return [request.url.path for request in self.site.requests]

    def asked(self) -> list[str]:
        """The paths asked since the requests were last cleared, robots.txt and the sitemap left out."""
        return [path for path in self.paths() if path not in ("/robots.txt", "/sitemap.xml")]


@pytest.fixture
def bench(tmp_path):
    site, clock = site_answers(), Clock()
    clock.now = START
    client = httpx.Client(transport=httpx.MockTransport(site.handler))
    yield Bench(tmp_path, site, clock, client)
    client.close()


def baseline(bench: Bench) -> dict:
    """The first run, which records the whole site as new; the requests are cleared after it."""
    first = bench.run()
    bench.site.requests.clear()
    return first


# A crawl site's run ---------------------------------------------------------------------------------------------------


def test_a_first_run_records_the_site_and_the_next_day_reads_the_sitemap_and_asks_the_hubs(bench):
    first = bench.run()
    assert (first["status"], first["stop_reason"], first["requests"]) == ("ok", "done", 10)  # robots, sitemap, 8 URLs
    assert sorted(bench.found()) == sorted((path, "new") for path in (
        "/", "/hazcom", "/publications", "/report.pdf", "/files/a.pdf", "/files/b.pdf", "/files/c.pdf", "/files/e.pdf"))
    assert first["details"]["sitemaps"]["added"] == 4 and first["details"]["hubs"]["chosen"] == 0
    assert first["details"]["asked"] == {"new": 8} and first["details"]["cycle"]["held"] == 8
    sitemap = hashlib.sha256(urlset("/", "/hazcom", "/publications", "/report.pdf")).hexdigest()
    assert not list((bench.root / "raw" / "example" / "objects").rglob(sitemap))  # read every day, never kept

    bench.site.requests.clear()
    second = bench.run(days=1)
    # The pages whose links reach the most held files: /publications reaches three, /hazcom the one more it links to.
    assert bench.paths() == ["/robots.txt", "/sitemap.xml", "/publications", "/hazcom"]
    assert [request.headers.get("if-none-match") for request in bench.site.requests[2:]] == ['"p1"', '"z1"']
    assert (second["status"], second["details"]["checked_unchanged"], bench.found(second)) == ("ok", 2, [])
    assert second["details"]["hubs"] == {"chosen": 2, "asked": 2, "checked_already": 0, "left": 0}


def test_the_re_check_asks_held_files_due_longest_first_and_records_what_each_answer_means(bench):
    baseline(bench)
    bench.site.set("/files/a.pdf", (304, b"", {}))
    bench.site.set("/files/b.pdf", (200, pdf_bytes(311), {**PDF, "etag": '"b2"'}))  # a new edition
    bench.site.set("/files/c.pdf", (404, b"", {}))
    bench.site.set("/files/e.pdf", (410, b"", {}))
    bench.site.set("/report.pdf", (200, pdf_bytes(400), {**PDF, "etag": '"r2"'}))  # put back unchanged
    bench.site.set("/hazcom", (200, page("Hazard communication", "/files/c.pdf", "/files/e.pdf",
                                         header="<span class='js-view-dom-id-0f3e'>menu</span>"), HTML))
    week = bench.run(days=7)
    # The hubs, then the files, due a week after their download, in the order they were checked.
    assert bench.asked() == ["/publications", "/hazcom", "/report.pdf", "/files/c.pdf", "/files/e.pdf",
                             "/files/a.pdf", "/files/b.pdf"]
    sent = {request.url.path: request.headers.get("if-none-match") for request in bench.site.requests}
    assert sent["/files/a.pdf"] == '"a1"' and sent["/report.pdf"] == '"r1"'  # every check is conditional
    assert bench.found(week) == [("/files/c.pdf", "gone"), ("/files/e.pdf", "gone"), ("/files/b.pdf", "changed")]
    assert week["details"]["checked_unchanged"] == 4  # the hubs (one in new bytes), the 304 and the same report
    assert (bench.row("/files/a.pdf")["state"], bench.row("/report.pdf")["etag"]) == ("unchanged", '"r2"')
    gone = bench.row("/files/c.pdf")
    assert (gone["state"], gone["gone_since"]) == ("not_found", iso(bench.clock.now - timedelta(seconds=40)))
    assert gone["purge_after"] == iso(datetime.fromisoformat(gone["gone_since"]) + timedelta(days=30))
    assert bench.change("/files/b.pdf", "changed")["previous_content_sha256"] == hashlib.sha256(
        pdf_bytes(310)).hexdigest()

    # A week on: the gone file is back, the report has moved, a file serves an error page, one refuses, one is busy.
    bench.site.set("/files/c.pdf", (200, pdf_bytes(320), PDF))
    bench.site.set("/report.pdf", (301, b"", {"location": "/reports/2026.pdf"}))
    bench.site.set("/reports/2026.pdf", (200, pdf_bytes(401), PDF))
    bench.site.set("/files/b.pdf", (200, SOFT_ERROR, PDF))
    bench.site.set("/files/a.pdf", (403, b"Access Denied", {}))
    bench.site.set("/files/e.pdf", (429, b"", {"retry-after": "120"}))
    bench.site.requests.clear()
    later = bench.run(days=7)
    assert bench.found(later) == [("/report.pdf", "moved"), ("/reports/2026.pdf", "new"),
                                  ("/files/c.pdf", "restored"), ("/files/b.pdf", "rejected")]
    assert bench.asked().index("/reports/2026.pdf") == bench.asked().index("/report.pdf") + 1  # new URLs come first
    assert bench.change("/files/b.pdf", "rejected")["reason"] == "soft_error"
    for path in ("/files/a.pdf", "/files/b.pdf"):  # a refusal or a refused body leaves the copy held current
        row = bench.row(path)
        assert row["state"] in ("fetched", "unchanged") and "the copy held stays current" in row["error"], path
        assert iso(bench.clock.now) < row["next_check_at"] <= iso(bench.clock.now + timedelta(days=1)), path
    assert bench.row("/files/e.pdf")["gone_since"] is not None  # a 429 for a gone file changes nothing
    assert 120.0 in bench.clock.sleeps  # the Retry-After asked for, longer than the Crawl-delay
    assert later["status"] == "ok"


def test_a_hub_that_drops_its_last_link_to_a_file_has_the_file_confirmed(bench):
    baseline(bench)
    bench.run(days=1)
    # /publications drops a.pdf (linked from nowhere else) and c.pdf (still linked from /hazcom, a hub checked
    # yesterday), and links a new file.
    bench.site.set("/publications", (200, page("Publications", "/files/b.pdf", "/files/d.pdf"), HTML))
    bench.site.set("/files/d.pdf", (200, pdf_bytes(340), PDF))
    bench.site.set("/files/a.pdf", (404, b"", {}))
    bench.site.requests.clear()
    today = bench.run(days=1)
    assert bench.asked() == ["/publications", "/hazcom", "/files/d.pdf", "/files/a.pdf"]  # hubs, new, confirmation
    assert bench.found(today) == [("/publications", "changed"), ("/files/d.pdf", "new"), ("/files/a.pdf", "gone")]
    assert today["details"]["confirmations_queued"] == {"no longer linked": 1}
    assert bench.change("/files/a.pdf", "gone")["note"].startswith(
        f"asked to confirm: no longer linked from {HOST}/publications")
    with bench.db() as db:
        links = {row[0].removeprefix(HOST) for row in db.execute("select to_url from links where from_url = ?",
                                                                (f"{HOST}/publications",))}
    assert links == {"/", "/files/b.pdf", "/files/d.pdf"}  # the page's links as they stand today
    assert (bench.row("/files/c.pdf")["confirm_reason"], bench.row("/files/c.pdf")["state"]) == (None, "fetched")


def test_the_sitemap_pass_queues_new_urls_confirms_dropped_ones_and_clears_those_listed_again(bench):
    baseline(bench)
    bench.site.set("/sitemap.xml", (200, urlset("/", "/hazcom", "/publications", "/news"), XML))
    bench.site.set("/news", (200, page("News"), HTML))
    bench.site.set("/report.pdf", (410, b"", {}))
    dropped = bench.run(days=1)
    assert bench.asked() == ["/publications", "/hazcom", "/news", "/report.pdf"]  # hubs, new, confirmation
    assert bench.found(dropped) == [("/news", "new"), ("/report.pdf", "gone")]
    assert dropped["details"]["sitemaps"] == {"read": True, "complete": True, "listed": 4, "added": 1, "refused": [],
                                              "unread": [], "dropped": 1, "listed_again": 0, "asked_again": 0}
    report = bench.row("/report.pdf")
    assert report["dropped_from_sitemap_at"] is not None and report["confirm_reason"] is None
    assert bench.change("/report.pdf", "gone")["note"].startswith("asked to confirm: dropped from the sitemap")

    # Listed again: the mark goes, and the URL, gone since, is asked about in its own time (a week after).
    bench.site.set("/sitemap.xml", (200, urlset("/", "/hazcom", "/publications", "/news", "/report.pdf"), XML))
    bench.site.requests.clear()
    back = bench.run(days=1)
    assert back["details"]["sitemaps"]["listed_again"] == 1
    assert bench.row("/report.pdf")["dropped_from_sitemap_at"] is None and "/report.pdf" not in bench.asked()


def test_a_sitemap_read_only_in_part_judges_no_drop_and_a_refused_one_says_so(bench):
    baseline(bench)
    bench.site.set("/sitemap.xml", (503, b"", {}))
    partial = bench.run(days=1)
    assert partial["status"] == "partial" and "sitemaps not read whole" in partial["note"]
    assert partial["details"]["sitemaps"]["unread"] == [f"{HOST}/sitemap.xml: HTTP 503"]
    assert all(bench.row(path)["dropped_from_sitemap_at"] is None for path in ("/", "/hazcom", "/report.pdf"))
    assert partial["details"]["hubs"]["asked"] == 2  # the rest of the run goes on

    bench.site.set("/sitemap.xml", (200, b"<!DOCTYPE html><html><body>Request blocked</body></html>", HTML))
    refused = bench.run(days=1)
    assert refused["details"]["sitemaps"]["refused"] == [
        f"{HOST}/sitemap.xml: refused, the host served an HTML page instead of the sitemap"]
    assert refused["status"] == "partial" and bench.row("/report.pdf")["dropped_from_sitemap_at"] is None


def test_a_sitemap_that_drops_too_many_held_urls_at_once_queues_no_confirmations(bench, monkeypatch):
    baseline(bench)
    monkeypatch.setattr(daily, "SUSPECT_DROPS", 2)
    bench.site.set("/sitemap.xml", (200, urlset("/"), XML))  # three held URLs gone from it at once
    result = bench.run(days=1)
    assert "broken sitemap" in result["details"]["sitemaps"]["suspect"]
    assert result["details"]["confirmations_queued"] == {} and bench.asked() == ["/publications", "/hazcom"]
    assert bench.row("/report.pdf")["dropped_from_sitemap_at"] is not None


def test_a_url_listed_again_is_asked_again_but_not_when_a_broken_sitemap_brings_back_many_at_once(bench, monkeypatch):
    listing = ["/", "/hazcom", "/publications", "/report.pdf", "/old-1", "/old-2", "/old-3"]
    bench.site.set("/sitemap.xml", (200, urlset(*listing), XML))
    baseline(bench)  # the three /old-* answer 404: listed, never held
    monkeypatch.setattr(daily, "SUSPECT_DROPS", 2)
    bench.site.set("/sitemap.xml", (200, urlset(*listing[:-1]), XML))
    bench.run(days=1)  # /old-3 is dropped
    bench.site.set("/sitemap.xml", (200, urlset(*listing), XML))
    bench.site.requests.clear()
    back = bench.run(days=1)  # and listed again: asked again, as a new URL
    assert back["details"]["sitemaps"]["asked_again"] == 1 and "/old-3" in bench.asked()

    bench.site.set("/sitemap.xml", (200, urlset("/"), XML))
    broken = bench.run(days=1)  # a broken read drops six at once, three of them held
    assert "broken sitemap" in broken["details"]["sitemaps"]["suspect"]
    bench.site.set("/sitemap.xml", (200, urlset(*listing), XML))
    bench.site.requests.clear()
    recovered = bench.run(days=1)  # its end brings all six back: the marks go, and nothing is asked for it
    sitemaps = recovered["details"]["sitemaps"]
    assert (sitemaps["listed_again"], sitemaps["asked_again"]) == (6, 0) and "broken sitemap" in sitemaps["recovered"]
    assert bench.asked() == ["/publications", "/hazcom"]
    assert all(bench.row(path)["dropped_from_sitemap_at"] is None for path in listing)
    assert {bench.row(path)["state"] for path in listing[4:]} == {"not_found"}


def test_the_queue_asks_new_urls_then_confirmations_then_returns_then_the_re_check(bench, monkeypatch):
    bench.site.set("/sitemap.xml", (200, urlset("/", "/hazcom", "/publications", "/report.pdf", "/old-1", "/old-2"),
                                    XML))
    baseline(bench)  # /old-1 and /old-2 answer 404: listed, never held
    monkeypatch.setattr(daily, "RETURNING_PER_RUN", 1)
    bench.site.set("/sitemap.xml", (200, urlset("/", "/hazcom", "/publications", "/old-1", "/old-2", "/new"), XML))
    bench.site.set("/new", (200, page("New"), HTML))
    bench.site.set("/old-1", (200, page("Back again"), HTML))
    result = bench.run(days=31)
    assert bench.asked() == ["/publications", "/hazcom",  # the hub pass
                             "/new", "/report.pdf",  # a new URL, a confirmation (dropped from the sitemap)
                             "/old-1",  # a URL that never gave a copy, asked again after 30 days (one a run here)
                             # the one due longest first: the files since a week after their download, the page since
                             # a month after
                             "/files/c.pdf", "/files/e.pdf", "/files/a.pdf", "/files/b.pdf", "/"]
    assert result["details"]["asked"] == {"hub": 2, "new": 1, "confirmation": 1, "returning": 1, "recheck": 5}
    assert ("/old-1", "new") in bench.found(result) and bench.row("/old-2")["state"] == "not_found"
    assert result["details"]["left"]["returning_due"] == 1  # /old-2's turn comes next run


def test_files_keep_their_weekly_turn_when_pages_checked_longer_ago_come_due(bench):
    """A file is due a week after its check and a page a month after. When the run cannot ask everything that is due,
    the files due since a week and more go before the pages due only since today, although those were checked longer
    ago: OSHA's crawl fetched its 34,168 pages in six days, so a month on they come due together, more than six days of
    requests, and files taken after them would wait two weeks and more."""
    baseline(bench)
    queue_only = Plan(budget=1000)
    bench.run(days=9, plan=queue_only)  # the five files, due since day 7, checked again; the pages are due on day 30
    bench.site.requests.clear()
    result = bench.run(days=22, plan=queue_only, budget=4)  # day 31: robots.txt and three re-checks
    assert bench.asked() == ["/report.pdf", "/files/c.pdf", "/files/e.pdf"]  # due since day 16, the pages since day 30
    assert result["stop_reason"] == "request limit" and result["details"]["left"]["checks_due"] == 5


def test_a_sustained_refusal_stops_the_run_and_retires_nothing(bench):
    links = [f"/files/f{number:02}.pdf" for number in range(45)]
    bench.site.set("/publications", (200, page("Publications", *links), HTML))
    for link in links:
        bench.site.set(link, (200, pdf_bytes(300 + int(link[-6:-4])), PDF))
    baseline(bench)
    held = bench.held()
    for path in ["/", "/hazcom", "/publications", "/sitemap.xml", "/report.pdf", *links,
                 *(f"/files/{name}.pdf" for name in FILES)]:
        bench.site.set(path, (403, b"<html><body>Access Denied</body></html>", {}))
    bench.site.set("/files/f00.pdf", (404, CHALLENGE, {}))  # a challenge page served as 404 is a refusal too
    result = bench.run(days=8)
    assert (result["status"], result["stop_reason"]) == ("refused", "possible block")
    assert result["requests"] == 2 + 40 and bench.clock.sleeps.count(BLOCK_PAUSE_SECONDS) == 3
    assert "nothing was retired" in result["note"] and bench.found(result) == []
    assert bench.held() == held and "HTTP 404" in bench.row("/files/f00.pdf")["error"]
    with bench.db() as db:
        assert db.execute("select count(*) from urls where gone_since is not null").fetchone()[0] == 0


def many_files(bench: Bench, count: int) -> list[str]:
    """/publications links `count` more files, each its own PDF."""
    links = [f"/files/f{number:03}.pdf" for number in range(count)]
    bench.site.set("/publications", (200, page("Publications", *links), HTML))
    for number, link in enumerate(links):
        bench.site.set(link, (200, pdf_bytes(1000 + number), PDF))
    return links


def test_urls_that_refused_before_never_make_a_block_and_take_turns(bench):
    links = many_files(bench, 15)
    baseline(bench)
    for link in links:
        bench.site.set(link, (403, b"Access Denied", {}))
    first = bench.run(days=8)  # a first refusal of each: the crawl's own rule pauses once after ten in a row
    assert bench.clock.sleeps.count(BLOCK_PAUSE_SECONDS) == 1 and first["stop_reason"] == "done"
    bench.site.requests.clear()
    second = bench.run(days=1)  # now they refused before: nine in a row, then the rest wait
    assert bench.asked()[2:] == links[:9] and bench.clock.sleeps.count(BLOCK_PAUSE_SECONDS) == 1
    assert second["status"] == "ok" and "refused before still refused 9 times in a row" in second["note"]
    bench.site.requests.clear()
    bench.run(days=1)
    assert bench.asked()[2:] == [*links[9:], *links[:3]]  # those left yesterday come first
    assert held_copies_kept(bench, links)


def held_copies_kept(bench: Bench, paths: list[str]) -> bool:
    return all(bench.row(path)["state"] == "fetched" and bench.row(path)["gone_since"] is None for path in paths)


def test_a_site_that_stops_answering_is_waited_for_then_left_for_the_next_run(bench):
    links = many_files(bench, 125)
    baseline(bench)
    for path in ["/", "/hazcom", "/publications", "/report.pdf", *links, *(f"/files/{name}.pdf" for name in FILES)]:
        bench.site.set(path, (503, b"", {}))
    result = bench.run(days=8)
    assert (result["status"], result["stop_reason"]) == ("unavailable", "site unavailable")
    assert result["requests"] == 2 + 4 * daily.OUTAGE_THRESHOLD and bench.clock.sleeps.count(BLOCK_PAUSE_SECONDS) == 3
    assert bench.found(result) == [] and held_copies_kept(bench, links[:20])


def test_a_robots_txt_that_disallows_everything_is_a_refusal(bench):
    baseline(bench)
    bench.site.set("/robots.txt", (200, b"User-agent: *\nDisallow: /\n", {}))
    result = bench.run(days=8)
    assert bench.paths() == ["/robots.txt"] and result["status"] == "refused"
    assert "URLs robots.txt disallows, not asked (their copies stay current)" in result["note"]
    assert "disallowed by robots.txt" in bench.row("/files/a.pdf")["error"] and bench.found(result) == []


STRICTER = example_site(name="example", sitemaps=(f"{HOST}/sitemap.xml",), max_bytes=10**6,
                        unneeded_paths=("/grants/",))  # a rule added after the crawl: /grants/ is not needed


def test_held_copies_the_project_does_not_need_are_kept_and_never_asked_about(bench):
    """Copies downloaded before a not-needed rule named them stay held, but a run marks them first: they are neither
    re-checked, confirmed nor hubs, and the cycle leaves them out, so a new link on such a page is never even read
    (on OSHA 13,855 of the 41,118 held copies, 2026-11-24). Once the rules no longer name them they are asked again."""
    bench.site.set("/sitemap.xml", (200, urlset("/", "/hazcom", "/publications", "/report.pdf", "/grants/list"), XML))
    bench.site.set("/grants/list", (200, page("Grants", "/files/grant-0.pdf"), HTML))
    bench.site.set("/files/grant-0.pdf", (200, pdf_bytes(690), PDF))  # linked from the grants page alone
    baseline(bench)  # all ten held, before the rule
    bench.site.set("/grants/list", (200, page("Grants", "/files/grant-0.pdf", "/files/grant-1.pdf"), HTML))
    bench.site.set("/files/grant-1.pdf", (200, pdf_bytes(700), PDF))
    bench.site.set("/sitemap.xml", (200, urlset("/", "/hazcom", "/publications", "/report.pdf"), XML))  # dropped
    result = bench.run(days=31, site=STRICTER)  # every copy is due
    assert not [path for path in bench.asked() if "grant" in path]
    assert result["details"]["held_not_needed"] == {"held": 2, "marked": 2, "cleared": 0}
    assert result["details"]["confirmations_queued"] == {}  # dropped from the sitemap, but not needed
    assert result["details"]["cycle"]["held"] == 8 and result["details"]["left"]["checks_due"] == 0
    assert "held copies the project does not need, kept but no longer asked about: 2" in result["note"]
    for path in ("/grants/list", "/files/grant-0.pdf"):  # kept as they were, only marked
        row = bench.row(path)
        assert (row["state"], row["skip_reason"], row["gone_since"]) == ("fetched", "not_needed", None), path
        assert row["content_sha256"] is not None and row["confirm_reason"] is None, path
    assert bench.row("/grants/list")["dropped_from_sitemap_at"] is not None
    assert bench.row("/files/grant-1.pdf") == {}  # the page that links it was not read again

    bench.site.requests.clear()
    again = bench.run(days=1)  # the rule is gone
    assert again["details"]["held_not_needed"] == {"held": 0, "marked": 0, "cleared": 2}
    assert {"/grants/list", "/files/grant-0.pdf", "/files/grant-1.pdf"} <= set(bench.asked())
    assert ("/files/grant-1.pdf", "new") in bench.found(again)


def test_a_url_due_for_its_return_that_the_project_does_not_need_is_skipped_never_asked(bench):
    """A URL that answered 404 before a not-needed rule named it is never asked for its return: were it back, it would
    be a new URL the project does not need (on OSHA 445 of the 1,792 such URLs, 2026-11-24)."""
    bench.site.set("/sitemap.xml", (200, urlset("/", "/hazcom", "/publications", "/report.pdf", "/grants/old",
                                                 "/old-1"), XML))
    baseline(bench)  # /grants/old and /old-1 answer 404: listed, never held
    bench.site.set("/grants/old", (200, page("Back again"), HTML))
    result = bench.run(days=31, site=STRICTER)
    assert "/grants/old" not in bench.asked() and "/old-1" in bench.asked()
    assert (bench.row("/grants/old")["state"], bench.row("/grants/old")["skip_reason"]) == ("skipped", "not_needed")
    assert result["details"]["skipped_not_needed"] == {"returning": 1} and result["details"]["asked"]["returning"] == 1
    assert "1 URLs the project does not need, skipped (1 returning)" in result["note"]
    assert not [path for path, _ in bench.found(result) if "grants" in path]


def test_a_url_that_refused_is_not_asked_for_its_return(bench):
    bench.site.set("/sitemap.xml", (200, urlset("/", "/hazcom", "/publications", "/report.pdf", "/refused",
                                                 "/broken"), XML))
    bench.site.set("/refused", (403, b"Access Denied", {}))
    bench.site.set("/broken", (500, b"", {}))
    baseline(bench)  # each tried three times, at 5 and 30 minutes, then an error
    assert (bench.row("/refused")["state"], bench.row("/broken")["state"]) == ("error", "error")
    result = bench.run(days=31)
    assert "/broken" in bench.asked() and "/refused" not in bench.asked()
    assert result["details"]["asked"]["returning"] == 1


def test_a_run_stops_at_its_deadline_and_the_next_goes_on_where_it_stopped(bench):
    baseline(bench)
    before = bench.clock.now
    cut = bench.run(days=1, hours=15 / 3600)  # robots.txt and the sitemap, then the time is up
    assert (cut["status"], cut["stop_reason"]) == ("partial", "time limit")
    assert "2 hubs not asked" in cut["note"] and bench.clock.now == before + timedelta(days=1, seconds=15)
    bench.site.requests.clear()
    resumed = bench.run(days=1 / 24)
    assert bench.paths() == ["/robots.txt", "/publications", "/hazcom"]  # the sitemap was read whole an hour ago
    assert resumed["status"] == "ok" and resumed["details"]["sitemaps"]["read"] is False
    bench.site.requests.clear()
    again = bench.run(days=1 / 24)
    assert bench.paths() == ["/robots.txt"] and again["details"]["hubs"]["checked_already"] == 2


def test_a_run_keeps_to_its_request_budget_and_the_next_asks_what_it_left(bench):
    first = bench.run(budget=5)  # robots.txt, the sitemap and three of the eight new URLs
    assert (first["status"], first["stop_reason"], first["requests"]) == ("partial", "request limit", 5)
    assert first["details"]["left"]["new"] == 5 and "5 new URLs waiting" in first["note"]
    bench.site.requests.clear()
    second = bench.run(days=1 / 24)
    assert bench.paths() == ["/robots.txt", "/report.pdf", "/files/c.pdf", "/files/e.pdf", "/files/a.pdf",
                             "/files/b.pdf"]
    assert second["status"] == "ok" and len(bench.found()) == 8


def test_a_crash_loses_only_the_answer_being_recorded_and_the_next_run_resumes(bench):
    calls, answer = [], bench.site.handler

    def crashing(request):
        calls.append(request.url.path)
        if len(calls) == 6:  # asking for /report.pdf
            raise RuntimeError("the disk is full")
        return answer(request)

    bench.client = httpx.Client(transport=httpx.MockTransport(crashing))
    with pytest.raises(RuntimeError, match="the disk is full"):
        bench.run()
    with bench.db() as db:
        assert db.execute("select stop_reason from runs").fetchone()[0] == "interrupted"
        assert db.execute("select count(*) from urls where state = 'fetched'").fetchone()[0] == 3
    assert bench.found() == [("/", "new"), ("/hazcom", "new"), ("/publications", "new")]  # each recorded whole
    bench.site.requests.clear()
    resumed = bench.run(days=1 / 24)
    assert bench.asked() == ["/report.pdf", "/files/c.pdf", "/files/e.pdf", "/files/a.pdf", "/files/b.pdf"]
    assert resumed["status"] == "ok" and len(bench.found()) == 8
    bench.client.close()


def test_choose_hubs_covers_the_most_files_with_the_fewest_needed_pages(tmp_path):
    store = Store(tmp_path / "crawl.sqlite")
    pages = {"/a": ["/1.pdf", "/2.pdf", "/3.pdf"], "/b": ["/3.pdf", "/4.pdf"], "/c": ["/4.pdf", "/5.pdf"],
             "/grants/list": ["/6.pdf", "/7.pdf", "/8.pdf", "/9.pdf"], "/d": ["/5.pdf"]}
    files = {f"/{number}.pdf" for number in range(1, 10)} | {"/gone.pdf"}
    for path in [*pages, *files]:
        store.insert_url(HOST + path, source="sitemap", state="fetched", now="2026-11-18T00:00:00Z")
        store.update(HOST + path, content_sha256="x", sha256="x",
                     detected_type="pdf" if path.endswith(".pdf") else "html")
    store.update(f"{HOST}/gone.pdf", state="not_found", gone_since="2026-11-20T00:00:00Z")
    for path, targets in pages.items():
        store.add_links(HOST + path, [HOST + target for target in [*targets, "/gone.pdf"]])
    site = example_site(unneeded_paths=("/grants/",))  # the listing the project does not need is never a hub
    assert choose_hubs(store.db, site, 10) == [f"{HOST}/a", f"{HOST}/c"]  # /b and /d add nothing after them
    assert choose_hubs(store.db, site, 1) == [f"{HOST}/a"]
    store.close()


def test_holds_and_its_sql_form_agree(tmp_path):
    store = Store(tmp_path / "crawl.sqlite")
    cases = {"verified": {"content_sha256": "c", "sha256": "s"}, "unexamined": {"sha256": "s"},
             "refused": {"sha256": "s", "state": "rejected", "detected_type": "html"},
             "examined-and-refused": {"sha256": "s", "detected_type": "html"}, "nothing": {}}
    for name, fields in cases.items():
        store.insert_url(f"{HOST}/{name}", source="sitemap", state="fetched", now="2026-11-18T00:00:00Z")
        if fields:
            store.update(f"{HOST}/{name}", **fields)
    held = {row[0] for row in store.db.execute(f"select url from urls where {HELD_SQL}")}
    assert held == {row["url"] for row in store.db.execute("select * from urls") if holds(row)} == {
        f"{HOST}/verified", f"{HOST}/unexamined"}
    store.close()


# www.msha.gov, eCFR and the Federal Register --------------------------------------------------------------------------


MSHA = example_site(name="msha", canonical_host="www.msha.example", hosts=frozenset({"www.msha.example"}),
                    sitemaps=("https://www.msha.example/sitemap.xml",))


def probe(*sitemap_answers, robots=(200, b"User-agent: *\n", {})) -> dict:
    site, clock = Answers(), Clock()
    site.set("/robots.txt", robots)
    site.set("/sitemap.xml", *sitemap_answers)
    with httpx.Client(transport=httpx.MockTransport(site.handler)) as client:
        return sources.probe_msha(MSHA, client, sleep=clock.sleep, clock=lambda: clock.now.timestamp())


@pytest.mark.parametrize(("answer", "status", "words"), [
    ((403, b"", {}), "refused", "refused, the host answered HTTP 403"),
    ((200, b"<!DOCTYPE html><html><head><meta charset=utf-8></head><body>Blocked</body></html>", HTML), "refused",
     "served an HTML page"),
    ((503, b"", {}), "unavailable", "HTTP 503"),
    ("no answer", "unavailable", "ConnectError"),
    ((200, b"<urlset><url>", XML), "error", "not well-formed XML"),
    ((200, urlset("/", "/news"), XML), "skipped", "not updated automatically"),
])
def test_www_msha_gov_is_probed_once_and_a_refusal_is_recorded(answer, status, words):
    result = probe(answer)
    assert result["status"] == status and words in result["note"] and result["requests"] == 2


def test_a_robots_txt_that_disallows_the_probe_is_a_refusal_and_nothing_else_is_asked():
    result = probe((200, urlset("/"), XML), robots=(200, b"User-agent: *\nDisallow: /\n", {}))
    assert (result["status"], result["requests"]) == ("refused", 1) and "disallows" in result["note"]


def ecfr_version(root: Path, title: int, body: bytes, seen: str) -> str:
    """A govinfo title file held in the bulk store, first seen at `seen`; returns its SHA-256."""
    digest = hashlib.sha256(body).hexdigest()
    path = root / "raw" / "bulk" / "objects" / digest[:2] / digest[2:4] / digest
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    store = BulkStore(root / "state" / "bulk.sqlite")
    url = f"https://www.govinfo.gov/bulkdata/ECFR/title-{title}/ECFR-title{title}.xml"
    store.db.execute("insert into versions (url, sha256, content_length, last_modified, first_seen_at)"
                     " values (?, ?, ?, ?, ?)", (url, digest, len(body), None, seen))
    store.close()
    return digest


def title_file(title: int, amended: str) -> bytes:
    """The start of a govinfo eCFR title file, as the real ones begin (AMDDATE about 450 bytes in)."""
    return (f'<?xml version="1.0" encoding="UTF-8" ?>\n<DLPSTEXTCLASS>\n<HEADER>\n<FILEDESC>\n<TITLESTMT>\n<TITLE>\n'
            f"Title {title}</TITLE>\n</TITLESTMT>\n</FILEDESC>\n</HEADER>\n<AMDDATE>{amended}\n</AMDDATE>\n"
            "</DLPSTEXTCLASS>\n").encode()


def test_the_ecfr_titles_whose_govinfo_edition_changed_since_the_last_run_become_hints(tmp_path):
    state = OfficialState(tmp_path / "state" / "official.sqlite")
    assert sources.ecfr_hints(tmp_path, state, now=START, run_id="r0")["status"] == "skipped"  # no bulk store yet
    ecfr_version(tmp_path, 29, title_file(29, "Nov. 9, 2026"), "2026-11-01T14:10:00Z")  # before the first look-back
    title_30 = ecfr_version(tmp_path, 30, title_file(30, "Nov. 12, 2026"), "2026-11-23T14:10:00Z")
    first = sources.ecfr_hints(tmp_path, state, now=START, run_id="r1")
    assert first["details"]["hints"] == [{"title": 30, "issue_date": "2026-11-12", "previous_issue_date": None,
                                          "sha256": title_30}]
    ecfr_version(tmp_path, 29, title_file(29, "Nov. 19, 2026"), "2026-11-25T14:10:00Z")
    second = sources.ecfr_hints(tmp_path, state, now=START + timedelta(days=1), run_id="r2")
    assert [(hint["title"], hint["issue_date"], hint["previous_issue_date"])
            for hint in second["details"]["hints"]] == [(29, "2026-11-19", "2026-11-09")]
    assert second["note"] == "title 29: issue 2026-11-19 (was 2026-11-09)"
    third = sources.ecfr_hints(tmp_path, state, now=START + timedelta(days=2), run_id="r3")
    assert third["details"]["hints"] == [] and third["note"] == "no new govinfo edition since 2026-11-25T14:10:00Z"
    assert state.db.execute("select count(*) from hints where source = 'ecfr'").fetchone()[0] == 2
    state.close()


class FederalRegister:
    """The Federal Register API's documents endpoint: final rules per CFR title from `rules`, or one status for all."""

    def __init__(self, rules: dict[int, list[dict]]):
        self.rules, self.requests, self.status = rules, [], None

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text="User-agent: *\nDisallow: /search\n")
        if self.status is not None:
            return httpx.Response(self.status, text="<html><body>Busy</body></html>")
        params = request.url.params
        assert params.get_list("conditions[type][]") == ["RULE"]
        title, since = int(params["conditions[cfr][title]"]), params["conditions[publication_date][gte]"]
        results = [rule for rule in self.rules.get(title, []) if rule["publication_date"] >= since]
        return httpx.Response(200, json={"count": len(results), "results": results})


def rule(number: str, published: str, *titles: int, effective: str | None = None) -> dict:
    return {"document_number": number, "publication_date": published, "effective_on": effective, "type": "Rule",
            "cfr_references": [{"title": title, "part": 1910} for title in titles]}


def test_final_rules_amending_title_29_or_30_since_the_last_run_become_hints_once(tmp_path):
    api = FederalRegister({29: [rule("2026-24001", "2026-11-23", 29, effective="2027-01-01"),
                                rule("2026-23001", "2026-11-10", 29)],
                           30: [rule("2026-24002", "2026-11-24", 30, 29, effective="2026-12-24")]})
    state, clock = OfficialState(tmp_path / "official.sqlite"), Clock()
    with httpx.Client(transport=httpx.MockTransport(api.handler)) as client:
        def read(now: datetime) -> dict:
            return sources.federal_register_hints(client, state, now=now, run_id=iso(now), sleep=clock.sleep,
                                                  clock=lambda: clock.now.timestamp())

        first = read(START)
        assert [request.url.params.get("conditions[publication_date][gte]") for request in api.requests[1:]] == [
            "2026-11-17", "2026-11-17"]  # a first run looks back a week
        assert [(hint["title"], hint["document_number"], hint["effective_on"])
                for hint in first["details"]["hints"]] == [(29, "2026-24001", "2027-01-01"),
                                                           (30, "2026-24002", "2026-12-24")]
        api.rules[29].append(rule("2026-24100", "2026-11-25", 29))
        second = read(START + timedelta(days=1))
        assert api.requests[-1].url.params["conditions[publication_date][gte]"] == "2026-11-24"  # from the last read
        assert [hint["document_number"] for hint in second["details"]["hints"]] == ["2026-24100"]  # none twice
        api.status = 403
        refused = read(START + timedelta(days=2))
        assert refused["status"] == "refused" and state.mark("federal_register:read_through") == "2026-11-25"
        api.status = 503
        assert read(START + timedelta(days=2))["status"] == "unavailable"
    assert state.db.execute("select count(*) from hints where source = 'federal_register'").fetchone()[0] == 3
    state.close()


# A whole run ----------------------------------------------------------------------------------------------------------


OSHA = example_site(name="osha", sitemaps=(f"{HOST}/sitemap.xml",), max_bytes=10**6)
LEGACY = example_site(name="arlweb", canonical_host="legacy.example.test", hosts=frozenset({"legacy.example.test"}),
                      sitemaps=())
SITES = {"osha": OSHA, "arlweb": LEGACY, "msha": MSHA}
PLANS = {"osha": PLAN, "arlweb": Plan(budget=100, share=timedelta(hours=2))}


@dataclass
class Whole:
    root: Path
    clock: Clock
    client: httpx.Client

    def run(self, **options) -> dict:
        return run_official(self.root, self.client, sites=SITES, plans=PLANS, sleep=lambda seconds: self.clock.sleep(
            seconds), now=lambda: self.clock.now, until=self.clock.now + timedelta(hours=12), **options)


@pytest.fixture
def whole(tmp_path):
    """The example site as OSHA, its legacy host as arlweb (a URL the MSHA crawl's links seeded, as on the VM), an
    MSHA host that refuses, and the Federal Register API, on one mocked network."""
    example, legacy, clock = site_answers(), Answers(), Clock()
    clock.now = START
    legacy.set("/robots.txt", (200, b"User-agent: *\n", {}))
    legacy.set("/old/report.pdf", (200, pdf_bytes(500), PDF))
    api = FederalRegister({29: [rule("2026-24001", "2026-11-23", 29)]})
    hosts = {"www.example.test": example.handler, "legacy.example.test": legacy.handler,
             "www.federalregister.gov": api.handler}

    def network(request: httpx.Request) -> httpx.Response:
        handler = hosts.get(request.url.host)
        return handler(request) if handler else httpx.Response(403)  # www.msha.example refuses everything

    Store(tmp_path / "state" / "crawl-osha.sqlite").close()
    store = Store(tmp_path / "state" / "crawl-arlweb.sqlite")
    store.insert_url("https://legacy.example.test/old/report.pdf", source="links:msha", state="pending",
                     now="2026-11-18T00:00:00Z")
    store.close()
    client = httpx.Client(transport=httpx.MockTransport(network))
    yield Whole(tmp_path, clock, client)
    client.close()


def test_a_whole_run_asks_every_source_in_turn_and_records_each(whole):
    result = whole.run()
    assert list(result["sources"]) == ["msha", "ecfr", "federal_register", "arlweb", "osha"]
    assert {name: outcome["status"] for name, outcome in result["sources"].items()} == {
        "msha": "refused", "ecfr": "skipped", "federal_register": "ok", "arlweb": "ok", "osha": "ok"}
    state = sqlite3.connect(whole.root / "state" / "official.sqlite")
    state.row_factory = sqlite3.Row
    try:
        [official] = state.execute("select * from runs").fetchall()
        assert (official["run_id"], official["stop_reason"], official["trigger"]) == (result["run_id"], "done", "cli")
        rows = {row["source"]: row for row in state.execute("select * from sources")}
        assert rows["msha"]["host"] == "www.msha.gov" and "never worked around" in rows["msha"]["note"]
        assert rows["osha"]["crawl_run_id"] and json.loads(rows["osha"]["details"])["changes"] == {"new": 8}
        assert json.loads(rows["arlweb"]["details"])["changes"] == {"new": 1}
        assert state.execute("select document_number from hints").fetchone()[0] == "2026-24001"
    finally:
        state.close()


def test_a_site_whose_lock_another_writer_holds_is_skipped_after_a_short_wait(whole):
    started = whole.clock.now
    with SiteLock(whole.root / "state" / "crawl-osha.lock"):
        result = whole.run(sources=["osha", "arlweb"])
    osha = result["sources"]["osha"]
    assert (osha["status"], osha["stop_reason"]) == ("skipped", "lock held") and "crawl-osha.lock" in osha["note"]
    assert result["sources"]["arlweb"]["status"] == "ok"  # the others still run
    assert started + run.LOCK_WAIT <= whole.clock.now < started + run.LOCK_WAIT + timedelta(minutes=5)


def test_a_short_holder_of_a_lock_such_as_a_watchers_check_is_waited_for(whole):
    lock = SiteLock(whole.root / "state" / "crawl-osha.lock").__enter__()
    real_sleep = whole.clock.sleep

    def sleep(seconds: float) -> None:  # the watcher finishes its read while the run waits
        if lock.handle is not None:
            lock.__exit__(None, None, None)
        real_sleep(seconds)

    whole.clock.sleep = sleep
    result = whole.run(sources=["osha"])
    assert result["sources"]["osha"]["status"] == "ok" and run.LOCK_RETRY_SECONDS in whole.clock.sleeps


def test_a_source_that_fails_is_recorded_as_an_error_and_the_others_still_run(whole, monkeypatch):
    def broken(*args, **kwargs):
        raise OSError("the disk is full")

    monkeypatch.setattr(run, "ecfr_hints", broken)
    result = whole.run(sources=["ecfr", "federal_register"])
    assert result["sources"]["ecfr"] == {"status": "error", "note": "OSError: the disk is full"}
    assert result["sources"]["federal_register"]["status"] == "ok"
    with closing(sqlite3.connect(whole.root / "state" / "official.sqlite")) as state:
        assert state.execute("select status, stop_reason from sources where source = 'ecfr'").fetchone() == (
            "error", "interrupted")
        assert state.execute("select stop_reason from runs").fetchone()[0] == "errors"


def test_a_run_whose_deadline_has_passed_asks_nothing(tmp_path):
    clock = Clock()
    with httpx.Client(transport=httpx.MockTransport(lambda request: pytest.fail("asked"))) as client:
        result = run_official(tmp_path, client, until=clock.now - timedelta(minutes=1), sleep=clock.sleep,
                              now=lambda: clock.now)
    assert {outcome["status"] for outcome in result["sources"].values()} == {"skipped"}


def test_arlweb_takes_at_most_its_share_of_the_window(whole):
    """An arlweb robots.txt that cannot be read is waited for, as the crawl waits, but only for arlweb's share of the
    window, so OSHA still has the rest."""
    unreachable = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(503)))
    official = OfficialRun(whole.root, unreachable, until=whole.clock.now + timedelta(hours=12), sites=SITES,
                           plans=PLANS, sleep=whole.clock.sleep, now=lambda: whole.clock.now)
    started = whole.clock.now
    result = official.source("arlweb")
    official.close("done")
    unreachable.close()
    assert (result["status"], result["stop_reason"]) == ("unavailable", "robots.txt unreachable")
    assert whole.clock.now == started + PLANS["arlweb"].share


def test_a_site_whose_inventory_was_not_backfilled_is_not_asked_until_it_is(tmp_path):
    """The server keeps no originals, so a copy recorded before content hashes cannot be compared with an answer in
    other bytes, and every such page would be reported as changed. A run leaves a site whose inventory (here of the
    first version, as the boot copies it) still holds such copies alone and says why; once the inventory is backfilled
    where its originals are, the site is asked as on any other day."""
    site, clock, archive = site_answers(), Clock(), tmp_path / "archive"
    clock.now = START
    inventory = tmp_path / "state" / "crawl-osha.sqlite"
    inventory.parent.mkdir()
    first_store(inventory, [held(f"{HOST}/hazcom", page("Hazard communication", "/files/c.pdf", "/files/e.pdf"),
                                 HTML["content-type"], archive=archive),
                            held(f"{HOST}/files/c.pdf", pdf_bytes(320), PDF["content-type"], archive=archive)])

    def run_osha() -> dict:
        with httpx.Client(transport=httpx.MockTransport(site.handler)) as client:
            return run_official(tmp_path, client, sources=["osha"], sites=SITES, plans=PLANS, sleep=clock.sleep,
                                now=lambda: clock.now, until=clock.now + timedelta(hours=12),
                                publish=False)["sources"]["osha"]

    skipped = run_osha()
    assert (skipped["status"], skipped["stop_reason"], skipped["requests"]) == ("skipped", "not backfilled", 0)
    assert skipped["details"] == {"copies_without_content_hash": 2} and site.requests == []
    assert skipped["note"].startswith("2 held copies in state/crawl-osha.sqlite have no content hash")
    assert "`ssp official backfill-hashes osha`" in skipped["note"]
    with closing(sqlite3.connect(tmp_path / "state" / "official.sqlite")) as state:
        assert state.execute("select status, stop_reason from sources").fetchall() == [("skipped", "not backfilled")]

    with closing(Store(inventory)) as store:
        assert backfill_hashes(store, archive)["usable"] == 2
    clock.now += timedelta(days=1)
    asked = run_osha()
    assert asked["status"] == "ok" and asked["requests"] > 0 and site.requests
    with closing(sqlite3.connect(inventory)) as db:
        assert db.execute("select count(*) from changes where change = 'changed'").fetchone()[0] == 0


# The command line -----------------------------------------------------------------------------------------------------


def test_ssp_official_run_and_status(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("SAFESTART_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(cli, "setup_logging", lambda path: None)
    calls = []

    def fake_run(root, client, **options):
        calls.append(options)
        return {"run_id": "official-1", "until": iso(options["until"]),
                "sources": {"osha": {"status": "error" if options["budget"] == 1 else "ok"}}}

    monkeypatch.setattr(run, "run_official", fake_run)
    monkeypatch.setattr(cli, "utc_now", lambda: START)
    assert cli.main(["official", "run", "--site", "osha", "--site", "ecfr", "--until", "02:00", "--budget", "40"]) == 0
    assert calls[-1] == {"sources": ["osha", "ecfr"], "until": datetime(2026, 11, 25, 2, 0, tzinfo=UTC), "budget": 40}
    assert json.loads(capsys.readouterr().out)["until"] == "2026-11-25T02:00:00Z"
    assert cli.main(["official", "run", "--budget", "1"]) == 1  # a source failed
    assert calls[-1]["until"] == datetime(2026, 11, 25, 4, 30, tzinfo=UTC) and calls[-1]["sources"] is None
    capsys.readouterr()
    with pytest.raises(SystemExit):
        cli.main(["official", "run", "--site", "elsewhere"])
    with pytest.raises(SystemExit, match="--until tomorrow"):
        cli.main(["official", "run", "--until", "tomorrow"])

    assert cli.main(["official", "status"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["latest_run"] is None and set(report["sources"]) == set(run.SOURCES)
    assert report["sources"]["osha"]["inventory"] == "none on this host" and report["sources"]["osha"]["lock"] == "free"
    assert not list((tmp_path / "state").glob("*.lock"))  # status only reads: no lock file is made
    with SiteLock(tmp_path / "state" / "crawl-osha.lock"):
        assert run.lock_state(tmp_path, "osha") == "held"


def test_status_shows_the_queue_the_cycle_and_the_latest_outcomes(bench):
    baseline(bench)
    state = OfficialState(bench.root / "state" / "official.sqlite")
    state.start_run("official-1", started_at=iso(START), until=iso(START + timedelta(hours=12)), trigger="flow")
    state.start_source("official-1", "osha", host="www.osha.gov", started_at=iso(START))
    state.end_source("official-1", "osha", ended_at=iso(START), status="ok", note="fine", requests=10,
                     stop_reason="done")
    state.close()
    week = START + timedelta(days=8)
    report = run.status(bench.root, week)
    assert report["latest_run"]["run_id"] == "official-1" and report["sources"]["osha"]["last"]["status"] == "ok"
    crawl = run.crawl_status(bench.root / "state" / "crawl-example.sqlite", week)
    assert crawl["queue"] == {"retries": 0, "new": 0, "confirmations": 0, "returning_due": 0, "checks_due": 5}
    assert crawl["cycle"]["held"] == 8 and crawl["cycle"]["checkedLast30Days"] == 8
    assert crawl["changes_last_7_days"] == {} and crawl["latest_crawl_run"]["stop_reason"] == "done"
