from __future__ import annotations

import hashlib
import io
import sqlite3
import zipfile
from contextlib import closing
from itertools import pairwise

import httpx
import pytest

import test_crawler
from safestart_pipeline import verify
from safestart_pipeline.acquire.bulk import BulkStore
from safestart_pipeline.crawl.fetcher import ObjectStore
from safestart_pipeline.verify import (
    Polite,
    clean_section,
    object_path,
    parse_amddate,
    read_only,
    verify_bulk,
    verify_crawl,
)
from test_crawler import HOST, setup  # noqa: F401  (the crawl fixture)

SITE = "https://www.example.test"
OTHER = "https://other.example.test"


class Clock:
    """A clock that moves only when the code under test sleeps."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def redirecting_site(clock: Clock, sent: list[tuple[str, str, float]]):
    """A site that asks for 10 s between requests and moves its sitemap (301, as OSHA's does), and another host."""
    routes = {
        f"{SITE}/robots.txt": httpx.Response(200, text="User-agent: *\nCrawl-delay: 10\nDisallow: /private\n"),
        f"{SITE}/sitemap.xml": httpx.Response(301, headers={"location": "/sitemap_index.xml"}),
        f"{SITE}/sitemap_index.xml": httpx.Response(302, headers={"location": f"{OTHER}/sitemap.xml"}),
        f"{OTHER}/robots.txt": httpx.Response(404),
        f"{OTHER}/sitemap.xml": httpx.Response(200, text="<urlset/>"),
        f"{SITE}/moved": httpx.Response(308, headers={"location": "/private/page"}),
        f"{SITE}/loop": httpx.Response(301, headers={"location": "/loop"}),
    }

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append((request.method, str(request.url), clock.now))
        return routes.get(str(request.url), httpx.Response(404))

    return handler


def test_live_checks_follow_redirects_one_paced_hop_at_a_time():
    clock, sent = Clock(), []
    with httpx.Client(transport=httpx.MockTransport(redirecting_site(clock, sent))) as client:
        live = Polite(client, sleep=clock.sleep, clock=clock)
        answer = live.request("GET", f"{SITE}/sitemap.xml")
        assert (answer.status_code, str(answer.url), answer.text) == (200, f"{OTHER}/sitemap.xml", "<urlset/>")
        assert [(method, url) for method, url, _ in sent] == [
            ("GET", f"{SITE}/robots.txt"), ("GET", f"{SITE}/sitemap.xml"), ("GET", f"{SITE}/sitemap_index.xml"),
            ("GET", f"{OTHER}/robots.txt"), ("GET", f"{OTHER}/sitemap.xml")]
        times = {url: moment for _, url, moment in sent}
        assert times[f"{SITE}/sitemap.xml"] - times[f"{SITE}/robots.txt"] >= 10  # the host's Crawl-delay ...
        assert times[f"{SITE}/sitemap_index.xml"] - times[f"{SITE}/sitemap.xml"] >= 10  # ... on the redirect's hop too
        assert times[f"{OTHER}/sitemap.xml"] - times[f"{OTHER}/robots.txt"] >= 1  # another host: the least delay
        assert live.request("HEAD", f"{SITE}/sitemap.xml").request.method == "HEAD"  # a HEAD stays a HEAD
        with pytest.raises(PermissionError, match="/private/page"):
            live.request("GET", f"{SITE}/moved")  # robots.txt rules each hop
        with pytest.raises(httpx.TooManyRedirects):
            live.request("GET", f"{SITE}/loop")
    assert all(later[2] - earlier[2] >= 10 for earlier, later in pairwise(sent)
               if earlier[1].startswith(SITE) and later[1].startswith(SITE))


def test_a_wal_database_is_read_on_a_read_only_mount(tmp_path, monkeypatch):
    path = tmp_path / "state" / "bulk.sqlite"
    path.parent.mkdir()
    with closing(sqlite3.connect(path)) as db:  # closed by its last connection: no -wal or -shm file is left
        db.execute("pragma journal_mode=wal")
        db.execute("create table files (name text)")
        db.execute("insert into files values ('Mines.zip')")
        db.commit()
    sidecars = [path.with_name(f"bulk.sqlite-{suffix}") for suffix in ("wal", "shm")]
    assert not any(sidecar.exists() for sidecar in sidecars)
    monkeypatch.setattr(verify.os, "access", lambda folder, mode: False)  # as the containers' read-only data root
    with closing(read_only(path)) as db:
        assert [tuple(row) for row in db.execute("select name from files")] == [("Mines.zip",)]
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            db.execute("insert into files values ('New.zip')")
    assert not any(sidecar.exists() for sidecar in sidecars)  # read as immutable, nothing written beside it


def test_a_finished_crawl_is_complete_and_damaged_files_are_found(setup, tmp_path):  # noqa: F811
    crawler, fake, clock = setup
    crawler.seed_from_sitemaps()
    crawler.crawl()
    report = verify_crawl(crawler.site, tmp_path, rehash=True)
    assert report["verdict"] == {"complete": True, "reasons": []}
    assert report["waiting"] == 0 and report["sitemap"]["urls"] == 6
    assert {reason: item["count"] for reason, item in report["skipped"].items()} == {
        "robots": 2, "asset": 1, "size_cap": 1}
    assert report["skipped"]["size_cap"]["examples"] == [f"{HOST}/big.pdf  (0.0 MB)"]
    assert report["failed"]["not_found"]["count"] == 1 and report["redirects"] == {"target fetched": 1}
    assert report["outside_links"]["by_host"] == {"other.test": 1}
    assert report["objects"]["missing"] == report["objects"]["wrong_hash"] == 0

    page = crawler.store.get(f"{HOST}/b")
    object_path(tmp_path, "example", page["sha256"]).write_bytes(b"tampered")
    object_path(tmp_path, "example", crawler.store.get(f"{HOST}/c")["sha256"]).unlink()
    damaged = verify_crawl(crawler.site, tmp_path, rehash=True)
    assert not damaged["verdict"]["complete"]
    objects = damaged["objects"]
    assert (objects["missing"], objects["wrong_size"], objects["wrong_hash"]) == (1, 1, 1)


def test_the_live_check_finds_sitemap_changes_politely(setup, tmp_path, monkeypatch):  # noqa: F811
    crawler, fake, clock = setup
    crawler.seed_from_sitemaps()
    crawler.crawl()
    today = test_crawler.PAGES.replace("<url><loc>https://www.example.test/gone</loc></url>",
                                      "<url><loc>https://www.example.test/new</loc></url>")
    today = today.replace("<lastmod>2026-11-01</lastmod>", "<lastmod>2026-12-01</lastmod>")
    monkeypatch.setattr(test_crawler, "PAGES", today)
    waits: list[float] = []
    client = httpx.Client(transport=httpx.MockTransport(fake.handler))
    report = verify_crawl(crawler.site, tmp_path, live=Polite(client, sleep=waits.append))
    live = report["live_sitemap"]
    assert (live["added"], live["dropped"], live["changed_after_fetch"]) == (1, 1, 1)
    assert live["added_examples"] == [f"{HOST}/new"] and live["changed_examples"] == [f"{HOST}/a"]
    assert not report["verdict"]["complete"]
    assert waits and min(waits) > 9.0  # robots.txt asks for 10 s between requests
    client.close()


ECFR_XML = """<?xml version="1.0" encoding="UTF-8"?>
<DLPSTEXTCLASS><HEADER><AMDDATE>Nov. 9, 2026</AMDDATE></HEADER><TEXT><BODY><ECFRBRWS>
<DIV5 N="1" TYPE="PART"><DIV8 N="§ 1.1" TYPE="SECTION"><HEAD>§ 1.1 Scope.</HEAD></DIV8>
<DIV8 N="§§ 1.2—1.3" TYPE="SECTION"><HEAD>§§ 1.2-1.3 [Reserved]</HEAD></DIV8></DIV5>
</ECFRBRWS></BODY></TEXT></DLPSTEXTCLASS>"""
LISTING = '<html><body><a href="DataSets/Mines.zip">Mines</a> <a href="DataSets/New.zip">New</a></body></html>'
GOVINFO_DATE = "Thu, 12 Nov 2026 22:17:51 GMT"


def ecfr_handler(request: httpx.Request) -> httpx.Response:
    host, path = request.url.host, request.url.path
    if path == "/robots.txt":
        return httpx.Response(404)
    if host == "www.ecfr.gov" and path.endswith("/titles.json"):
        return httpx.Response(200, json={"titles": [
            {"number": 29, "latest_amended_on": "2026-11-10", "up_to_date_as_of": "2026-11-16"},
            {"number": 30, "latest_amended_on": "2026-11-12", "up_to_date_as_of": "2026-11-16"}]})
    if host == "www.ecfr.gov" and "/structure/" in path:
        return httpx.Response(200, json={"type": "title", "children": [{"type": "part", "children": [
            {"type": "section", "identifier": "1.1"}, {"type": "section", "identifier": "1.2 - 1.3", "reserved": True},
            {"type": "section", "identifier": "1.4"}]}]})
    if host == "www.govinfo.gov":
        return httpx.Response(200, headers={"last-modified": GOVINFO_DATE})
    if host == "arlweb.msha.gov" and path.endswith("OGIMSHA.asp"):
        return httpx.Response(200, text=LISTING, headers={"content-type": "text/html"})
    if host == "arlweb.msha.gov" and path.endswith("Mines.zip"):
        return httpx.Response(200, headers={"content-length": str(len(MINES)),
                                            "last-modified": "Wed, 18 Nov 2026 13:16:24 GMT"})
    return httpx.Response(404)


def zipped(name: str, text: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(name, text)
    return buffer.getvalue()


MINES = zipped("Mines.txt", "MINE_ID|NAME\n1|Demo\n")


def held(tmp_path, url: str, content: bytes, name: str, source: str, modified: str) -> None:
    objects, store = ObjectStore(tmp_path / "raw" / "bulk" / "objects"), BulkStore(tmp_path / "state" / "bulk.sqlite")
    temp = objects.temp_path()
    temp.write_bytes(content)
    digest = hashlib.sha256(content).hexdigest()
    objects.commit(temp, digest)
    store.db.execute("insert into files (url, source, name, listed_at, state, content_length, sha256, last_modified,"
                     " verified) values (?, ?, ?, '2026-11-17T00:00:00Z', 'fetched', ?, ?, ?, 1)",
                     (url, source, name, len(content), digest, modified))
    store.db.commit()
    store.close()


def test_bulk_files_are_checked_against_todays_sources(tmp_path):
    held(tmp_path, "https://www.govinfo.gov/bulkdata/ECFR/title-29/ECFR-title29.xml", ECFR_XML.encode(),
         "ECFR-title29.xml", "ecfr-govinfo", GOVINFO_DATE)
    held(tmp_path, "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Mines.zip", MINES, "Mines.zip", "msha-ogd",
         "Tue, 17 Nov 2026 13:54:23 GMT")
    client = httpx.Client(transport=httpx.MockTransport(ecfr_handler))
    report = verify_bulk(tmp_path, live=Polite(client, sleep=lambda seconds: None))
    assert report["problems"] == []
    listing = report["msha_listing_today"]
    assert listing["new"] == ["https://arlweb.msha.gov/OpenGovernmentData/DataSets/New.zip"]
    assert [item["name"] for item in listing["rewritten_same_size"]] == ["Mines.zip"] and listing["changed"] == []
    title29, title30 = report["ecfr_today"]
    assert (title29["official_sections"], title29["our_sections"], title29["missing_sections"]) == (3, 2, 1)
    assert title29["missing_examples"] == ["1.4"] and title29["behind"] and not title29["source_newer"]
    assert title30 == {"title": 30, "error": "not downloaded"}
    reasons = " ".join(report["verdict"]["reasons"])
    assert not report["verdict"]["complete"] and "New.zip" not in reasons and "1 files listed today" in reasons
    assert "title 29: 1 official sections" in reasons and "amended to 2026-11-09" in reasons
    client.close()


def answering(changes):
    """ecfr_handler, except for the requests named: (method, URL ending) -> a function of the request that answers or
    raises."""
    def handler(request: httpx.Request) -> httpx.Response:
        for (method, ending), outcome in changes.items():
            if request.method == method and str(request.url).endswith(ending):
                return outcome(request)
        return ecfr_handler(request)

    return handler


def unavailable(request: httpx.Request) -> httpx.Response:
    return httpx.Response(503, text="<html>Service Unavailable</html>", headers={"content-type": "text/html"})


def timed_out(request: httpx.Request) -> httpx.Response:
    raise httpx.ReadTimeout("timed out", request=request)


def hold_ecfr_and_mines(tmp_path):
    held(tmp_path, "https://www.govinfo.gov/bulkdata/ECFR/title-29/ECFR-title29.xml", ECFR_XML.encode(),
         "ECFR-title29.xml", "ecfr-govinfo", GOVINFO_DATE)
    held(tmp_path, "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Mines.zip", MINES, "Mines.zip", "msha-ogd",
         "Tue, 17 Nov 2026 13:54:23 GMT")


def test_a_failed_live_request_is_reported_and_the_rest_of_the_report_is_kept(tmp_path):
    hold_ecfr_and_mines(tmp_path)
    changes = {("GET", "/titles.json"): unavailable, ("HEAD", "/Mines.zip"): timed_out}
    with httpx.Client(transport=httpx.MockTransport(answering(changes))) as client:
        report = verify_bulk(tmp_path, live=Polite(client, sleep=lambda seconds: None))
    assert report["problems"] == [] and all(entry["hash_ok"] for entry in report["files"])  # the rehash is kept
    listing = report["msha_listing_today"]
    assert listing["errors"] == ["Mines.zip: timed out"] and listing["gone"] == [] and listing["listed_today"] == 2
    title29 = report["ecfr_today"][0]
    assert title29["error"].startswith("eCFR API: HTTP 503") and "official_sections" not in title29
    assert (title29["our_sections"], title29["source_newer"]) == (2, False)  # govinfo still answered
    reasons = report["verdict"]["reasons"]
    assert "1 held files not checked today: Mines.zip: timed out" in reasons
    assert any(reason.startswith("title 29: eCFR API: HTTP 503") for reason in reasons)


def test_a_listing_page_that_cannot_be_read_is_an_error_not_an_empty_listing(tmp_path):
    hold_ecfr_and_mines(tmp_path)
    with httpx.Client(transport=httpx.MockTransport(answering({("GET", "/OGIMSHA.asp"): unavailable}))) as client:
        report = verify_bulk(tmp_path, live=Polite(client, sleep=lambda seconds: None))
    error = "HTTP 503 from https://arlweb.msha.gov/OpenGovernmentData/OGIMSHA.asp"
    assert report["msha_listing_today"] == {"error": error}  # not an empty listing with every file delisted
    assert f"MSHA's open-data page not compared today: {error}" in report["verdict"]["reasons"]
    assert report["ecfr_today"][0]["missing_sections"] == 1  # the eCFR comparison still ran


def test_a_damaged_bulk_file_is_a_problem(tmp_path):
    held(tmp_path, "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Mines.zip", MINES, "Mines.zip", "msha-ogd", "x")
    store = BulkStore(tmp_path / "state" / "bulk.sqlite")
    digest = store.db.execute("select sha256 from files").fetchone()[0]
    store.close()
    path = object_path(tmp_path, "bulk", digest)
    path.write_bytes(path.read_bytes()[:-1] + b"!")
    report = verify_bulk(tmp_path)
    assert report["problems"] == ["Mines.zip"] and report["files"][0]["hash_ok"] is False


@pytest.mark.parametrize(("raw", "clean"), [("§ 56.14105", "56.14105"),
                                            ("§§ 2520.104-2—2520.104-3", "2520.104-2 - 2520.104-3")])
def test_section_numbers_compare_across_sources(raw, clean):
    assert clean_section(raw) == clean


def test_amendment_dates_read_as_iso():
    assert parse_amddate("Nov. 9, 2026") == "2026-11-09" and parse_amddate("May 12, 2026") == "2026-05-12"
    assert parse_amddate("?") is None
