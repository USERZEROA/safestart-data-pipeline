from __future__ import annotations

import hashlib
import io
import zipfile

import httpx
import pytest

import test_crawler
from safestart_pipeline.acquire.bulk import BulkStore
from safestart_pipeline.crawl.fetcher import ObjectStore
from safestart_pipeline.verify import Polite, clean_section, object_path, parse_amddate, verify_bulk, verify_crawl
from test_crawler import HOST, setup  # noqa: F401  (the crawl fixture)


def test_a_finished_crawl_is_complete_and_damaged_files_are_found(setup, tmp_path):  # noqa: F811
    crawler, fake, clock = setup
    crawler.seed_from_sitemaps()
    crawler.crawl()
    report = verify_crawl(crawler.site, tmp_path, rehash=True)
    assert report["verdict"] == {"complete": True, "reasons": []}
    assert report["waiting"] == 0 and report["sitemap"]["urls"] == 6
    assert {reason: item["count"] for reason, item in report["skipped"].items()} == {"robots": 2, "asset": 1, "size_cap": 1}
    assert report["skipped"]["size_cap"]["examples"] == [f"{HOST}/big.pdf  (0.0 MB)"]
    assert report["failed"]["not_found"]["count"] == 1 and report["redirects"] == {"target fetched": 1}
    assert report["outside_links"]["by_host"] == {"other.test": 1}
    assert report["objects"]["missing"] == report["objects"]["wrong_hash"] == 0

    page = crawler.store.get(f"{HOST}/b")
    object_path(tmp_path, "example", page["sha256"]).write_bytes(b"tampered")
    object_path(tmp_path, "example", crawler.store.get(f"{HOST}/c")["sha256"]).unlink()
    damaged = verify_crawl(crawler.site, tmp_path, rehash=True)
    assert not damaged["verdict"]["complete"]
    assert (damaged["objects"]["missing"], damaged["objects"]["wrong_size"], damaged["objects"]["wrong_hash"]) == (1, 1, 1)


def test_the_live_check_finds_sitemap_changes_politely(setup, tmp_path, monkeypatch):  # noqa: F811
    crawler, fake, clock = setup
    crawler.seed_from_sitemaps()
    crawler.crawl()
    today = test_crawler.PAGES.replace("<url><loc>https://www.example.test/gone</loc></url>", "<url><loc>https://www.example.test/new</loc></url>")
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
        return httpx.Response(200, json={"titles": [{"number": 29, "latest_amended_on": "2026-11-10", "up_to_date_as_of": "2026-11-16"},
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
        return httpx.Response(200, headers={"content-length": str(len(MINES)), "last-modified": "Wed, 18 Nov 2026 13:16:24 GMT"})
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
    store.db.execute("insert into files (url, source, name, listed_at, state, content_length, sha256, last_modified, verified)"
                     " values (?, ?, ?, '2026-11-17T00:00:00Z', 'fetched', ?, ?, ?, 1)", (url, source, name, len(content), digest, modified))
    store.db.commit()
    store.close()


def test_bulk_files_are_checked_against_todays_sources(tmp_path):
    held(tmp_path, "https://www.govinfo.gov/bulkdata/ECFR/title-29/ECFR-title29.xml", ECFR_XML.encode(), "ECFR-title29.xml", "ecfr-govinfo", GOVINFO_DATE)
    held(tmp_path, "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Mines.zip", MINES, "Mines.zip", "msha-ogd", "Tue, 17 Nov 2026 13:54:23 GMT")
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


def test_a_damaged_bulk_file_is_a_problem(tmp_path):
    held(tmp_path, "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Mines.zip", MINES, "Mines.zip", "msha-ogd", "x")
    store = BulkStore(tmp_path / "state" / "bulk.sqlite")
    digest = store.db.execute("select sha256 from files").fetchone()[0]
    store.close()
    path = object_path(tmp_path, "bulk", digest)
    path.write_bytes(path.read_bytes()[:-1] + b"!")
    report = verify_bulk(tmp_path)
    assert report["problems"] == ["Mines.zip"] and report["files"][0]["hash_ok"] is False


@pytest.mark.parametrize(("raw", "clean"), [("§ 56.14105", "56.14105"), ("§§ 2520.104-2—2520.104-3", "2520.104-2 - 2520.104-3")])
def test_section_numbers_compare_across_sources(raw, clean):
    assert clean_section(raw) == clean


def test_amendment_dates_read_as_iso():
    assert parse_amddate("Nov. 9, 2026") == "2026-11-09" and parse_amddate("May 12, 2026") == "2026-05-12" and parse_amddate("?") is None
