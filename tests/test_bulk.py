import hashlib
import io
import zipfile
from functools import partial

import httpx
import pytest
from prefect.testing.utilities import prefect_test_harness

from safestart_pipeline.acquire.bulk import (
    MSHA_LISTING,
    BulkAcquirer,
    BulkFile,
    ecfr_files,
    msha_dataset_links,
    verify,
)
from safestart_pipeline.crawl.locks import SiteLock
from safestart_pipeline.flows import acquire as acquire_flow

PREFIX = "https://arlweb.msha.gov/OpenGovernmentData/DataSets/"
LISTING = b"""<html><body>
<a href="DataSets/Accidents.zip">Accidents</a> <a href="DataSets/Accidents_Definition_File.txt">definition</a>
<a href="/OpenGovernmentData/DataSets/Noise_Samples_Definition_File.txt">noise</a>
<a href="DataSets/107(a)_Orders_Issued_Definition_File.txt">107(a)</a>
<a href="/stats/part50/rptonpart50.pdf">report</a> <a href="ogimsha-instructions.asp">help</a>
<a href="DataSets/Accidents.zip">again</a>
</body></html>"""
SECTIONS = b"""<?xml version="1.0"?>
<ECFR><DIV1 TYPE="TITLE"><DIV5 TYPE="PART"><DIV8 TYPE="SECTION"><HEAD>1.1</HEAD></DIV8>
<DIV8 TYPE="SECTION"><HEAD>1.2</HEAD></DIV8><DIV8 TYPE="APPENDIX"/></DIV5></DIV1></ECFR>"""


def zipped(text: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("Accidents.txt", text)
    return buffer.getvalue()


def test_listing_keeps_data_sets_and_definitions_once_in_page_order():
    assert msha_dataset_links(LISTING) == [
        PREFIX + "Accidents.zip",
        PREFIX + "Accidents_Definition_File.txt",
        PREFIX + "Noise_Samples_Definition_File.txt",
        PREFIX + "107(a)_Orders_Issued_Definition_File.txt",
    ]


def test_verify_each_kind(tmp_path):
    good_zip = tmp_path / "a.zip"
    good_zip.write_bytes(zipped("MINE_ID|DOCUMENT_NO\n1|2\n"))
    ok, note, details = verify(good_zip, "Accidents.zip")
    assert ok and details["members"] == [{"name": "Accidents.txt", "size": 24}]

    broken = tmp_path / "b.zip"
    broken.write_bytes(b"PK not really a zip")
    assert verify(broken, "Violations.zip")[0] is False

    xml = tmp_path / "t.xml"
    xml.write_bytes(SECTIONS)
    ok, note, details = verify(xml, "ECFR-title30.xml")
    assert ok and details["sections"] == 2 and details["root"] == "ECFR"

    truncated = tmp_path / "u.xml"
    truncated.write_bytes(SECTIONS[:-20])
    assert verify(truncated, "ECFR-title29.xml")[0] is False

    text = tmp_path / "d.txt"
    text.write_bytes(b"FIELD|TYPE\nMINE_ID|CHAR(7)")
    assert verify(text, "Mines_Definition_File.txt")[:2] == (True, "2 line(s)")


def test_download_is_conditional_and_keeps_each_version(tmp_path):
    body = {"data": zipped("v1"), "etag": '"v1"'}
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        if str(request.url) == MSHA_LISTING:
            return httpx.Response(200, content=LISTING, headers={"content-type": "text/html"})
        if request.headers.get("if-none-match") == body["etag"]:
            return httpx.Response(304)
        return httpx.Response(200, content=body["data"], headers={"etag": body["etag"]})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    acquirer = BulkAcquirer(tmp_path, client, sleep=lambda seconds: None)
    files = acquirer.list_msha()
    assert len(files) == 4
    item = BulkFile("msha-ogd", PREFIX + "Accidents.zip")

    first = acquirer.download(item)
    assert first["state"] == "fetched" and first["verified"] and first["new_version"]
    assert acquirer.download(item)["state"] == "unchanged"
    assert requests[-1].headers["if-none-match"] == '"v1"'

    body.update(data=zipped("v2 replaced on Friday"), etag='"v2"')
    third = acquirer.download(item)
    assert third["state"] == "fetched" and third["new_version"]
    assert acquirer.store.version_count(item.url) == 2
    assert acquirer.store.summary()["msha-ogd/fetched"]["files"] == 1
    acquirer.store.close()
    client.close()


def test_a_download_that_fails_verification_leaves_the_copy_held_current(tmp_path):
    served = {"data": zipped("v1"), "etag": '"v1"'}
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        if request.headers.get("if-none-match") == served["etag"]:
            return httpx.Response(304)
        return httpx.Response(200, content=served["data"], headers={"etag": served["etag"]})

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        acquirer = BulkAcquirer(tmp_path, client, sleep=lambda seconds: None)
        item = BulkFile("msha-ogd", PREFIX + "Accidents.zip")
        acquirer.store.listed("msha-ogd", [item], page_url=None, page_sha256=None)
        assert acquirer.download(item)["state"] == "fetched"
        held = dict(acquirer.store.get(item.url))

        broken = b"<html>The service is unavailable.</html>"  # served as the zip, under a validator of its own
        served.update(data=broken, etag='"broken"')
        assert acquirer.download(item) == {"url": item.url, "state": "error", "verified": False,
                                           "note": "BadZipFile: File is not a zip file"}
        row = dict(acquirer.store.get(item.url))
        kept = ("sha256", "etag", "last_modified", "content_length", "verified", "verify_note", "details")
        assert {name: row[name] for name in kept} == {name: held[name] for name in kept}
        assert row["state"] == "error" and row["error"].startswith(f"download {hashlib.sha256(broken).hexdigest()}")
        assert acquirer.store.version_count(item.url) == 1

        # Asked with the held copy's validators, a file still broken is an error again, not a 304 for the broken one.
        assert acquirer.download(item)["state"] == "error"
        assert requests[-1].headers["if-none-match"] == '"v1"'
        served.update(data=zipped("v2 repaired"), etag='"v2"')
        assert acquirer.download(item)["new_version"]
        assert acquirer.store.version_count(item.url) == 2
        acquirer.store.close()


@pytest.mark.parametrize(("status", "rules", "reason"), [
    (200, "User-agent: *\nDisallow: /\n", "disallowed by robots.txt"),
    (503, "", "robots.txt unreachable"),
])
def test_the_open_data_page_is_asked_only_where_robots_txt_allows_it(tmp_path, status, rules, reason):
    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        if request.url.path == "/robots.txt":
            return httpx.Response(status, text=rules)
        return httpx.Response(200, content=LISTING)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        acquirer = BulkAcquirer(tmp_path, client, sleep=lambda seconds: None)
        with pytest.raises(RuntimeError, match=f"MSHA open-data page not asked: {reason}"):
            acquirer.list_msha()
        acquirer.store.close()
    assert requests == ["/robots.txt"]


def test_requests_wait_the_hosts_crawl_delay_or_the_longer_retry_after_it_asks_for(tmp_path):
    waits: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text="User-agent: *\nCrawl-delay: 30\n")
        if str(request.url) == MSHA_LISTING:
            return httpx.Response(200, content=LISTING)
        if request.url.path.endswith("/Accidents.zip"):
            return httpx.Response(503, headers={"retry-after": "120"})
        if request.url.path.endswith("/Accidents_Definition_File.txt"):
            return httpx.Response(429, headers={"retry-after": "86400"})  # at most an hour
        return httpx.Response(200, content=b"FIELD|TYPE\nMINE_ID|CHAR(7)\n")

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        acquirer = BulkAcquirer(tmp_path, client, sleep=waits.append)
        for item in acquirer.list_msha():
            acquirer.download(item)
        acquirer.store.close()
    # robots.txt, the open-data page, then the four files it lists.
    assert waits == [30.0, 30.0, 120.0, 3600.0, 30.0, 30.0]


def test_an_unreachable_robots_txt_is_a_failed_check_and_asked_again_by_the_next_request(tmp_path):
    robots = iter([httpx.Response(503), httpx.Response(404)])  # down, then missing (which allows everything)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return next(robots)
        return httpx.Response(200, content=SECTIONS)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        acquirer = BulkAcquirer(tmp_path, client, sleep=lambda seconds: None)
        item = acquirer.list_ecfr()[1]
        assert acquirer.download(item) == {"url": item.url, "state": "error"}
        assert acquirer.store.get(item.url)["error"] == "robots.txt unreachable"
        assert acquirer.download(item)["state"] == "fetched"
        acquirer.store.close()


# The acquire flow -----------------------------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def prefect_api():
    """A temporary Prefect API, so flow runs never reach a real workspace."""
    with prefect_test_harness():
        yield


@pytest.fixture
def hosts(prefect_api, monkeypatch, tmp_path):
    """The acquire flow on a temporary data root against mocked hosts, without pauses or waits between retries; returns
    the robots.txt each host answers (none: 404) and the URLs asked."""
    robots: dict[str, str] = {}
    asked: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        asked.append(str(request.url))
        if request.url.path == "/robots.txt":
            rules = robots.get(request.url.host)
            return httpx.Response(200, text=rules) if rules else httpx.Response(404)
        if str(request.url) == MSHA_LISTING:
            return httpx.Response(200, content=LISTING)
        return httpx.Response(200, content=SECTIONS if request.url.path.endswith(".xml") else zipped("data"))

    monkeypatch.setenv("SAFESTART_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(acquire_flow, "make_client", lambda: httpx.Client(transport=httpx.MockTransport(handler)))
    monkeypatch.setattr(acquire_flow, "BulkAcquirer", partial(BulkAcquirer, sleep=lambda seconds: None))
    monkeypatch.setattr(acquire_flow, "download_file", acquire_flow.download_file.with_options(retry_delay_seconds=0))
    return robots, asked


def test_while_the_arlweb_crawl_runs_the_flow_leaves_its_host_alone_with_a_note(hosts, tmp_path):
    _, asked = hosts
    with SiteLock(tmp_path / "state" / "crawl-arlweb.lock"):
        summary = acquire_flow.acquire_bulk()
    assert sorted(asked) == sorted(["https://www.govinfo.gov/robots.txt", *(item.url for item in ecfr_files())])
    assert summary["not_asked"] == {"msha-ogd": "the arlweb crawl is running, so its host was not asked"}
    assert summary["ecfr-govinfo/fetched"] == {"files": 2, "verified": 2}


def test_a_listing_robots_txt_refuses_leaves_the_other_source_to_download_then_fails_the_run(hosts):
    robots, asked = hosts
    robots["arlweb.msha.gov"] = "User-agent: *\nDisallow: /\n"
    with pytest.raises(RuntimeError, match="msha-ogd listing: MSHA open-data page not asked: disallowed by robots.txt"):
        acquire_flow.acquire_bulk()
    assert [url for url in asked if "arlweb" in url] == ["https://arlweb.msha.gov/robots.txt"]
    assert all(item.url in asked for item in ecfr_files())
