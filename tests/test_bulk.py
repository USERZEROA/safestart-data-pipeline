import io
import zipfile

import httpx

from safestart_pipeline.acquire.bulk import MSHA_LISTING, BulkAcquirer, BulkFile, msha_dataset_links, verify

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
