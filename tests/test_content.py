"""crawl/content.py: what a body is by its first bytes, which bodies are refused and why, and the change hash of a page
(stable across the values Drupal changes on every request, changed by any edit of the text)."""

from __future__ import annotations

import hashlib
import io
import struct
import zipfile

import pytest

from safestart_pipeline.crawl import content
from safestart_pipeline.crawl.content import examine, main_text, ole_streams, page_hash

SITE = "https://www.example.test"
FREE, END, FAT_SECTOR, NO_STREAM = 0xFFFFFFFF, 0xFFFFFFFE, 0xFFFFFFFD, 0xFFFFFFFF


def pdf(size: int = 2000, *, end: bool = True) -> bytes:
    head, tail = b"%PDF-1.6\n%\xe2\xe3\xcf\xd3\n", b"\nstartxref\n116\n%%EOF\n" if end else b"\nstream\nx\x9c"
    return head + b"0" * (size - len(head) - len(tail)) + tail


def ooxml(folder: str, *, damaged: bool = False, content_types: bool = True) -> bytes:
    """An Office Open XML package with one part in `folder`; `damaged` spoils a byte of the stored part."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_STORED) as archive:
        if content_types:
            archive.writestr("[Content_Types].xml", "<Types/>")
        archive.writestr("_rels/.rels", "<Relationships/>")
        archive.writestr(f"{folder}/main.xml", "<part>" + "text " * 50 + "</part>")
    data = bytearray(buffer.getvalue())
    if damaged:
        spot = data.index(b"text text")
        data[spot] ^= 0xFF
    return bytes(data)


def ole_entry(name: str, kind: int, *, left: int = NO_STREAM, right: int = NO_STREAM, child: int = NO_STREAM) -> bytes:
    encoded = name.encode("utf-16-le") + b"\0\0"
    entry = encoded.ljust(64, b"\0") + struct.pack("<HBB", len(encoded), kind, 1)
    entry += struct.pack("<III", left, right, child) + b"\0" * 36 + struct.pack("<IQ", END, 0)
    assert len(entry) == 128
    return entry


def ole(*names: str, nested: str | None = None, directory: int = 1) -> bytes:
    """A version 3 compound file (512-byte sectors): the header, one FAT sector and one directory sector holding the
    root, a stream per name at the top level (as a chain of right siblings) and, with `nested`, a storage holding a
    stream of that name. `directory` names the first directory sector (1 is the right one)."""
    entries = [ole_entry("Root Entry", 5, child=1)]
    top = [*[(name, 2) for name in names], *([("ObjectPool", 1)] if nested else [])]
    for index, (name, kind) in enumerate(top, start=1):
        right = index + 1 if index < len(top) else NO_STREAM
        entries.append(ole_entry(name, kind, right=right, child=len(top) + 1 if kind == 1 else NO_STREAM))
    if nested:
        entries.append(ole_entry(nested, 2))
    assert len(entries) <= 4  # one directory sector
    header = bytes.fromhex("d0cf11e0a1b11ae1") + b"\0" * 16 + struct.pack("<HHHHH", 0x3E, 3, 0xFFFE, 9, 6)
    header += b"\0" * 6 + struct.pack("<IIIIIIIII", 0, 1, directory, 0, 4096, END, 0, END, 0)
    header += struct.pack("<109I", 0, *[FREE] * 108)
    fat = struct.pack("<128I", FAT_SECTOR, END, *[FREE] * 126)
    return header + fat + b"".join(entries).ljust(512, b"\0")


def verdict(tmp_path, body: bytes, url: str, content_type: str | None = None):
    path = tmp_path / "body"
    path.write_bytes(body)
    return examine(path, url=f"{SITE}{url}", content_type=content_type, sha256=hashlib.sha256(body).hexdigest())


def test_files_are_told_apart_by_their_bytes_and_hashed_by_their_sha256(tmp_path):
    cases = [
        (pdf(), "/report.pdf", "application/pdf", "pdf"),
        (ooxml("word"), "/form.docx", None, "docx"),
        (ooxml("xl"), "/data.xlsx", None, "xlsx"),
        (ooxml("ppt"), "/talk.pptx", None, "pptx"),
        (ole("WordDocument", "\x05SummaryInformation"), "/old.doc", "application/msword", "doc"),
        (ole("Workbook"), "/old.xls", None, "xls"),
        (ole("Book"), "/older.xls", None, "xls"),
        (ole("Current User", "PowerPoint Document"), "/old.ppt", None, "ppt"),
        (ole("VisioDocument"), "/plan.vsd", None, "other"),
        (ooxml("data", content_types=False), "/bundle.zip", "application/zip", "zip"),
        (b'<?xml version="1.0" encoding="utf-8"?>\n<rss><channel><title>News</title></channel></rss>', "/news.xml",
         "application/rss+xml", "xml"),
        (b"Date,Company\n2026-11-01,Acme\n", "/fatalities.csv", "application/octet-stream", "text"),
        (b"Date,Company\n2026-11-01,Acme\n", "/fy15.csv", "application/vnd.ms-excel", "text"),
        (b"\xff\xfeN\0o\0t\0e\0s\0", "/notes.txt", "text/plain", "text"),
        (b"\x89PNG\r\n\x1a\n" + b"\0" * 40, "/sign.png", "image/png", "image"),
        (b"\xc5\xd0\xd3\xc6" + b"\0" * 40 + b"%!PS-Adobe-3.0 EPSF-3.0", "/logo.eps", "application/postscript", "image"),
        (b"RIFF\x24\0\0\0WAVEfmt " + b"\0" * 20, "/alarm.wav", "audio/wav", "media"),
        (b"\x30\x26\xb2\x75\x8e\x66\xcf\x11" + b"\0" * 40, "/briefing.wmv", "video/x-ms-wmv", "media"),
        (b"{\\rtf1\\ansi Mine safety}", "/rules.doc", "application/msword", "other"),  # RTF at a .doc address: kept
        (ooxml("word"), "/form.doc", "application/msword", "docx"),  # a docx at a .doc address: kept as a docx
        (b"\x00\x01\x02\x03" * 20, "/download", None, "other"),
    ]
    for body, url, declared, expected in cases:
        result = verdict(tmp_path, body, url, declared)
        assert (result.detected_type, result.reason) == (expected, None), url
        assert result.content_sha256 == hashlib.sha256(body).hexdigest(), url


def test_html_where_a_file_belongs_is_a_soft_error(tmp_path):
    # OSHA's own case: a page served with status 200 and Content-Type application/pdf at a PDF address.
    page = b"\r\n<!DOCTYPE html>\n<html lang=\"en-us\"><head><title>OSHA</title></head><body>Not here</body></html>"
    for url, declared in (("/laws-regs/federalregister/1997-02-07.pdf", "application/pdf"),
                          ("/news.xml", "text/html"), ("/data.csv", "text/csv"),
                          ("/download", "application/pdf"), ("/download", "application/octet-stream")):
        result = verdict(tmp_path, page, url, declared)
        assert (result.detected_type, result.reason, result.content_sha256) == ("html", "soft_error", None), url
    for url, declared in (("/topic", "text/html"), ("/topic.htm", "text/html"), ("/news.asp", None),
                          ("/topic", "text/plain"), ("/laws-regs/standardnumber/1910/1910.1200", "text/html")):
        result = verdict(tmp_path, page, url, declared)
        assert (result.detected_type, result.reason) == ("html", None), url
    xhtml = b'<?xml version="1.0"?>\n<html xmlns="http://www.w3.org/1999/xhtml"><body><p>Hi</p></body></html>'
    assert verdict(tmp_path, xhtml, "/page", "application/xhtml+xml").detected_type == "html"
    assert verdict(tmp_path, xhtml, "/page.xml", "application/xml").reason == "soft_error"


def test_bodies_that_fail_their_own_type_check_are_unreadable(tmp_path):
    cases = [
        (pdf(end=False), "/cut.pdf", "a truncated PDF"),
        (pdf(4000, end=False)[:2000] + b"%%EOF" + b"x" * 1500, "/linearized.pdf", "a truncated PDF"),
        (ooxml("word", damaged=True), "/form.docx", "fails its CRC check"),
        (b"PK\x03\x04" + b"\0" * 60, "/broken.zip", "a damaged zip"),
        (ole("WordDocument", directory=7), "/old.doc", "directory cannot be read"),
        (b'<?xml version="1.0"?>\n<rss><channel>', "/news.xml", "XML that does not parse"),
    ]
    for body, url, note in cases:
        result = verdict(tmp_path, body, url)
        assert (result.reason, result.content_sha256) == ("unreadable", None), url
        assert note in result.note, url


def test_text_or_a_picture_where_a_document_belongs_is_a_type_mismatch(tmp_path):
    cases = [
        (b"The requested file could not be found.", "/report.pdf", "text"),
        (b'<?xml version="1.0"?><Error><Code>AccessDenied</Code></Error>', "/report.pdf", "xml"),
        (b"GIF89a" + b"\0" * 30, "/form.xlsx", "image"),
        (b"Service unavailable", "/feed.xml", "text"),
        (b"Not found", "/sign.png", "text"),
    ]
    for body, url, detected in cases:
        result = verdict(tmp_path, body, url, "application/pdf")
        assert (result.detected_type, result.reason, result.content_sha256) == (detected, "type_mismatch", None), url


def test_empty_bodies_and_pages_without_anything_are_refused(tmp_path):
    for body in (b"", b"  \r\n\t ", b"<html><head></head><body>\n</body></html>"):
        assert verdict(tmp_path, body, "/page", "text/html").reason == "empty"
    # A frameset, a page that only embeds a file or only links on: nothing to read, but the links are needed.
    for body in (b'<html><frameset><frame src="/menu.htm"></frameset></html>',
                 b'<html><body><iframe src="/report.pdf"></iframe></body></html>',
                 b'<html><body><a href="/report.pdf"></a></body></html>'):
        result = verdict(tmp_path, body, "/page", "text/html")
        assert (result.detected_type, result.reason, result.content_sha256) == ("html", None, page_hash("")), body


def test_the_top_level_streams_of_a_compound_file_decide_its_type(tmp_path):
    path = tmp_path / "embedded.doc"
    path.write_bytes(ole("WordDocument", nested="Workbook"))  # a Word document holding an embedded workbook
    with path.open("rb") as handle:
        assert ole_streams(handle, path.stat().st_size) == {"WordDocument", "ObjectPool"}
    assert verdict(tmp_path, path.read_bytes(), "/embedded.doc").detected_type == "doc"


def drupal_page(main: str, *, view: str = "c0ffee", build: str = "form-Ab1", token: str = "tok1",
                version: str = "1794618558", menu: str = "About OSHA", sidebar: str = "News", base: str = "") -> bytes:
    """An OSHA-like page: the values that change with every request (Drupal's view DOM id, form build id and token,
    cache-busting query strings), a header, a footer and a side bar around the main content."""
    return f"""<!DOCTYPE html>
<html lang="en"><head><title>Hazard Communication | OSHA</title>
<link rel="stylesheet" href="{base}/core/styles.css?v={version}">
<script src="{base}/sites/default/bundle.js?v={version}"></script>
<script type="application/json" data-drupal-selector="drupal-settings-json">{{"v":"{view}","t":"{token}"}}</script>
</head><body class="path-node">
<header id="navbar" role="banner"><nav><a href="{base}/aboutosha">{menu}</a></nav>
<form action="/search"><input type="hidden" name="form_build_id" value="{build}">
<input type="hidden" name="form_token" value="{token}"><input name="q" placeholder="Search"></form></header>
<div role="main" class="main-container js-quickedit-main-content"><div id="main-content"></div>
<div class="view js-view-dom-id-{view}"><article><h1>Hazard Communication</h1>{main}
<img src="{base}/sites/default/files/styles/large/sign.png?itok={token}" alt="">
<form class="views-exposed-form"><input type="hidden" name="form_build_id" value="{build}"></form>
<!-- cache tag {version} --></article></div></div>
<aside role="complementary">{sidebar}</aside>
<footer role="contentinfo">Page built at {version}</footer>
</body></html>""".encode()


MAIN = "<p>Employers must label containers.</p><table><tr><td>29 CFR 1910.1200</td><td>Effective 2026</td></tr></table>"


def test_the_change_hash_ignores_what_changes_with_every_request(tmp_path):
    first = verdict(tmp_path, drupal_page(MAIN), "/hazcom", "text/html")
    assert (first.detected_type, first.reason) == ("html", None)
    noisy = [
        drupal_page(MAIN, view="d00d42", build="form-Zz9", token="tok2", version="1794618569"),
        drupal_page(MAIN, base="/index%2Ephp"),  # links written under an alias prefix
        drupal_page(MAIN, menu="About Us", sidebar="Other news today"),  # outside the main content
        drupal_page(MAIN.replace("<p>", "\n    <p>\n  ").replace(" must ", "\n  must" + chr(0xA0))),  # white space
    ]
    for body in noisy:
        assert verdict(tmp_path, body, "/hazcom", "text/html").content_sha256 == first.content_sha256
    assert first.content_sha256 != hashlib.sha256(drupal_page(MAIN)).hexdigest()


def test_any_edit_of_the_main_text_changes_the_hash(tmp_path):
    first = verdict(tmp_path, drupal_page(MAIN), "/hazcom", "text/html").content_sha256
    edits = [MAIN.replace("label", "mark"), MAIN.replace("1910.1200", "1910.1201"), MAIN.replace("2026", "2027"),
             MAIN + "<p>New: training records.</p>", MAIN.replace("<td>Effective 2026</td>", "")]
    hashes = {verdict(tmp_path, drupal_page(edit), "/hazcom", "text/html").content_sha256 for edit in edits}
    assert first not in hashes and len(hashes) == len(edits)


def test_the_main_content_is_found_the_way_pages_mark_it():
    assert main_text(b"<html><body><nav>Menu</nav><main>A <b>b</b></main><div role=main>C</div></body></html>") == "Ab"
    assert main_text(b"<html><body><nav>Menu</nav><div role='main'>C d</div><article>E</article></body></html>") == "Cd"
    assert main_text(b"<html><body><div>Menu</div><article>E <i>f</i></article></body></html>") == "Ef"
    page = (b"<html><body><header>Top</header><nav>Menu</nav><table><tr><td>Mine</td><td>safety</td></tr></table>"
            b"<div role=navigation>Links</div><aside>Side</aside><footer>Bottom</footer></body></html>")
    assert main_text(page) == "Minesafety"
    assert main_text(b"<html><body><main>a<script>var token = 1;</script>b<style>p {}</style>c</main></body></html>") \
        == "abc"


def test_a_page_too_large_to_parse_is_hashed_whole(tmp_path, monkeypatch):
    monkeypatch.setattr(content, "MAX_PAGE_BYTES", 100)
    body = drupal_page(MAIN)
    result = verdict(tmp_path, body, "/hazcom", "text/html")
    assert (result.detected_type, result.content_sha256) == ("html", hashlib.sha256(body).hexdigest())
    assert "hashed whole" in result.note


@pytest.mark.parametrize("head", [b"%PDF-", b"PK\x03\x04", bytes.fromhex("d0cf11e0a1b11ae1"), b"<html>", b"text"])
def test_a_short_or_damaged_body_never_raises(tmp_path, head):
    for tail in (b"", b"\0", b"\xff" * 10):
        result = verdict(tmp_path, head + tail, "/file")
        assert result.reason is None or result.reason in content.REASONS


def test_a_failure_no_check_foresaw_is_a_refusal_not_a_crash(tmp_path, monkeypatch):
    # A zip whose LZMA-compressed member is corrupt: lzma's own error, not one of zipfile's.
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_LZMA) as archive:
        archive.writestr("data.csv", "Date,Company\n" * 2000)
    data = bytearray(buffer.getvalue())
    start = data.index(b"data.csv") + 40
    data[start:start + 30] = bytes(byte ^ 0x5A for byte in data[start:start + 30])
    result = verdict(tmp_path, bytes(data), "/data.zip", "application/zip")
    assert (result.detected_type, result.reason) == ("zip", "unreadable") and "a damaged zip" in result.note

    def surprise(*args):
        raise KeyError("a check that went wrong")

    monkeypatch.setattr(content, "zip_type", surprise)
    result = verdict(tmp_path, ooxml("word"), "/form.docx")
    assert (result.reason, result.content_sha256) == ("unreadable", None) and "KeyError" in result.note

    def unreadable_disk(*args):
        raise PermissionError("the object is locked")

    monkeypatch.setattr(content, "zip_type", unreadable_disk)
    with pytest.raises(PermissionError):  # the bytes could not be read: no verdict on them
        verdict(tmp_path, ooxml("word"), "/form.docx")


def test_xml_is_read_element_by_element(tmp_path, monkeypatch):
    real, held = content.etree.iterparse, []

    def watched(*args, **kwargs):  # how many elements the document element holds after each one is read
        root = None
        for event, element in real(*args, **kwargs):
            root = element if root is None else root
            yield event, element
            held.append(len(root))

    monkeypatch.setattr(content.etree, "iterparse", watched)
    body = (b'<?xml version="1.0"?>\n<!-- exported --><?xml-stylesheet href="s.xsl"?><records>'
            + b"<r><a>1</a><b>two</b><!-- note --></r>" * 20_000 + b"</records><!-- end -->")
    assert verdict(tmp_path, body, "/records.xml", "application/xml").detected_type == "xml"
    # lxml parses a read-ahead block at a time (under a thousand of these records); without the records read being
    # dropped the tree would hold all 20,000 by the end.
    assert max(held) < 2_000 and held[-1] == 0
    assert verdict(tmp_path, body[:-30], "/records.xml", "application/xml").reason == "unreadable"
