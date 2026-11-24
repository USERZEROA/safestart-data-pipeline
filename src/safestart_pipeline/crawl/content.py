"""What a downloaded body is, whether it is usable, and the hash that says whether it changed.

`examine` looks at a stored body's first bytes, and at as much more of it as its type needs, and returns a Verdict:
the type the bytes show, the change hash, and the reason the body is refused, if it is.

Types, from the bytes alone:
- pdf: "%PDF-" at the start, and "%%EOF" within the last 1,024 bytes (a truncated PDF has none there);
- docx, xlsx, pptx: a zip with [Content_Types].xml whose members pass zipfile's CRC test (testzip), told apart by the
  folder of their main part (word/, xl/, ppt/); a zip without [Content_Types].xml is a plain zip;
- doc, xls, ppt: an OLE compound file (D0 CF 11 E0 A1 B1 1A E1) whose directory can be read, told apart by the stream
  at its top level (WordDocument, Workbook or Book, PowerPoint Document);
- zip: any other zip that passes testzip;
- xml: markup that parses as XML and is not XHTML;
- html: a page lxml parses;
- text: anything else without NUL bytes and with almost no control characters.
These are the feed's types (FEED_TYPES). Pictures, sound and video ("image", "media") and other binary files ("other",
such as executables, Flash, RTF and PostScript) are recognized too, so the MSHA and arlweb crawls, which keep them,
never see them refused.

Refusals (REASONS):
- soft_error: an HTML page where the URL's extension or the declared Content-Type says a file belongs (OSHA serves an
  HTML page with status 200 and Content-Type application/pdf at some PDF addresses);
- type_mismatch: text, XML, a picture or a recording where the URL's extension says a PDF, an Office file or a zip
  belongs, or text where it says XML (an error message in place of the file);
- unreadable: bytes that claim a type but fail its check (a PDF without %%EOF, a damaged zip, an OLE directory that does
  not hold together, XML that does not parse), and bytes on which a check fails in a way it does not foresee (a
  decompressor's own error), so that one malformed file never stops a crawl or a backfill;
- empty: no bytes, only white space, or a page with neither text nor any link, frame or picture;
- too_large: over the site's size limit (the fetcher stops reading; the crawler records the refusal).
A document of another kind than its extension names (a .doc address that serves RTF or a docx) is kept as what it is.

The change hash ("content hash") of a page is the SHA-256 of the text of its main content: the <main> element, or else
the element with role="main", or <article>, or else the body without its header, footer, navigation and side bars.
Scripts, styles and comments are dropped and every white-space character is removed. Markup is not hashed, so the values
that change with every request (Drupal's js-view-dom-id-* classes, form build ids and tokens, cache-busting query
strings on scripts, styles and images) and the parts of a page around its content never count as a change, while any
edit of the text does. Every other type's change hash is its SHA-256. HASH_FORMAT names this rule; a crawl store records
it in its meta table, since hashes made under another rule cannot be compared with these.
"""

from __future__ import annotations

import codecs
import hashlib
import logging
import lzma
import re
import struct
import zipfile
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from lxml import etree, html

from .urls import FILE_EXTENSIONS, extension

FEED_TYPES = ("html", "pdf", "docx", "xlsx", "pptx", "doc", "xls", "ppt", "zip", "xml", "text")
OTHER_TYPES = ("image", "media", "other")
REASONS = ("soft_error", "type_mismatch", "too_large", "unreadable", "empty")
HASH_FORMAT = "main-text/1"

HEAD_BYTES = 64 * 1024
PDF_TAIL_BYTES = 1024
MAX_PAGE_BYTES = 20 * 1024 * 1024  # larger pages are hashed whole, not parsed (the largest held OSHA page is 6.8 MB)
MAX_ZIP_TEST_BYTES = 8 * 1024**3  # members are CRC-checked up to this much uncompressed data
OLE_MAGIC = bytes.fromhex("d0cf11e0a1b11ae1")
ZIP_MAGICS = (b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")
HTML_TYPES = frozenset({"text/html", "application/xhtml+xml"})
NEUTRAL_TYPES = frozenset({"", "text/plain"})  # declared types that say nothing about a page or a file
INVISIBLE = dict.fromkeys((0x200B, 0x200C, 0x200D, 0x2060, 0xFEFF))  # zero-width characters, not white space
CONTROL_BYTES = bytes(code for code in range(32) if chr(code) not in "\t\n\r\f\v\x1a")
CONTROLS = dict.fromkeys(CONTROL_BYTES)  # for str.translate, which deletes them
CHROME = ("header", "footer", "nav", "aside")
CHROME_ROLES = ("banner", "navigation", "contentinfo", "complementary", "search")
EMBEDS = ("a", "area", "frame", "iframe", "embed", "object", "img")
HTML_START = re.compile(rb"<(!doctype\s+html|html|head|body|title|meta|link|div|p|table|center|font|frameset|h[1-6]"
                        rb"|span|a|br|img|form|ul|ol|li|b|i|strong|em|hr|pre|section|header|nav|main|article|footer"
                        rb"|aside|iframe|script|style|noscript|base)[\s/>]", re.IGNORECASE)
HTML_MARKER = re.compile(rb"<(!doctype\s+html|html|head|body)[\s>]", re.IGNORECASE)
log = logging.getLogger(__name__)

# What a URL's extension says belongs there, and the detected types that cannot be that file.
EXPECTED = {
    "document": frozenset(".pdf .doc .dot .docx .docm .xls .xlsx .xlsm .ppt .pptx .pps .ppsx .zip .kmz .odt .ods"
                          " .odp".split()),
    "xml": frozenset({".xml", ".kml"}),
    "binary": frozenset(".jpg .jpeg .png .gif .bmp .tif .tiff .webp .ico .mp3 .wav .m4a .wma .aac .ogg .mp4 .m4v .mov"
                        " .wmv .avi .flv .webm .mpg .mpeg .exe .msi .mdb .accdb .dwg .swf .eps .ai .psd .gz .tgz .tar"
                        " .7z .rar .mobi .epub .wpd".split()),
}
REFUSED_FOR = {
    "document": frozenset({"text", "xml", "image", "media"}),
    "xml": frozenset({"text", "image", "media"}),
    "binary": frozenset({"text", "xml"}),
}


@dataclass(frozen=True)
class Verdict:
    detected_type: str | None  # what the bytes are; None for an empty body
    content_sha256: str | None  # the change hash; None when the body is refused
    reason: str | None = None  # one of REASONS when the body is refused
    note: str = ""

    @property
    def usable(self) -> bool:
        return self.reason is None


def media_type(content_type: str | None) -> str:
    """'text/html; charset=utf-8' -> 'text/html'."""
    return (content_type or "").split(";", 1)[0].strip().lower()


def expected_kind(url: str) -> str | None:
    """document, xml or binary when the URL's extension names that kind of file."""
    suffix = extension(url)
    return next((kind for kind, suffixes in EXPECTED.items() if suffix in suffixes), None)


def file_expected(url: str, content_type: str | None) -> bool:
    """Whether the URL's extension or the declared Content-Type says a file, not a page, belongs at the URL."""
    declared = media_type(content_type)
    return extension(url) in FILE_EXTENSIONS or (declared not in HTML_TYPES and declared not in NEUTRAL_TYPES)


def examine(path: Path, *, url: str, content_type: str | None, sha256: str) -> Verdict:
    """The verdict on the body stored at `path`, which `url` answered with `content_type`; `sha256` is its digest.
    OSError when the body cannot be read (that says nothing about its bytes); any other failure of a check is a
    verdict, "unreadable"."""
    with path.open("rb") as handle:
        size = handle.seek(0, 2)
        handle.seek(0)
        try:
            return judge(handle, size, url=url, content_type=content_type, sha256=sha256)
        except OSError:
            raise
        except Exception as problem:  # bytes no check foresaw: refused, never a crash that stops the crawl
            log.exception("the check of the body of %s failed", url)
            return Verdict("other", None, "unreadable", f"the check failed: {type(problem).__name__}: {problem}"[:300])


def judge(handle: BinaryIO, size: int, *, url: str, content_type: str | None, sha256: str) -> Verdict:
    detected, problem = sniff(handle, handle.read(HEAD_BYTES), size)
    if detected is None:
        return Verdict(None, None, "empty", "no bytes" if size == 0 else "only white space")
    if problem:
        return Verdict(detected, None, "unreadable", problem)
    if detected == "html":
        if file_expected(url, content_type):
            declared = media_type(content_type) or "no Content-Type"
            return Verdict("html", None, "soft_error", f"an HTML page served as {declared} at a file address")
        if size > MAX_PAGE_BYTES:
            return Verdict("html", sha256, note=f"a {size:,}-byte page: hashed whole, not normalized")
        handle.seek(0)
        return page_verdict(handle.read())
    kind = expected_kind(url)
    if kind and detected in REFUSED_FOR[kind]:
        return Verdict(detected, None, "type_mismatch", f"{detected} where the {extension(url)} address names a file")
    return Verdict(detected, sha256)


def page_verdict(content: bytes) -> Verdict:
    try:
        text = main_text(content)
    except (etree.ParserError, ValueError) as problem:
        return Verdict("html", None, "unreadable", f"HTML that does not parse: {problem}"[:300])
    if text is None:
        return Verdict("html", None, "empty", "a page with neither text nor any link, frame or picture")
    return Verdict("html", page_hash(text))


def page_hash(text: str) -> str:
    """The change hash of a page whose main text (white space removed) is `text`."""
    return hashlib.sha256(f"{HASH_FORMAT}\n{text}".encode()).hexdigest()


def main_text(content: bytes) -> str | None:
    """The text of a page's main content with every white-space character removed ('' when it has none), or None for a
    page with neither text nor any link, frame or picture. ParserError or ValueError when lxml cannot read it. Bytes
    that are valid UTF-8 are read as UTF-8 (lxml would read a page without a charset declaration as Latin-1); others
    as the page declares."""
    try:
        content.decode("utf-8")
        parser = html.HTMLParser(encoding="utf-8")
    except UnicodeDecodeError:
        parser = None
    document = html.document_fromstring(content, parser=parser)
    for node in document.xpath("//script|//style|//noscript|//template"):
        node.drop_tree()
    body = document.body if document.body is not None else document
    if not "".join(document.itertext()).strip() and not any(True for _ in document.iter(*EMBEDS)):
        return None
    regions = outermost(document.xpath("//main") or document.xpath("//*[@role='main']") or document.xpath("//article"))
    if not regions:
        roles = " or ".join(f"@role='{role}'" for role in CHROME_ROLES)
        for node in outermost(body.xpath(" | ".join([*(f".//{tag}" for tag in CHROME), f".//*[{roles}]"]))):
            node.drop_tree()
        regions = [body]
    text = "".join("".join(region.itertext()) for region in regions)
    return "".join(text.translate(INVISIBLE).split())


def outermost(elements: list) -> list:
    """The elements that lie inside none of the others, in document order."""
    chosen = set(elements)
    return [element for element in elements if not any(parent in chosen for parent in element.iterancestors())]


# Sniffing -------------------------------------------------------------------------------------------------------------


def sniff(handle: BinaryIO, head: bytes, size: int) -> tuple[str | None, str | None]:
    """(type, problem): the type the first bytes show and, when the body fails that type's check, why. (None, None) for
    a body of nothing but white space."""
    if not head.strip() and size <= len(head):
        return None, None
    if head.startswith(b"%PDF-"):
        return "pdf", pdf_problem(handle, size)
    if head.startswith(ZIP_MAGICS):
        return zip_type(handle)
    if head.startswith(OLE_MAGIC):
        try:
            return ole_type(ole_streams(handle, size)), None
        except ValueError as problem:
            return "other", f"an OLE file whose directory cannot be read: {problem}"
    binary = binary_type(head)
    if binary:
        return binary, None
    text = decoded_start(head)
    if text is None:
        return "other", None
    if text.lstrip().startswith("<"):
        return markup_type(handle, head)
    return "text", None


def pdf_problem(handle: BinaryIO, size: int) -> str | None:
    handle.seek(max(0, size - PDF_TAIL_BYTES))
    if b"%%EOF" not in handle.read():
        return f"no %%EOF in the last {PDF_TAIL_BYTES:,} bytes (a truncated PDF)"
    return None


def zip_type(handle: BinaryIO) -> tuple[str, str | None]:
    """docx, xlsx, pptx or zip, and a problem when the archive or a member is damaged."""
    kind = "zip"
    try:
        with zipfile.ZipFile(handle) as archive:
            names = archive.namelist()
            if "[Content_Types].xml" in names:
                kind = ooxml_type(names)
            if sum(info.file_size for info in archive.infolist()) > MAX_ZIP_TEST_BYTES:
                return kind, None  # too much to decompress for a check; the central directory reads
            try:
                broken = archive.testzip()
            except (NotImplementedError, RuntimeError):  # a compression method zipfile lacks, or encrypted members
                return kind, None
    except (zipfile.BadZipFile, zlib.error, lzma.LZMAError, EOFError, OSError, ValueError, struct.error) as problem:
        return kind, f"a damaged zip: {problem}"[:300]
    return kind, (f"zip member {broken} fails its CRC check" if broken else None)


def ooxml_type(names: list[str]) -> str:
    folders = {name.split("/", 1)[0] for name in names if "/" in name}
    return next((kind for folder, kind in (("word", "docx"), ("xl", "xlsx"), ("ppt", "pptx")) if folder in folders),
                "zip")


def ole_type(streams: set[str]) -> str:
    if "WordDocument" in streams:
        return "doc"
    if "Workbook" in streams or "Book" in streams:
        return "xls"
    if "PowerPoint Document" in streams:
        return "ppt"
    return "other"


def ole_streams(handle: BinaryIO, size: int) -> set[str]:
    """The names at the top level of an OLE compound file (MS-CFB), the streams and storages its root holds. ValueError
    when the header, the allocation table or the directory does not hold together."""
    handle.seek(0)
    header = handle.read(512)
    if len(header) < 512:
        raise ValueError("a header shorter than 512 bytes")
    shift = struct.unpack_from("<H", header, 30)[0]
    if shift not in (9, 12):
        raise ValueError(f"a sector size of 2^{shift} bytes")
    sector = 1 << shift
    fat_count, first_directory = struct.unpack_from("<II", header, 44)
    first_difat, difat_count = struct.unpack_from("<II", header, 68)
    sectors = (size - 1) // sector  # sector n starts at byte (n + 1) * sector; the last one may be short

    def sector_bytes(number: int) -> bytes:
        if number >= sectors:
            raise ValueError(f"sector {number} lies beyond the end of the file")
        handle.seek((number + 1) * sector)
        return handle.read(sector).ljust(sector, b"\0")

    def read(number: int) -> tuple[int, ...]:
        return struct.unpack(f"<{sector // 4}I", sector_bytes(number))

    fat_sectors = [number for number in struct.unpack_from("<109I", header, 76) if number < 0xFFFFFFFA]
    seen: set[int] = set()
    number = first_difat
    while number < 0xFFFFFFFA and len(seen) < difat_count:
        if number in seen:
            raise ValueError("a loop in the DIFAT chain")
        seen.add(number)
        entries = read(number)
        fat_sectors += [entry for entry in entries[:-1] if entry < 0xFFFFFFFA]
        number = entries[-1]
    fat = [entry for number in fat_sectors[:fat_count] for entry in read(number)]
    directory = bytearray()
    seen.clear()
    number = first_directory
    while number < 0xFFFFFFFA:
        if number in seen or number >= len(fat):
            raise ValueError("a broken directory chain")
        seen.add(number)
        directory += sector_bytes(number)
        number = fat[number]
    entries = [directory[offset:offset + 128] for offset in range(0, len(directory) - 127, 128)]
    if not entries or entries[0][66] != 5:
        raise ValueError("no root entry")
    names: set[str] = set()
    stack, visited = [struct.unpack_from("<I", entries[0], 76)[0]], set()
    while stack:  # the root's children form a tree through their left and right siblings
        index = stack.pop()
        if index >= len(entries) or index in visited:
            continue
        visited.add(index)
        entry = entries[index]
        length = struct.unpack_from("<H", entry, 64)[0]
        if entry[66] in (1, 2) and 2 <= length <= 64:
            names.add(bytes(entry[:length - 2]).decode("utf-16-le", "replace"))
        stack += struct.unpack_from("<II", entry, 68)
    return names


def binary_type(head: bytes) -> str | None:
    """image, media or other for the binary formats the crawls keep, by their magic numbers."""
    if (head.startswith((b"\xff\xd8\xff", b"\x89PNG\r\n\x1a\n", b"GIF87a", b"GIF89a", b"II*\x00", b"MM\x00*",
                         b"\xc5\xd0\xd3\xc6")) or (head[:4] == b"RIFF" and head[8:12] == b"WEBP")
            or head[4:12] in (b"ftypavif", b"ftypheic")):
        return "image"
    if (head.startswith((b"ID3", b"OggS", b"fLaC", b"FLV\x01", b"\x1a\x45\xdf\xa3", b"\x00\x00\x01\xba",
                         b"\x00\x00\x01\xb3", b"\x30\x26\xb2\x75\x8e\x66\xcf\x11"))
            or (head[:4] == b"RIFF" and head[8:12] in (b"WAVE", b"AVI ")) or head[4:8] in (b"ftyp", b"moov", b"mdat")):
        return "media"
    if head.startswith((b"{\\rtf", b"%!PS", b"MZ", b"FWS", b"CWS", b"ZWS", b"Rar!\x1a\x07", b"7z\xbc\xaf\x27\x1c",
                        b"\x1f\x8b", b"\x00\x01\x00\x00Standard Jet DB", b"\x00\x01\x00\x00Standard ACE DB")):
        return "other"
    return None


def decoded_start(head: bytes) -> str | None:
    """The first bytes as text, or None when they hold NUL bytes or more than 1% control characters."""
    for bom, encoding in ((codecs.BOM_UTF16_LE, "utf-16-le"), (codecs.BOM_UTF16_BE, "utf-16-be")):
        if head.startswith(bom):
            text = head[len(bom):].decode(encoding, "ignore")
            controls = len(text) - len(text.translate(CONTROLS))
            return None if "\0" in text or controls > len(text) / 100 else text
    # UTF-8 and the single-byte code pages keep control characters as the bytes below 0x20.
    if b"\0" in head or len(head) - len(head.translate(None, CONTROL_BYTES)) > len(head) / 100:
        return None
    head = head.removeprefix(codecs.BOM_UTF8)
    return head.decode("utf-8", "ignore") if valid_utf8(head) else head.decode("cp1252", "replace")


def valid_utf8(head: bytes) -> bool:
    """Whether the bytes are UTF-8, allowing for a character cut off at the end of the sample."""
    try:
        head.decode("utf-8")
    except UnicodeDecodeError as problem:
        return problem.start >= len(head) - 3
    return True


def markup_type(handle: BinaryIO, head: bytes) -> tuple[str, str | None]:
    """html or xml for a body that starts with a tag, or text for one that is neither."""
    start = head.removeprefix(codecs.BOM_UTF8).lstrip()
    declared_xml = start[:5].lower() == b"<?xml"
    if not declared_xml and (HTML_START.match(start) or (start.startswith(b"<!--") and HTML_MARKER.search(head))):
        return "html", None
    root = xml_root(handle)
    if root is not None:
        return ("html" if root == "html" else "xml"), None
    if HTML_MARKER.search(head):
        return "html", None
    return ("xml", "XML that does not parse") if declared_xml else ("text", None)


def xml_root(handle: BinaryIO) -> str | None:
    """The local name of the document element when the whole body parses as XML; None otherwise. Each element is
    dropped once it has been read, so memory stays flat however large the file (a site's limit is 2 GB)."""
    handle.seek(0)
    root = None
    try:
        for event, element in etree.iterparse(handle, events=("start", "end"), resolve_entities=False,
                                              no_network=True, load_dtd=False, huge_tree=True):
            if event == "start" and root is None:
                root = etree.QName(element).localname.lower()
            elif event == "end":
                element.clear()
                parent = element.getparent()
                if parent is not None:  # the elements read before it: clear() alone leaves them in the tree
                    while element.getprevious() is not None:
                        del parent[0]
    except (etree.XMLSyntaxError, ValueError):
        return None
    return root
