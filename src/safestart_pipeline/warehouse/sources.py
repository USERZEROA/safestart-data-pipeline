"""The warehouse's two inputs besides the lake: the eCFR text of titles 29 and 30, and the three crawl catalogs.

Both are read once per build and staged as Parquet, so DuckDB loads them with the same SQL as the lake.

The eCFR files are govinfo's bulk XML. Each DIV8 TYPE="SECTION" element is one section: N="§ 75.400", a HEAD that
repeats the number before the heading, the paragraphs, and a CITA source note listing the Federal Register documents
that set the section's text ("[61 FR 55527, Oct. 25, 1996, as amended at 80 FR 52987, Sept. 2, 2015]"). The latest of
those dates is when the text last changed. A section without a note still has the text its subpart or part was
published with, so its date comes from the nearest enclosing SOURCE note instead.

A crawl catalog is the crawler's SQLite state: one row per URL it learned of, in whatever state the URL ended. Its kind
(page, pdf, data, office, other) comes from the URL alone, by the rule the crawler itself uses: a path whose last
segment has no known file extension is a page ('/1910.1200' and '/standardnumber/1910/1910.7' are pages).
"""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Iterator
from datetime import date
from pathlib import Path

from lxml import etree

from ..crawl.urls import FILE_EXTENSIONS, extension
from ..lake.msha_ogd import object_path
from ..verify import clean_section, parse_amddate
from ..verify import read_only as verify_read_only

ECFR_SOURCE = "ecfr-govinfo"
CRAWL_SITES = ("osha", "msha", "arlweb")
PDF = frozenset({".pdf"})
OFFICE = frozenset({".doc", ".docx", ".docm", ".dot", ".rtf", ".odt", ".wpd", ".xls", ".xlsx", ".xlsm", ".ods", ".ppt",
                    ".pptx", ".pps", ".ppsx", ".odp", ".vsd", ".vss"})
DATA = frozenset({".csv", ".txt", ".xml", ".json", ".zip", ".gz", ".tgz", ".tar", ".7z", ".rar", ".dat", ".mdb",
                  ".accdb", ".db", ".kml", ".kmz"})
# Files the crawler's extension list does not name (it treats them as pages that might link onward).
OTHER_FILES = frozenset({".wvx", ".avif"})
# Parts of a section that are not its text: the heading (kept on its own), the provenance notes, and eCFR's links to an
# amendment not yet in effect ("Link to an amendment published at 91 FR 72793, Nov. 12, 2026.").
NOT_TEXT = frozenset({"HEAD", "CITA", "SECAUTH", "XREF"})
FR_DATE = re.compile(r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+(\d{1,2}),\s+(\d{4})\b")
MONTHS = {name: number for number, name in enumerate(
    ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"), 1)}


def read_only(path: Path) -> sqlite3.Connection:
    """A reading connection that returns plain tuples (verify.read_only, which also opens a WAL database on a mount it
    cannot write)."""
    db = verify_read_only(path)
    db.row_factory = None
    return db


# eCFR -----------------------------------------------------------------------------------------------------------------


def ecfr_file(data_root: Path, title: int) -> tuple[Path, str]:
    """The verified govinfo XML of a title the bulk store holds, and its SHA-256, whatever its last check found: an
    error, a refusal or a download that failed verification leaves the copy held current, as the lake keeps its own."""
    db = read_only(data_root / "state" / "bulk.sqlite")
    try:
        row = db.execute(
            "select sha256 from files where source = ? and name = ? and verified = 1 and sha256 is not null",
            (ECFR_SOURCE, f"ECFR-title{title}.xml"),
        ).fetchone()
    finally:
        db.close()
    if not row or not row[0]:
        raise FileNotFoundError(f"no verified download of eCFR title {title} in the bulk store")
    return object_path(data_root, row[0]), row[0]


def fr_dates(text: str) -> list[date]:
    """Every date written the Federal Register way ('Oct. 25, 1996', 'Sept. 2, 2015', 'June 28, 1974') in a note."""
    found = []
    for month, day, year in FR_DATE.findall(text):
        try:
            found.append(date(int(year), MONTHS[month], int(day)))
        except ValueError:
            continue
    return found


def section_heading(head: str | None, number: str) -> str | None:
    """The heading without the section number the HEAD starts with ('§ 75.400   Accumulation of ...')."""
    if head is None:
        return None
    text = " ".join(head.split())
    prefix = " ".join(number.split())
    if prefix and text.startswith(prefix):
        return text[len(prefix):].strip()
    return re.sub(r"^§+\s*\S+\s*", "", text)


def section_text(section: etree._Element) -> str:
    """The section's paragraphs, notes and tables as plain text, one block per line."""
    blocks = []
    for child in section:
        if isinstance(child.tag, str) and child.tag not in NOT_TEXT:
            block = " ".join("".join(child.itertext()).split())
            if block:
                blocks.append(block)
    return "\n".join(blocks)


def amended_on(section: etree._Element) -> date | None:
    """The latest Federal Register date in the section's source note, or else in the nearest enclosing SOURCE note."""
    notes = section.findall("CITA")
    holder = section.getparent()
    while not notes and holder is not None:
        notes = holder.findall("SOURCE")
        holder = holder.getparent()
    dates = [day for note in notes for day in fr_dates("".join(note.itertext()))]
    return max(dates) if dates else None


def part_number(section: etree._Element) -> int | None:
    """The number of the enclosing part; None for a lettered part such as 29 CFR part 1912a."""
    holder = section.getparent()
    while holder is not None and not (holder.tag == "DIV5" and holder.get("TYPE") == "PART"):
        holder = holder.getparent()
    number = holder.get("N", "") if holder is not None else ""
    return int(number) if number.isdigit() else None


def ecfr_sections(path: Path, title: int) -> Iterator[tuple]:
    """(section, title, part, heading, text, amended_on) for every section of a title file, streamed: each section,
    subpart and part is cleared once read, so memory stays at one part however large the title."""
    with path.open("rb") as handle:
        for _, element in etree.iterparse(handle, events=("end",), tag=("DIV5", "DIV6", "DIV8"), huge_tree=True,
                                          resolve_entities=False):
            if element.tag == "DIV8" and element.get("TYPE") == "SECTION":
                number = element.get("N") or ""
                heading = section_heading(element.findtext("HEAD"), number)
                yield (clean_section(number), title, part_number(element), heading, section_text(element),
                       amended_on(element))
                element.clear()
            elif element.tag in ("DIV5", "DIV6"):
                element.clear()


def ecfr_amended(path: Path) -> str | None:
    """The title's own amendment date (AMDDATE, at the start of the file) as YYYY-MM-DD."""
    with path.open("rb") as handle:
        for _, element in etree.iterparse(handle, events=("end",), tag="AMDDATE", huge_tree=True,
                                          resolve_entities=False):
            return parse_amddate((element.text or "").strip())
    return None


# Crawl catalogs -------------------------------------------------------------------------------------------------------


def kind_of(url: str) -> str:
    """page, pdf, data, office or other, from the URL's extension."""
    suffix = extension(url)
    if suffix in PDF:
        return "pdf"
    if suffix in OFFICE:
        return "office"
    if suffix in DATA:
        return "data"
    if suffix in FILE_EXTENSIONS or suffix in OTHER_FILES:
        return "other"
    return "page"


def media_type(content_type: str | None) -> str | None:
    """'text/html; charset=utf-8' -> 'text/html'."""
    value = (content_type or "").split(";", 1)[0].strip().lower()
    return value or None


def catalog_rows(path: Path, site: str) -> Iterator[tuple]:
    """(url, site, state, kind, content_type, bytes, sha256, fetched_at, in_sitemap) for every URL of a crawl catalog,
    read without changing it (a crawl may be writing to it)."""
    db = read_only(path)
    try:
        cursor = db.execute("select url, state, content_type, content_length, sha256, fetched_at, in_sitemap from urls")
        for url, state, content_type, length, sha256, fetched_at, in_sitemap in cursor:
            yield url, site, state, kind_of(url), media_type(content_type), length, sha256, fetched_at, bool(in_sitemap)
    finally:
        db.close()
