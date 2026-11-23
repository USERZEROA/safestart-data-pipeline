"""`python -m safestart_pipeline.sample <folder>`: a small synthetic data root that the whole stack runs on.

The real data root holds 4.5 GB gathered over days of polite downloads. This one takes a few seconds and well under a
megabyte, so CI can run every service on it end to end and a reader can start the stack without the real data
(docs/DEPLOYMENT.md). It is written as the pipeline writes its own data root and read by the same code:

- the bulk store (`state/bulk.sqlite`, `raw/bulk/objects/`) holds MSHA's files as the acquire step records them: two
  weekly versions of the four datasets the replay and the version watcher read (inspections, violations, 107(a)
  orders, accidents) and of mines, one of the quarterly production reports, each with its definition file, and the
  eCFR XML of titles 29 and 30 with every section the violations cite;
- the lake is built from those zips by the lake builder itself, so the batch flow without downloads finds every
  version current and goes on to build the warehouse and the exports;
- the second version adds the records of the week from NEWS_FROM on, which the version watcher announces live; the
  replay sends the history before that day (`--until`), and CI checks what arrives of both;
- one crawl catalog, the legacy host's (`state/crawl-arlweb.sqlite`), fills the warehouse's document table. The MSHA and
  OSHA catalogs are left out on purpose: they are also the inventories the sitemap watcher compares the live sitemaps
  with, and against a tiny inventory every page those sites list would look new.

Every mine, record and URL path is invented: mine IDs start with 99, a state code MSHA does not use; the districts
(C99, M99), record keys (starting with S) and names ("Sample ...") are not MSHA's either. The files keep MSHA's own
quirks, which the lake is built to read: Windows-1252 text, quoted fields with an unescaped quote inside, and the 107(a)
export's spreadsheet preamble. The folder must be new or empty, so a real data root is never written over.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import logging
import sqlite3
import sys
import zipfile
from collections.abc import Sequence
from datetime import date
from pathlib import Path

from .acquire.bulk import MSHA_LISTING, MSHA_PREFIX, SCHEMA
from .crawl.store import SCHEMA as CRAWL_SCHEMA
from .lake.msha_ogd import DATASETS, build_lake, object_path

NEWS_FROM = date(2026, 11, 9)  # the first day of the second version's new week
# The two weekly releases: when the acquire step first saw each and the Last-Modified time MSHA served it with.
WEEKS = (("2026-11-10T14:05:12Z", "Tue, 10 Nov 2026 13:54:23 GMT"),
         ("2026-11-17T14:05:09Z", "Tue, 17 Nov 2026 13:54:23 GMT"))
D = date.fromisoformat
Column = tuple[str, str, str, str]  # as the file's header writes it, Oracle type, declared length, description
Row = tuple[object, ...]

MINE_COLUMNS: tuple[Column, ...] = (
    ("MINE_ID", "VARCHAR2", "7", "Identification number assigned to the mine by MSHA."),
    ("CURRENT_MINE_NAME", "VARCHAR2", "50", "Name of the mine."),
    ("COAL_METAL_IND", "VARCHAR2", "1", "C for a coal mine, M for metal/non-metal."),
    ("CURRENT_MINE_TYPE", "VARCHAR2", "20", "Underground, Surface or Facility."),
    ("CURRENT_MINE_STATUS", "VARCHAR2", "50", "Current status of the mine."),
    ("CURRENT_STATUS_DT", "DATE", "10", "Date the mine obtained its current status."),
    ("CURRENT_CONTROLLER_ID", "VARCHAR2", "7", "Identifier of the current controller."),
    ("CURRENT_CONTROLLER_NAME", "VARCHAR2", "100", "Name of the current controller."),
    ("CURRENT_OPERATOR_ID", "VARCHAR2", "7", "Identifier of the current operator."),
    ("CURRENT_OPERATOR_NAME", "VARCHAR2", "60", "Name of the current operator."),
    ("STATE", "VARCHAR2", "2", "State the mine is in."),
    ("FIPS_CNTY_NM", "VARCHAR2", "80", "County the mine is in."),
    ("DISTRICT", "VARCHAR2", "3", "MSHA district that inspects the mine."),
    ("PRIMARY_SIC", "VARCHAR2", "80", "Primary commodity, by Standard Industrial Classification."),
    ("PRIMARY_CANVASS", "VARCHAR2", "20", "Primary canvass of the mine."),
    ("LONGITUDE", "VARCHAR2", "10", "Longitude of the mine."),
    ("LATITUDE", "VARCHAR2", "9", "Latitude of the mine."),
    ("NO_EMPLOYEES", "NUMBER", "5", "Number of employees."),
)
MINES: tuple[Row, ...] = (
    ("9900001", "Sample Ridge No. 1 Mine", "C", "Underground", "Active", D("2011-03-14"), "S01", "Sample Coal Holdings",
     "S11", "Sample Ridge Mining LLC", "WV", "BOONE", "C99", "Coal (Bituminous)", "Coal", "-81.7012", "37.9541", 210),
    ("9900002", 'Sample Hollow "No. 2" Surface Mine', "C", "Surface", "Active", D("2015-06-01"), "S01",
     "Sample Coal Holdings", "S12", "Sample Hollow Energy Inc", "KY", "PIKE", "C99", "Coal (Bituminous)", "Coal",
     "-82.5194", "37.4787", 85),
    ("9900003", "Sample Butte Gold Mill", "M", "Surface", "Active", D("2009-09-20"), "S02", "Sample Metals Corp", "S13",
     "Sample Butte Gold LLC", "NV", "ELKO", "M99", "Gold Ore", "MetalOre", "-115.7631", "40.8324", 140),
    ("9900004", "Sample Creek Quarry", "M", "Surface", "Active", D("2004-02-10"), "S03", "Sample Aggregates Co", "S14",
     "Sample Creek Stone LLC", "AL", "SHELBY", "M99", "Crushed, Broken Limestone NEC", "Stone", "-86.8119", "33.2621",
     32),
)
# The second week: the quarry is idled and a sand pit opens.
MINES_NEW_WEEK: tuple[Row, ...] = (
    *MINES[:3],
    (*MINES[3][:4], "Temporarily Idled", D("2026-11-12"), *MINES[3][6:]),
    ("9900005", "Sample Flats Sand Pit", "M", "Surface", "New Mine", D("2026-11-02"), "S03", "Sample Aggregates Co",
     "S15", "Sample Flats Sand LLC", "TX", "WARD", "M99", "Construction Sand and Gravel", "Sand & gravel",
     "-103.1702", "31.5133", 12),
)
INSPECTION_COLUMNS: tuple[Column, ...] = (
    ("EVENT_NO", "VARCHAR2", "7", "Identification number of the inspection."),
    ("MINE_ID", "VARCHAR2", "7", "Mine inspected."),
    ("INSPECTION_BEGIN_DT", "DATE", "10", "Date the inspection began."),
    ("INSPECTION_END_DT", "DATE", "10", "Date the inspection ended."),
    ("ACTIVITY_CODE", "VARCHAR2", "20", "Kind of inspection, coded."),
    ("ACTIVITY", "VARCHAR2", "55", "Kind of inspection."),
    ("COAL_METAL_IND", "VARCHAR2", "1", "C for coal, M for metal/non-metal."),
)
REGULAR, SPOT = ("E01", "Regular Safety and Health Inspection"), ("E16", "Spot Inspection")
INSPECTIONS: tuple[Row, ...] = (
    ("S100001", "9900001", D("2025-06-02"), D("2025-06-27"), *REGULAR, "C"),
    ("S100002", "9900003", D("2025-09-08"), D("2025-09-10"), *REGULAR, "M"),
    ("S100003", "9900002", D("2026-02-03"), D("2026-02-03"), *SPOT, "C"),
    ("S100004", "9900001", D("2026-04-06"), D("2026-05-01"), *REGULAR, "C"),
    ("S100005", "9900004", D("2026-08-11"), D("2026-08-12"), *REGULAR, "M"),
    ("S100006", "9900001", D("2026-11-03"), D("2026-11-05"), *SPOT, "C"),
)
INSPECTIONS_NEW: tuple[Row, ...] = (
    ("S100007", "9900002", D("2026-11-09"), D("2026-11-10"), *SPOT, "C"),
    ("S100008", "9900005", D("2026-11-12"), D("2026-11-12"), *REGULAR, "M"),
)
VIOLATION_COLUMNS: tuple[Column, ...] = (
    ("EVENT_NO", "VARCHAR2", "7", "Inspection during which the violation was cited."),
    ("VIOLATION_NO", "VARCHAR2", "30", "Identification number of the citation or order."),
    ("MINE_ID", "VARCHAR2", "7", "Mine cited."),
    ("MINE_NAME", "VARCHAR2", "50", "Name of the mine cited."),
    ("MINE_TYPE", "VARCHAR2", "20", "Underground, Surface or Facility."),
    ("COAL_METAL_IND", "VARCHAR2", "1", "C for coal, M for metal/non-metal."),
    ("VIOLATION_ISSUE_DT", "DATE", "10", "Date the citation or order was issued."),
    ("VIOLATION_OCCUR_DT", "DATE", "10", "Date the violation occurred."),
    ("SIG_SUB", "VARCHAR2", "1", "Y when significant and substantial."),
    ("PART_SECTION", "VARCHAR2", "20", "Part and section of Title 30 CFR cited."),
    ("LIKELIHOOD", "VARCHAR2", "20", "Likelihood of an injury or illness."),
    ("INJ_ILLNESS", "VARCHAR2", "20", "Expected severity of the injury or illness."),
    ("NEGLIGENCE", "VARCHAR2", "20", "Degree of the operator's negligence."),
    ("PROPOSED_PENALTY", "NUMBER", "9,2", "Penalty proposed, in dollars."),
    ("AMOUNT_PAID", "NUMBER", "9,2", "Amount paid, in dollars."),
    ("BILL_PRINT_DT", "DATE", "10", "Date the assessment was billed."),
    ("TERMINATION_DT", "DATE", "10", "Date the violation was terminated."),
)


def violation(number: str, event: str, mine: Row, issued: str, section: str, sig_sub: str, gravity: tuple[str, ...],
              penalty: float) -> Row:
    """A violation in VIOLATION_COLUMNS' order, cited at one of the sample's mines the day it occurred, not yet
    billed."""
    day = D(issued)
    return (event, number, mine[0], mine[1], mine[3], mine[2], day, day, sig_sub, section, *gravity, penalty, None,
            None, None)


LIKELY = ("Reasonably Likely", "Lost Workdays or Restricted Duty", "Moderate")
UNLIKELY = ("Unlikely", "No Lost Workdays", "Low")
SEVERE = ("Highly Likely", "Permanently Disabling", "High")
RIDGE, HOLLOW, BUTTE, CREEK, FLATS = MINES_NEW_WEEK
VIOLATIONS: tuple[Row, ...] = (
    violation("S200001", "S100001", RIDGE, "2025-06-03", "75.400", "Y", LIKELY, 1205.0),
    violation("S200002", "S100001", RIDGE, "2025-06-10", "75.1100-3(b)", "N", UNLIKELY, 245.0),
    violation("S200003", "S100002", BUTTE, "2025-09-08", "56.14107(a)", "Y", SEVERE, 3300.0),
    violation("S200004", "S100003", HOLLOW, "2026-02-03", "77.404(a)", "N", UNLIKELY, 300.0),
    violation("S200005", "S100004", RIDGE, "2026-04-07", "75.400", "Y", LIKELY, 1850.0),
    violation("S200006", "S100004", RIDGE, "2026-04-20", "75.370(a)(1)", "Y", LIKELY, 2400.0),
    violation("S200007", "S100005", CREEK, "2026-08-11", "56.14107(a)", "N", UNLIKELY, 180.0),
    violation("S200008", "S100006", RIDGE, "2026-11-04", "75.400", "N", UNLIKELY, 600.0),
)
VIOLATIONS_NEW: tuple[Row, ...] = (
    violation("S200009", "S100007", HOLLOW, "2026-11-09", "77.1104", "Y", LIKELY, 900.0),
    violation("S200010", "S100007", HOLLOW, "2026-11-10", "77.404(a)", "N", UNLIKELY, 250.0),
    violation("S200011", "S100008", FLATS, "2026-11-12", "56.14107(a)", "Y", SEVERE, 1500.0),
)
# MSHA writes the 107(a) export for a spreadsheet: readable column names and a preamble before the header.
ORDER_COLUMNS: tuple[Column, ...] = (
    ("Coal or Metal", "VARCHAR2", "1", "Coal or metal/non-metal mine."),
    ("Mine ID", "VARCHAR2", "7", "Mine where the order was issued."),
    ("Mine Name", "VARCHAR2", "50", "Name of the mine."),
    ("State Abbreviation", "VARCHAR2", "2", "State the mine is in."),
    ("Violation No.", "VARCHAR2", "7", "Number of the order."),
    ("Violation Date Issued", "DATE", "10", "Date the order was issued."),
    ("Termination Date", "DATE", "10", "Date the order was terminated."),
)
ORDERS: tuple[Row, ...] = (("M", "9900003", BUTTE[1], "NV", "S200003", D("2025-09-08"), D("2025-09-09")),)
ORDERS_NEW: tuple[Row, ...] = (("M", "9900005", FLATS[1], "TX", "S200011", D("2026-11-12"), D("2026-11-13")),)
ACCIDENT_COLUMNS: tuple[Column, ...] = (
    ("DOCUMENT_NO", "VARCHAR2", "12", "Identification number of the accident report."),
    ("MINE_ID", "VARCHAR2", "7", "Mine where the accident occurred."),
    ("CONTRACTOR_ID", "VARCHAR2", "10", "Contractor whose employee was injured, if any."),
    ("SUBUNIT", "VARCHAR2", "32", "Part of the mine where it occurred."),
    ("ACCIDENT_DT", "DATE", "10", "Date of the accident."),
    ("DEGREE_INJURY_CD", "VARCHAR2", "2", "Degree of injury, coded."),
    ("DEGREE_INJURY", "VARCHAR2", "30", "Degree of injury."),
    ("CLASSIFICATION", "VARCHAR2", "37", "Circumstances of the accident."),
    ("ACCIDENT_TYPE", "VARCHAR2", "30", "Kind of accident."),
    ("NO_INJURIES", "NUMBER", "4", "Number of people injured."),
    ("DAYS_RESTRICT", "NUMBER", "4", "Days of restricted work."),
    ("DAYS_LOST", "NUMBER", "3", "Days away from work."),
    ("NARRATIVE", "VARCHAR2", "384", "What happened."),
    ("COAL_METAL_IND", "VARCHAR2", "1", "C for coal, M for metal/non-metal."),
)
ACCIDENTS: tuple[Row, ...] = (
    ("S30000000001", "9900001", None, "UNDERGROUND", D("2025-07-15"), "03", "DAYS AWAY FROM WORK ONLY",
     "HANDLING OF MATERIALS", "Over-exertion in lifting objects", 1, 0, 12,
     "Strained a shoulder lifting a roof bolt bundle.", "C"),
    ("S30000000002", "9900003", None, "MILL OPERATION/PREPARATION PLANT", D("2026-01-22"), "06",
     "NO DYS AWY FRM WRK,NO RSTR ACT", "MACHINERY", "Struck by", 1, 0, 0,
     "Struck on the forearm by a conveyor guard being refitted.", "M"),
    ("S30000000003", "9900001", None, "UNDERGROUND", D("2026-06-30"), "05", "DAYS RESTRICTED ACTIVITY ONLY",
     "SLIP OR FALL OF PERSON", "Fall to the walkway", 1, 8, 0, "Slipped on a wet walkway and twisted a knee.", "C"),
)
ACCIDENTS_NEW: tuple[Row, ...] = (
    ("S30000000004", "9900002", "SCX01", "STRIP, QUARY, OPEN PIT", D("2026-11-11"), "04",
     "DAYS AWAY FROM WORK & RESTRCTD", "POWERED HAULAGE", "Caught in, under or between", 1, 5, 3,
     "A contractor's hand was caught while coupling a haul truck's tailgate.", "C"),
)
PRODUCTION_COLUMNS: tuple[Column, ...] = (
    ("MINE_ID", "VARCHAR2", "7", "Mine reporting."),
    ("STATE", "VARCHAR2", "2", "State the mine is in."),
    ("SUBUNIT_CD", "VARCHAR2", "2", "Part of the mine reported, coded."),
    ("SUBUNIT", "VARCHAR2", "32", "Part of the mine reported."),
    ("CAL_YR", "NUMBER", "4", "Calendar year."),
    ("CAL_QTR", "NUMBER", "1", "Calendar quarter."),
    ("AVG_EMPLOYEE_CNT", "NUMBER", "6,2", "Average number of employees."),
    ("HOURS_WORKED", "NUMBER", "9", "Hours worked by the employees."),
    ("COAL_PRODUCTION", "NUMBER", "9", "Tons of coal produced."),
    ("COAL_METAL_IND", "VARCHAR2", "1", "C for coal, M for metal/non-metal."),
)
QUARTERS = ((2025, 3), (2025, 4), (2026, 1), (2026, 2), (2026, 3))
# mine, state, subunit, employees, hours per quarter, coal per quarter, coal/metal
PRODUCERS = (("9900001", "WV", ("01", "UNDERGROUND"), 210, 105_000, 310_000, "C"),
             ("9900002", "KY", ("03", "STRIP, QUARY, OPEN PIT"), 85, 42_000, 150_000, "C"),
             ("9900003", "NV", ("03", "STRIP, QUARY, OPEN PIT"), 140, 70_000, 0, "M"),
             ("9900004", "AL", ("03", "STRIP, QUARY, OPEN PIT"), 32, 16_000, 0, "M"))
PRODUCTION: tuple[Row, ...] = tuple(
    (mine, state, *subunit, year, quarter, float(employees), hours, coal, flag)
    for mine, state, subunit, employees, hours, coal, flag in PRODUCERS for year, quarter in QUARTERS)
# Lake dataset -> its columns and rows as each week's release holds them (one release only of the production reports).
RELEASES: dict[str, tuple[tuple[Column, ...], tuple[tuple[Row, ...], ...]]] = {
    "mines": (MINE_COLUMNS, (MINES, MINES_NEW_WEEK)),
    "inspections": (INSPECTION_COLUMNS, (INSPECTIONS, INSPECTIONS + INSPECTIONS_NEW)),
    "violations": (VIOLATION_COLUMNS, (VIOLATIONS, VIOLATIONS + VIOLATIONS_NEW)),
    "orders_107a": (ORDER_COLUMNS, (ORDERS, ORDERS + ORDERS_NEW)),
    "accidents": (ACCIDENT_COLUMNS, (ACCIDENTS, ACCIDENTS + ACCIDENTS_NEW)),
    "mines_prod_quarterly": (PRODUCTION_COLUMNS, (PRODUCTION,)),
}
MEMBERS = {"orders_107a": "107(a)OrdersIssued.csv"}

TITLE_30 = """<?xml version="1.0" encoding="UTF-8" ?>
<DLPSTEXTCLASS><HEADER><FILEDESC><TITLESTMT><TITLE>Title 30: Mineral Resources</TITLE></TITLESTMT></FILEDESC></HEADER>
<TEXT><BODY><ECFRBRWS><AMDDATE>Nov. 12, 2026</AMDDATE>
<DIV1 N="1" TYPE="TITLE"><HEAD>Title 30 (sample)</HEAD><DIV3 N="I" TYPE="CHAPTER">
<DIV5 N="56" TYPE="PART"><HEAD>PART 56</HEAD>
<DIV8 N="§ 56.14107" TYPE="SECTION"><HEAD>§ 56.14107   Moving machine parts.</HEAD>
<P>(a) Moving machine parts shall be guarded to protect persons from contacting them.</P>
<CITA TYPE="N">[53 FR 32526, Aug. 25, 1988]</CITA></DIV8></DIV5>
<DIV5 N="75" TYPE="PART"><HEAD>PART 75</HEAD>
<SOURCE><HED>Source:</HED><PSPACE>35 FR 17890, Nov. 20, 1970, unless otherwise noted.</PSPACE></SOURCE>
<DIV8 N="§ 75.370" TYPE="SECTION"><HEAD>§ 75.370   Mine ventilation plan; submission and approval.</HEAD>
<P>(a)(1) The operator shall develop and follow a ventilation plan approved by the district manager.</P>
<CITA TYPE="N">[61 FR 9829, Mar. 11, 1996]</CITA></DIV8>
<DIV8 N="§ 75.400" TYPE="SECTION"><HEAD>§ 75.400   Accumulation of combustible materials.</HEAD>
<P>Coal dust, loose coal and other combustible materials shall be cleaned up and not be permitted to accumulate.</P>
<CITA TYPE="N">[61 FR 55527, Oct. 25, 1996, as amended at 80 FR 52987, Sept. 2, 2015]</CITA></DIV8>
<DIV8 N="§ 75.1100-3" TYPE="SECTION"><HEAD>§ 75.1100-3   Condition and examination of firefighting equipment.</HEAD>
<P>All firefighting equipment shall be maintained in a usable and operative condition.</P></DIV8></DIV5>
<DIV5 N="77" TYPE="PART"><HEAD>PART 77</HEAD>
<SOURCE><HED>Source:</HED><PSPACE>36 FR 9364, May 22, 1971, unless otherwise noted.</PSPACE></SOURCE>
<DIV8 N="§ 77.404" TYPE="SECTION"><HEAD>§ 77.404   Machinery and equipment; operation and maintenance.</HEAD>
<P>(a) Mobile and stationary machinery and equipment shall be maintained in safe operating condition.</P></DIV8>
<DIV8 N="§ 77.1104" TYPE="SECTION"><HEAD>§ 77.1104   Accumulations of combustible materials.</HEAD>
<P>Combustible materials shall not be allowed to accumulate where they can create a fire hazard.</P></DIV8>
</DIV5></DIV3></DIV1></ECFRBRWS></BODY></TEXT></DLPSTEXTCLASS>
"""
TITLE_29 = """<?xml version="1.0" encoding="UTF-8" ?>
<DLPSTEXTCLASS><TEXT><BODY><ECFRBRWS><AMDDATE>Nov. 10, 2026</AMDDATE>
<DIV1 N="1" TYPE="TITLE"><HEAD>Title 29 (sample)</HEAD>
<DIV5 N="1910" TYPE="PART"><HEAD>PART 1910</HEAD>
<DIV8 N="§ 1910.1200" TYPE="SECTION"><HEAD>§ 1910.1200   Hazard communication.</HEAD>
<P>(a) Purpose. Chemical hazards shall be classified and communicated to employees.</P>
<CITA TYPE="N">[77 FR 17785, Mar. 26, 2012]</CITA></DIV8>
<DIV8 N="§ 1910.1201" TYPE="SECTION"><HEAD>§ 1910.1201   Retention of DOT markings, placards and labels.</HEAD>
<P>(a) Markings shall be retained on packages received from shipment.</P></DIV8></DIV5>
</DIV1></ECFRBRWS></BODY></TEXT></DLPSTEXTCLASS>
"""
ECFR_LAST_MODIFIED = {29: "Thu, 12 Nov 2026 22:17:51 GMT", 30: "Fri, 13 Nov 2026 22:43:08 GMT"}
# The legacy host's catalog: url, and the content type and content of a page downloaded (none for one not found).
ARLWEB = (("https://arlweb.msha.gov/sample/fatals/2025/sample-report.asp", "text/html",
           b"<html><body><h1>Sample fatality report</h1></body></html>"),
          ("https://arlweb.msha.gov/sample/fatals/2025/sample-report.pdf", "application/pdf", b"%PDF-1.4 sample\n"),
          ("https://arlweb.msha.gov/sample/stats/sample-data.txt", "text/plain", b"MINE_ID|NAME\r\n9900001|Sample\r\n"),
          ("https://arlweb.msha.gov/sample/old/missing-page.asp", None, None))


def cell(value: object) -> str:
    """A value as MSHA's exports write it: text and dates quoted, numbers bare, nothing for a null."""
    if value is None:
        return ""
    if isinstance(value, date):
        return f'"{value:%m/%d/%Y}"'
    return str(value) if isinstance(value, int | float) else f'"{value}"'


def data_file(name: str, columns: Sequence[Column], rows: Sequence[Row], as_of: str) -> bytes:
    """A dataset's text file: Windows-1252, CRLF lines, a header and one line per record."""
    lines = ["|".join(column[0] for column in columns), *("|".join(map(cell, row)) for row in rows)]
    text = "\r\n".join(lines) + "\r\n"
    if name == "orders_107a":
        text = f"sep=|\r107(a) Order Issued Report\r\nData as of {as_of} - Violations issued through {as_of}\r\n" + text
    return text.encode("cp1252")


def zipped(member: str, data: bytes, when: tuple[int, int, int, int, int, int]) -> bytes:
    """One member in a zip with a fixed time and file attributes, stored uncompressed (zlib builds compress alike
    input differently), so the sample's versions have the same SHA-256, and the same names in the lake, wherever it is
    written."""
    info = zipfile.ZipInfo(member, date_time=when)
    info.create_system, info.external_attr = 3, 0o644 << 16
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(info, data)
    return buffer.getvalue()


def definition_file(name: str, columns: Sequence[Column]) -> bytes:
    """A dataset's definition file, naming each column's Oracle type as MSHA's do."""
    table = name.upper().replace("_", " ")
    lines = ["TABLE_NAME|COLUMN_NAME|DATA_TYPE|DATA_LENGTH|FIELD_DESCRIPTION",
             *(f"{table}|{header.upper()}|{kind}|{length}|{meaning}" for header, kind, length, meaning in columns)]
    return ("\r\n".join(lines) + "\r\n").encode("cp1252")


def keep(root: Path, blob: bytes) -> str:
    """Store a file in the bulk store's objects by its SHA-256, as the acquire step does; returns the digest."""
    digest = hashlib.sha256(blob).hexdigest()
    path = object_path(root, digest)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(blob)
    return digest


def record(db: sqlite3.Connection, root: Path, source: str, url: str, versions: Sequence[tuple[bytes, str, str]],
           content_type: str) -> None:
    """A bulk file with every version held, (content, first seen, Last-Modified) oldest first; the last is current.
    A file unchanged since an earlier week was last answered with a 304, as the acquire step records it."""
    for blob, seen, modified in versions:
        db.execute("insert into versions (url, sha256, content_length, last_modified, first_seen_at)"
                   " values (?, ?, ?, ?, ?)", (url, keep(root, blob), len(blob), modified, seen))
    blob, seen, modified = versions[-1]
    state, status = ("fetched", 200) if seen == WEEKS[-1][0] else ("unchanged", 304)
    db.execute("insert into files (url, source, name, listed_at, state, http_status, content_type, content_length,"
               " sha256, last_modified, fetched_at, verified, verify_note)"
               " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'sample file')",
               (url, source, url.rsplit("/", 1)[-1], WEEKS[-1][0], state, status, content_type, len(blob),
                hashlib.sha256(blob).hexdigest(), modified, WEEKS[-1][0]))


def write_bulk_store(root: Path) -> None:
    """Every release of every sample dataset with its definition file, the two eCFR titles, and the weekly listings.
    The store is left in SQLite's default journal mode, so the read-only containers read it without a -wal file."""
    db = sqlite3.connect(root / "state" / "bulk.sqlite")
    try:
        db.executescript(SCHEMA)
        for name, (columns, releases) in RELEASES.items():
            zip_name, definition_name = DATASETS[name]
            member = MEMBERS.get(name, zip_name.replace(".zip", ".txt"))
            versions = []
            for (seen, modified), rows in zip(WEEKS[-len(releases):], releases, strict=True):
                as_of = f"{date.fromisoformat(seen[:10]):%d-%b-%y}".upper()
                stamp = (int(seen[:4]), int(seen[5:7]), int(seen[8:10]), 6, 0, 0)
                versions.append((zipped(member, data_file(name, columns, rows, as_of), stamp), seen, modified))
            record(db, root, "msha-ogd", MSHA_PREFIX + zip_name, versions, "application/x-zip-compressed")
            definition = (definition_file(name, columns), WEEKS[0][0], "Tue, 16 May 2023 13:47:39 GMT")
            record(db, root, "msha-ogd", MSHA_PREFIX + definition_name, [definition], "text/plain")
        for title, xml in ((29, TITLE_29), (30, TITLE_30)):
            url = f"https://www.govinfo.gov/bulkdata/ECFR/title-{title}/ECFR-title{title}.xml"
            record(db, root, "ecfr-govinfo", url, [(xml.encode("utf-8"), WEEKS[-1][0], ECFR_LAST_MODIFIED[title])],
                   "text/xml")
        for seen, _ in WEEKS:
            db.execute("insert into listings (source, listed_at, page_url, files) values ('msha-ogd', ?, ?, ?)",
                       (seen, MSHA_LISTING, 2 * len(RELEASES)))
            db.execute("insert into listings (source, listed_at, files) values ('ecfr-govinfo', ?, 2)", (seen,))
        db.commit()
    finally:
        db.close()


def write_catalog(root: Path) -> None:
    """The legacy host's crawl catalog, with each downloaded page in the crawl's raw archive (raw/arlweb/objects/)."""
    db = sqlite3.connect(root / "state" / "crawl-arlweb.sqlite")
    try:
        db.executescript(CRAWL_SCHEMA)
        for url, content_type, content in ARLWEB:
            digest = content and hashlib.sha256(content).hexdigest()
            if content:
                path = root / "raw" / "arlweb" / "objects" / digest[:2] / digest[2:4] / digest
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
            db.execute("insert into urls (url, source, first_seen_at, state, http_status, content_type, content_length,"
                       " sha256, fetched_at) values (?, 'links', ?, ?, ?, ?, ?, ?, ?)",
                       (url, WEEKS[0][0], "fetched" if content else "not_found", 200 if content else 404, content_type,
                        content and len(content), digest, WEEKS[0][0] if content else None))
        db.execute("insert into meta (key, value) values ('last_stop', 'done')")
        db.commit()
    finally:
        db.close()


def write_sample(root: Path) -> dict:
    """Write the sample data root into `root` and build its lake; returns what the lake holds."""
    (root / "state").mkdir(parents=True, exist_ok=True)
    write_bulk_store(root)
    write_catalog(root)
    results = build_lake(root, only=list(RELEASES))
    return {"data_root": str(root), "news_from": NEWS_FROM.isoformat(),
            "lake": {name: {key: result[key] for key in ("parquet", "rows", "versions")}
                     for name, result in results.items() if "parquet" in result}}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m safestart_pipeline.sample",
                                     description="Write a small synthetic data root that the whole stack runs on.")
    parser.add_argument("folder", type=Path, help="a new or empty folder")
    args = parser.parse_args(argv)
    if args.folder.exists() and (not args.folder.is_dir() or any(args.folder.iterdir())):
        parser.error(f"{args.folder} is not an empty folder; the sample is written only into a new or empty one")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s", stream=sys.stderr)
    print(json.dumps(write_sample(args.folder.resolve()), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
