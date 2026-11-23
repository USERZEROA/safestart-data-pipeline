from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import date, datetime

import duckdb
import httpx
import polars as pl
import pytest

from safestart_pipeline.acquire.bulk import SCHEMA as BULK_SCHEMA
from safestart_pipeline.acquire.bulk import BulkAcquirer, BulkFile
from safestart_pipeline.crawl.store import SCHEMA as CRAWL_SCHEMA
from safestart_pipeline.lake.msha_ogd import object_path
from safestart_pipeline.warehouse.build import build
from safestart_pipeline.warehouse.exports import export
from safestart_pipeline.warehouse.lock import warehouse_lock
from safestart_pipeline.warehouse.named_queries import (
    markdown,
    query_names,
    question,
    run_query,
)
from safestart_pipeline.warehouse.sources import fr_dates, kind_of, section_heading

D = date.fromisoformat
M1, M2, M3 = "0100001", "0100002", "0100003"
UNLISTED = "0100009"  # a mine missing from the mines file
MINES = pl.DataFrame({
    "mine_id": [M1, M2, M3], "current_mine_name": ["Deep Coal No. 1", "Silver Pit", "Old Strip"],
    "coal_metal_ind": ["C", "M", "C"], "current_mine_type": ["Underground", "Surface", "Surface"],
    "current_mine_status": ["Active", "Active", "Abandoned"], "current_status_dt": [D("2001-01-01")] * 3,
    "state": ["WV", "NV", "KY"], "fips_cnty_nm": ["Boone", "Elko", "Pike"], "district": ["C04", "M6", "C07"],
    "primary_sic": ["Coal (Bituminous)", "Silver Ore", "Coal (Bituminous)"],
    "primary_canvass": ["Coal", "MetalOre", "Coal"],
    # The second mine's longitude is a typing error MSHA's file holds (-0.0003); it is dropped with its latitude.
    "latitude": ["37.5", "40.1", None], "longitude": ["-81.2", "-0.0003", None],
    "current_controller_id": ["C1", "C2", None], "current_controller_name": ["Coal Co", "Silver Co", None],
    "current_operator_id": ["O1", "O2", None], "current_operator_name": ["Coal Op", "Silver Op", None],
})
# Event 1000001 is listed twice, once per controller, as one event is in MSHA's file. Event 1000003 carries the metal
# flag though its mine is a coal mine.
INSPECTIONS = pl.DataFrame({
    "event_no": ["1000001", "1000001", "1000002", "1000003"], "mine_id": [M1, M1, M2, M1],
    "inspection_begin_dt": [D("2026-03-01"), D("2026-03-01"), D("2024-06-01"), D("2025-12-01")],
    "inspection_end_dt": [D("2026-03-05"), D("2026-03-05"), D("2024-06-02"), None],
    "activity_code": ["E01", "E01", "E01", "E02"], "activity": ["Regular inspection"] * 3 + ["Spot inspection"],
    "coal_metal_ind": ["C", "C", "M", "M"], "controller_id": ["C1", "C9", "C2", "C1"],
})
# Violation V3 is listed twice, once per contest docket. V6 is the newest (the 12-month anchor) and names an
# inspection that is not in the file. V7 carries the coal flag though its mine is a metal mine. The last row has no
# number (the lake turns MSHA's "?" into a null), so no key can hold it.
VIOLATION_ROWS = [
    # violation_no, event_no, mine_id, issued, part_section, sig_sub, proposed_penalty, coal_metal, docket_no
    ("V1", "1000001", M1, "2026-03-02", "75.400", "Y", 5000.0, "C", None),
    ("V2", "1000001", M1, "2026-03-02", "75.400", "N", 300.0, "C", None),
    ("V3", "1000001", M1, "2026-03-03", "75.1100-3", "Y", 1000.0, "C", "D-1"),
    ("V3", "1000001", M1, "2026-03-03", "75.1100-3", "Y", 1000.0, "C", "D-2"),
    ("V4", "1000003", M1, "2025-12-02", "77.403-ag", "N", 100.0, "C", None),
    ("V5", "1000002", M2, "2024-06-01", "56.14107(a)", "Y", 2000.0, "M", None),
    ("V6", "9999999", M2, "2026-06-30", "56.14107(a)", None, None, "M", None),
    ("V7", "1000002", M2, "2024-06-01", None, "N", 60.0, "C", None),
    ("V8", "1000003", M1, "2026-01-15", "1.1", "N", 50.0, "C", None),
    ("V9", "1000003", M1, "2026-01-16", "Sec. 103(a)", None, None, "C", None),
    (None, "1000001", M1, "2026-03-04", "75.400", "Y", 500.0, "C", None),
]
VIOLATIONS = pl.DataFrame({
    "violation_no": [row[0] for row in VIOLATION_ROWS], "event_no": [row[1] for row in VIOLATION_ROWS],
    "mine_id": [row[2] for row in VIOLATION_ROWS], "violation_issue_dt": [D(row[3]) for row in VIOLATION_ROWS],
    "violation_occur_dt": [D(row[3]) for row in VIOLATION_ROWS], "part_section": [row[4] for row in VIOLATION_ROWS],
    "sig_sub": [row[5] for row in VIOLATION_ROWS], "likelihood": ["Reasonably"] * len(VIOLATION_ROWS),
    "inj_illness": ["LostDays"] * len(VIOLATION_ROWS), "negligence": ["ModNegligence"] * len(VIOLATION_ROWS),
    "proposed_penalty": [row[6] for row in VIOLATION_ROWS], "amount_paid": [None] * len(VIOLATION_ROWS),
    "termination_dt": [None] * len(VIOLATION_ROWS), "coal_metal_ind": [row[7] for row in VIOLATION_ROWS],
    "mine_type": ["Underground"] * len(VIOLATION_ROWS), "docket_no": [row[8] for row in VIOLATION_ROWS],
}, schema_overrides={"amount_paid": pl.Float64, "termination_dt": pl.Date})
ACCIDENT_ROWS = [
    # document_no, mine_id, date, degree, days_lost, days_restrict, classification, contractor_id, coal_metal
    ("A1", M1, "2026-02-10", "01", None, None, "POWERED HAULAGE", None, "C"),  # fatal
    ("A2", M1, "2026-04-15", "03", 0, None, "HANDLING OF MATERIALS", None, "C"),  # days away, not yet counted
    ("A3", M1, "2026-05-20", "06", 0, 3, "SLIP OR FALL OF PERSON", None, "C"),  # degree 06 despite restricted days
    ("A4", M1, "2026-05-21", "05", 0, 4, "MACHINERY", "CTR1", "C"),  # a contractor's employee
    ("A5", M2, "2025-01-10", "00", None, None, "OTHER", None, "M"),  # accident only, no injury
    ("A6", M1, "2026-07-15", "02", None, None, "MACHINERY", None, "C"),  # a disability after the newest violation
    ("A7", M2, "2026-03-01", "07", 30, None, "DISORDERS (PHYSICAL AGENTS)", None, "C"),  # an illness, flagged coal
    ("A8", M2, "2025-11-20", "06", 0, 0, "SLIP OR FALL OF PERSON", None, "M"),  # in a quarter M2 has not reported
    ("A9", M2, "2026-05-05", "03", 2, None, "HANDLING OF MATERIALS", None, "M"),  # M2 reported that quarter, 0 hours
]
ACCIDENTS = pl.DataFrame({
    "document_no": [row[0] for row in ACCIDENT_ROWS], "mine_id": [row[1] for row in ACCIDENT_ROWS],
    "accident_dt": [D(row[2]) for row in ACCIDENT_ROWS], "degree_injury_cd": [row[3] for row in ACCIDENT_ROWS],
    "degree_injury": ["(description)"] * len(ACCIDENT_ROWS), "classification": [row[6] for row in ACCIDENT_ROWS],
    "accident_type": ["(type)"] * len(ACCIDENT_ROWS), "no_injuries": [1, 1, 1, 1, 0, 1, 1, 1, 1],
    "days_lost": [row[4] for row in ACCIDENT_ROWS], "days_restrict": [row[5] for row in ACCIDENT_ROWS],
    "subunit": ["UNDERGROUND"] * 4 + ["STRIP, QUARY, OPEN PIT", "UNDERGROUND"]
               + ["MILL OPERATION/PREPARATION PLANT"] * 3,
    "narrative": ["(narrative)"] * len(ACCIDENT_ROWS), "coal_metal_ind": [row[8] for row in ACCIDENT_ROWS],
    "contractor_id": [row[7] for row in ACCIDENT_ROWS],
}, schema_overrides={"days_lost": pl.Int64, "days_restrict": pl.Int64})
# The first quarter of M1 (a coal mine) is reported with the metal flag; the unlisted mine's report keeps its own. M2
# reports 2026 Q1, and 2026 Q2 without hours. M1's last report has no subunit, so no key can hold it.
QUARTERS = [(2025, 3), (2025, 4), (2026, 1), (2026, 2), (2026, 3)]
PRODUCTION = pl.DataFrame({
    "mine_id": [M1] * 5 + [M2, UNLISTED, M2, M1], "cal_yr": [year for year, _ in QUARTERS] + [2026] * 4,
    "cal_qtr": [quarter for _, quarter in QUARTERS] + [1, 2, 2, 1], "subunit_cd": ["01"] * 5 + ["03", "03", "03", None],
    "avg_employee_cnt": [25.0] * 9, "hours_worked": [50_000] * 5 + [10_000, 5_000, 0, 99],
    "coal_production": [1000] * 5 + [0, 0, 0, 1000], "coal_metal_ind": ["M", "C", "C", "C", "C", "M", "M", "M", "C"],
})
TITLE_30 = """<?xml version="1.0" encoding="UTF-8" ?>
<DLPSTEXTCLASS><HEADER><FILEDESC><TITLESTMT><TITLE>Title 30: Mineral Resources</TITLE></TITLESTMT></FILEDESC></HEADER>
<TEXT><BODY><ECFRBRWS><AMDDATE>Nov. 12, 2026
</AMDDATE>
<DIV1 N="1" TYPE="TITLE"><HEAD>Title 30</HEAD><DIV3 N="I" TYPE="CHAPTER">
<DIV5 N="1" TYPE="PART"><HEAD>PART 1—OFFICIAL EMBLEM</HEAD>
<SOURCE><HED>Source:</HED><PSPACE>39 FR 23997, June 28, 1974, unless otherwise noted.</PSPACE></SOURCE>
<DIV8 N="§ 1.1" TYPE="SECTION"><HEAD>§ 1.1   Official emblem.</HEAD><P>The following emblem is established.</P></DIV8>
</DIV5>
<DIV5 N="56" TYPE="PART"><HEAD>PART 56</HEAD>
<DIV8 N="§ 56.14107" TYPE="SECTION"><HEAD>§ 56.14107   Moving machine parts.</HEAD>
<P>(a) Moving machine parts shall be guarded.</P><CITA TYPE="N">[53 FR 32526, Aug. 25, 1988]</CITA></DIV8>
</DIV5>
<DIV5 N="75" TYPE="PART"><HEAD>PART 75</HEAD>
<SOURCE><HED>Source:</HED><PSPACE>35 FR 17890, Nov. 20, 1970, unless otherwise noted.</PSPACE></SOURCE>
<DIV6 N="E" TYPE="SUBPART"><HEAD>Subpart E—Combustible Materials</HEAD>
<DIV8 N="§ 75.400" TYPE="SECTION"><HEAD>§ 75.400   Accumulation of combustible materials.</HEAD>
<P>Coal dust, including <I>float coal dust</I> deposited on rock-dusted surfaces, shall be cleaned up.
</P><CITA TYPE="N">[61 FR 55527, Oct. 25, 1996, as amended at 80 FR 52987, Sept. 2, 2015]</CITA></DIV8>
<DIV8 N="§ 75.1100-3" TYPE="SECTION"><HEAD>§ 75.1100-3   Condition and examination of firefighting equipment.</HEAD>
<XREF ID="20261112" REFID="13">Link to an amendment published at 91 FR 72793, Nov. 12, 2026.</XREF>
<P>(a) All firefighting equipment shall be maintained.</P><P>(b) It shall be examined weekly.</P></DIV8>
</DIV6></DIV5></DIV3></DIV1></ECFRBRWS></BODY></TEXT></DLPSTEXTCLASS>
"""
TITLE_29 = """<?xml version="1.0" encoding="UTF-8" ?>
<DLPSTEXTCLASS><TEXT><BODY><ECFRBRWS><AMDDATE>Nov. 10, 2026</AMDDATE>
<DIV1 N="1" TYPE="TITLE"><HEAD>Title 29</HEAD>
<DIV5 N="1" TYPE="PART"><HEAD>PART 1</HEAD>
<DIV8 N="§ 1.1" TYPE="SECTION"><HEAD>§ 1.1   Purpose and scope.</HEAD><P>Wage rates.</P>
<CITA TYPE="N">[48 FR 19540, Apr. 29, 1983]</CITA></DIV8></DIV5>
<DIV5 N="1910" TYPE="PART"><HEAD>PART 1910</HEAD>
<DIV8 N="§ 1910.1200" TYPE="SECTION"><HEAD>§ 1910.1200   Hazard communication.</HEAD><P>(a) Purpose.</P>
<CITA TYPE="N">[77 FR 17785, Mar. 26, 2012]</CITA></DIV8>
<DIV8 N="§§ 1910.1201—1910.1202" TYPE="SECTION"><HEAD>§§ 1910.1201--1910.1202 [Reserved]</HEAD></DIV8></DIV5>
<DIV5 N="1912a" TYPE="PART"><HEAD>PART 1912a</HEAD>
<DIV8 N="§ 1912a.1" TYPE="SECTION"><HEAD>§ 1912a.1   Scope.</HEAD><P>Committees.</P></DIV8></DIV5>
</DIV1></ECFRBRWS></BODY></TEXT></DLPSTEXTCLASS>
"""
CATALOGS = {
    "osha": [
        # url, state, content_type, content_length, in_sitemap
        ("https://www.osha.gov/laws-regs/regulations/standardnumber/1910/1910.1200", "fetched",
         "text/html; charset=utf-8", 50_000, 1),
        ("https://www.osha.gov/sites/default/files/publications/OSHA3514.pdf", "fetched", "application/pdf",
         2_000_000, 1),
        ("https://www.osha.gov/data/inspections.zip", "skipped", None, None, 0),
        ("https://www.osha.gov/files/report.xlsx", "not_found", None, None, 0),
        ("https://www.osha.gov/images/photo.jpg", "skipped", None, None, 0),
    ],
    "msha": [
        ("https://www.msha.gov/", "fetched", "text/html; charset=utf-8", 30_000, 1),
        ("https://www.msha.gov/sites/default/files/alert.pdf", "fetched", "application/pdf", 100_000, 1),
    ],
    "arlweb": [
        ("https://arlweb.msha.gov/stats/centurystats/coalstats.asp", "fetched", "text/html", 10_000, 0),
        ("https://arlweb.msha.gov/OpenGovernmentData/DataSets/Mines.zip", "fetched", "application/x-zip-compressed",
         9_000_000, 0),
        ("https://arlweb.msha.gov/training/video.wvx", "fetched", "video/x-ms-wvx", 500, 0),
    ],
}
CONTRACT = {
    "mine": "mine_id mine_name coal_metal mine_type status status_date state county district primary_sic "
            "primary_canvass latitude longitude controller_id controller_name operator_id operator_name",
    "inspection": "event_no mine_id begin_date end_date activity_code activity coal_metal",
    "violation": "violation_no event_no mine_id issue_date occur_date part_section cfr_section sig_sub likelihood "
                 "injury_illness negligence proposed_penalty amount_paid termination_date coal_metal mine_type",
    "accident": "document_no mine_id accident_date degree_injury_cd degree_injury serious classification "
                "accident_type no_injuries days_lost days_restrict subunit narrative coal_metal contractor_id",
    "production_quarter": "mine_id cal_yr cal_qtr subunit_cd avg_employees hours_worked coal_production coal_metal",
    "cfr_section": "section title part heading text amended_on",
    "document": "url site state kind content_type bytes sha256 fetched_at in_sitemap",
    "dataset_version": "dataset version rows built_at is_current",
    "mart_top_cited_sections": "cfr_section heading citations ss_citations ss_share proposed_penalty_total last_cited",
    "mart_mine_scorecard": "mine_id mine_name coal_metal mine_type status state district latitude longitude "
                           "inspections_12m violations_12m ss_violations_12m proposed_penalty_12m accidents_12m "
                           "serious_accidents_12m hours_worked_12m injury_rate_12m top_sections_12m",
    "mart_ss_trend": "year coal_metal violations ss_violations ss_share",
    "mart_injury_rate": "year coal_metal injuries hours_worked rate_per_200k",
    "mart_district_year": "district year inspections violations ss_violations accidents",
}
KEYS = {"mine": ["mine_id"], "inspection": ["event_no"], "violation": ["violation_no"], "accident": ["document_no"],
        "production_quarter": ["mine_id", "cal_yr", "cal_qtr", "subunit_cd"], "cfr_section": ["section", "title"],
        "document": ["url"], "dataset_version": ["dataset", "version"]}


def write_dataset(lake, name, frame, *, older=None):
    """A lake dataset: Parquet named after a (fake) source SHA-256 and a manifest naming it current."""
    folder = lake / name
    folder.mkdir(parents=True)
    versions = []
    for number, version in enumerate(item for item in (older, frame) if item is not None):
        digest = hashlib.sha256(f"{name}-{number}".encode()).hexdigest()
        version.write_parquet(folder / f"{digest[:16]}.parquet")
        versions.append({"format": 3, "source_sha256": digest, "parquet": f"{digest[:16]}.parquet",
                         "rows": version.height, "built_at": f"2026-11-2{number}T10:00:00Z"})
    manifest = {"dataset": name, "versions": versions, "current": versions[-1]["parquet"]}
    (folder / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


def write_data_root(root):
    lake = root / "lake" / "msha_ogd"
    write_dataset(lake, "mines", MINES)
    write_dataset(lake, "inspections", INSPECTIONS)
    write_dataset(lake, "violations", VIOLATIONS, older=VIOLATIONS.head(3))
    write_dataset(lake, "accidents", ACCIDENTS)
    write_dataset(lake, "mines_prod_quarterly", PRODUCTION)
    (root / "state").mkdir()
    bulk = sqlite3.connect(root / "state" / "bulk.sqlite")
    bulk.executescript(BULK_SCHEMA)
    for title, xml in ((29, TITLE_29), (30, TITLE_30)):
        blob = xml.encode("utf-8")
        digest = hashlib.sha256(blob).hexdigest()
        object_path(root, digest).parent.mkdir(parents=True, exist_ok=True)
        object_path(root, digest).write_bytes(blob)
        bulk.execute("insert into files (url, source, name, listed_at, state, sha256, verified)"
                     " values (?, 'ecfr-govinfo', ?, '2026-11-20T00:00:00Z', 'fetched', ?, 1)",
                     (f"https://www.govinfo.gov/ECFR-title{title}.xml", f"ECFR-title{title}.xml", digest))
    bulk.commit()
    bulk.close()
    for site, rows in CATALOGS.items():
        db = sqlite3.connect(root / "state" / f"crawl-{site}.sqlite")
        db.executescript(CRAWL_SCHEMA)
        db.executemany(
            "insert into urls (url, source, in_sitemap, first_seen_at, state, content_type, content_length, sha256,"
            " fetched_at) values (?, 'sitemap', ?, '2026-11-18T00:00:00Z', ?, ?, ?, ?, ?)",
            [(url, in_sitemap, state, content_type, length, "ab" * 32 if state == "fetched" else None,
              "2026-11-18T00:22:55Z" if state == "fetched" else None)
             for url, state, content_type, length, in_sitemap in rows])
        db.commit()
        db.close()
    return root


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory):
    root = write_data_root(tmp_path_factory.mktemp("data"))
    target = tmp_path_factory.mktemp("out") / "warehouse.duckdb"
    summary = build(root, target)
    con = duckdb.connect(str(target), read_only=True)
    yield target, summary, con
    con.close()


def one(con, sql, *params):
    return con.execute(sql, params).fetchone()


def test_the_warehouse_has_the_contract_tables_views_and_keys(warehouse):
    _, summary, con = warehouse
    for relation, columns in CONTRACT.items():
        assert [row[0] for row in con.execute(f"describe {relation}").fetchall()] == columns.split(), relation
    keys = {table: sorted(columns) for table, columns in con.execute(
        "select table_name, constraint_column_names from duckdb_constraints() where constraint_type = 'PRIMARY KEY'"
    ).fetchall() if table in KEYS}
    assert keys == {table: sorted(columns) for table, columns in KEYS.items()}
    assert summary["rows"] == {"mine": 3, "inspection": 3, "violation": 9, "accident": 9, "production_quarter": 8,
                               "cfr_section": 8, "document": 10, "dataset_version": 6}
    assert one(con, "select data_type from information_schema.columns where table_name = 'mart_mine_scorecard'"
                    " and column_name = 'top_sections_12m'") == ("VARCHAR[]",)


def test_repeated_keys_are_loaded_once_and_every_copy_is_reported(warehouse):
    _, summary, con = warehouse
    copies = con.execute("select table_name, key, copy_no, kept, source_row from duplicate_row order by all").fetchall()
    assert [row[:4] for row in copies] == [("inspection", "1000001", 1, True), ("inspection", "1000001", 2, False),
                                           ("violation", "V3", 1, True), ("violation", "V3", 2, False)]
    assert {json.loads(row[4])["docket_no"] for row in copies if row[0] == "violation"} == {"D-1", "D-2"}
    assert summary["checks"]["unique_key:violation"]["rows"] == 1
    assert "1 extra copies of 1 repeated keys" in summary["checks"]["unique_key:violation"]["detail"]
    assert "copies of 0 keys differ" in summary["checks"]["unique_key:inspection"]["detail"]
    assert summary["checks"]["unique_key:mine"] == {"rows": 0, "detail": "every key is unique"}
    assert one(con, "select count(*) from violation where violation_no = 'V3'") == (1,)


def test_rows_with_a_null_key_are_left_out_counted_and_kept_for_inspection(warehouse):
    _, summary, con = warehouse
    checks = {name: check for name, check in summary["checks"].items() if name.startswith("null_key:")}
    assert checks["null_key:violation"] == {
        "rows": 1, "detail": "rows with a null in the key (violation_no) not loaded (every one is in null_key_row)"}
    assert checks["null_key:production_quarter"]["rows"] == 1
    assert checks["null_key:mine"] == {"rows": 0, "detail": "no row has a null in the key (mine_id)"}
    assert sum(check["rows"] for check in checks.values()) == 2
    rows = {table: json.loads(row) for table, row in con.execute(
        "select table_name, source_row from null_key_row").fetchall()}
    assert (rows["violation"]["violation_no"], rows["violation"]["proposed_penalty"]) == (None, 500.0)
    assert (rows["production_quarter"]["subunit_cd"], rows["production_quarter"]["hours_worked"]) == (None, 99)
    assert one(con, "select count(*) from violation where violation_no is null or proposed_penalty = 500") == (0,)


def test_references_citations_and_coordinates_are_checked(warehouse):
    _, summary, con = warehouse
    checks = {name: check["rows"] for name, check in summary["checks"].items()}
    assert checks["reference:violation.event_no"] == 1 and checks["reference:violation.mine_id"] == 0
    assert checks["reference:production_quarter.mine_id"] == 1  # the unlisted mine
    assert checks["cited_section_not_in_title_30"] == 1  # 77.403
    assert checks["part_section_not_parsed"] == 1  # "Sec. 103(a)"
    assert checks["section_number_in_both_titles"] == 1  # 1.1
    assert summary["checks"]["section_number_in_both_titles"]["detail"].startswith("1 violations cite such a number")
    assert checks["part_not_a_number"] == 1  # 1912a.1
    assert checks["mine_coordinates_dropped"] == 1
    assert con.execute("select mine_id, latitude, longitude from mine order by 1").fetchall() == [
        (M1, 37.5, -81.2), (M2, None, None), (M3, None, None)]
    assert one(con, "select count(*) from build_check") == (len(summary["checks"]),)


def test_violations_and_accidents_get_their_derived_columns(warehouse):
    _, _, con = warehouse
    assert dict(con.execute("select violation_no, cfr_section from violation").fetchall()) == {
        "V1": "75.400", "V2": "75.400", "V3": "75.1100-3", "V4": "77.403", "V5": "56.14107", "V6": "56.14107",
        "V7": None, "V8": "1.1", "V9": None}
    assert dict(con.execute("select violation_no, sig_sub from violation where violation_no in ('V1', 'V2', 'V6')")
                .fetchall()) == {"V1": True, "V2": False, "V6": None}
    # Serious by the degree of injury alone: 01-05, whatever the day counts say.
    assert dict(con.execute("select document_no, serious from accident").fetchall()) == {
        "A1": True, "A2": True, "A3": False, "A4": True, "A5": False, "A6": True, "A7": False, "A8": False, "A9": True}
    # The rules the SQL files use are macros made from the shared definitions.
    assert one(con, "select is_fatal('01'), is_fatal('02'), is_injury('06'), is_injury('07'), is_injury(null)") == (
        True, False, True, False, False)


def test_a_mines_records_count_under_the_mines_commodity(warehouse):
    _, summary, con = warehouse
    flags = {row[0]: row[1] for row in con.execute("""
        select 'inspection ' || event_no, coal_metal from inspection
        union all select 'violation ' || violation_no, coal_metal from violation
        union all select 'accident ' || document_no, coal_metal from accident
        union all select concat_ws(' ', 'production', mine_id, cal_yr, cal_qtr), coal_metal from production_quarter
    """).fetchall()}
    assert (flags["inspection 1000003"], flags["violation V7"], flags["accident A7"]) == ("C", "M", "M")
    assert (flags[f"production {M1} 2025 3"], flags[f"production {UNLISTED} 2026 2"]) == ("C", "M")
    checks = {name: check for name, check in summary["checks"].items() if name.startswith("commodity_differs")}
    assert checks == {
        "commodity_differs_from_mine:inspection": {"rows": 1, "detail": "counted under their mine's flag: "
                                                                        "1 flagged M at C mines"},
        "commodity_differs_from_mine:violation": {"rows": 1, "detail": "counted under their mine's flag: "
                                                                       "1 flagged C at M mines"},
        "commodity_differs_from_mine:accident": {"rows": 1, "detail": "counted under their mine's flag: "
                                                                      "1 flagged C at M mines"},
        "commodity_differs_from_mine:production_quarter": {"rows": 1, "detail": "counted under their mine's flag: "
                                                                                "1 flagged M at C mines"}}


def test_cfr_sections_hold_heading_text_and_the_date_their_text_last_changed(warehouse):
    _, _, con = warehouse
    sections = {(row[0], row[1]): row[2:] for row in con.execute(
        "select title, section, part, heading, text, amended_on from cfr_section").fetchall()}
    part, heading, text, amended = sections[(30, "75.400")]
    assert (part, heading, amended) == (75, "Accumulation of combustible materials.", date(2015, 9, 2))
    assert text == "Coal dust, including float coal dust deposited on rock-dusted surfaces, shall be cleaned up."
    firefighting = "(a) All firefighting equipment shall be maintained.\n(b) It shall be examined weekly."
    assert sections[(30, "75.1100-3")][2:] == (firefighting, date(1970, 11, 20))
    assert sections[(30, "1.1")][3] == date(1974, 6, 28) and sections[(29, "1.1")][1] == "Purpose and scope."
    assert sections[(29, "1910.1201 - 1910.1202")][1:3] == ("[Reserved]", "")
    assert sections[(29, "1912a.1")][0] is None
    info = dict(con.execute("select key, value from build_info").fetchall())
    assert (info["ecfr.title30.amended"], info["ecfr.title29.amended"]) == ("2026-11-12", "2026-11-10")


def test_documents_get_a_kind_from_their_url(warehouse):
    _, _, con = warehouse
    assert [kind_of(url) for url in ("https://www.osha.gov/laws-regs/standardnumber/1910/1910.7(b)",
                                     "https://x.gov/a.PDF", "https://x.gov/a.csv", "https://x.gov/a.pps",
                                     "https://x.gov/a.mp4", "https://x.gov/page.aspx")] == [
        "page", "pdf", "data", "office", "other", "page"]
    kinds = con.execute("select site, kind, count(*) from document group by all order by all").fetchall()
    assert kinds == [("arlweb", "data", 1), ("arlweb", "other", 1), ("arlweb", "page", 1), ("msha", "page", 1),
                     ("msha", "pdf", 1), ("osha", "data", 1), ("osha", "office", 1), ("osha", "other", 1),
                     ("osha", "page", 1), ("osha", "pdf", 1)]
    assert one(con, "select content_type, fetched_at, in_sitemap from document where url = ?",
               "https://www.msha.gov/") == ("text/html", datetime(2026, 11, 18, 0, 22, 55), True)
    assert con.execute("select dataset, is_current from dataset_version where dataset = 'violations' order by built_at"
                       ).fetchall() == [("violations", False), ("violations", True)]


def test_the_marts_count_the_12_months_up_to_the_newest_violation(warehouse):
    _, _, con = warehouse
    card = {row[0]: row[1:] for row in con.execute(
        "select mine_id, inspections_12m, violations_12m, ss_violations_12m, proposed_penalty_12m, accidents_12m,"
        " serious_accidents_12m, hours_worked_12m, injury_rate_12m, top_sections_12m from mart_mine_scorecard"
    ).fetchall()}
    # Window (2025-06-30, 2026-06-30]; the injury rate pairs the four quarters 2025Q3-2026Q2 with their injuries.
    assert card[M1] == (2, 6, 2, 6450.0, 4, 3, 200_000.0, 3.0, ["75.400", "1.1", "75.1100-3", "77.403"])
    # An illness and two injuries outside the quarters M2 reported hours for: none counts in its rate.
    assert card[M2] == (0, 1, 0, 0.0, 3, 1, 10_000.0, 0.0, ["56.14107"])
    assert card[M3] == (0, 0, 0, 0.0, 0, 0, 0.0, None, [])
    # V7, flagged coal, counts under its metal mine.
    assert con.execute("select * from mart_ss_trend order by year, coal_metal").fetchall() == [
        (2024, "M", 2, 1, 0.5), (2025, "C", 1, 0, 0.0), (2026, "C", 5, 2, 0.4), (2026, "M", 1, 0, 0.0)]
    # Hours count under their mine's commodity, the unlisted mine's under its own flag. The yearly rate counts every
    # injury of a quarter with reported hours, whichever mine it happened at.
    rates = {(row[0], row[1]): row[2:] for row in con.execute("select * from mart_injury_rate").fetchall()}
    assert rates == {(2025, "C"): (0, 100_000.0, 0.0), (2026, "C"): (4, 150_000.0, pytest.approx(16 / 3)),
                     (2026, "M"): (1, 15_000.0, pytest.approx(40 / 3))}
    assert one(con, "select * from mart_district_year where district = 'C04' and year = 2026") == (
        "C04", 2026, 1, 5, 2, 5)
    assert one(con, "select * from mart_top_cited_sections where cfr_section = '75.400'") == (
        "75.400", "Accumulation of combustible materials.", 2, 1, 0.5, 5300.0, date(2026, 3, 2))


def test_a_mines_rate_counts_its_injuries_only_in_the_quarters_it_reported_hours_for(warehouse):
    _, _, con = warehouse
    # M2 reported hours only for 2026 Q1 among the rate quarters 2025Q3-2026Q2. Its two injuries fall in 2025 Q4, which
    # it has not reported, and 2026 Q2, which it reported without hours: neither quarter adds hours or injuries.
    assert one(con, "select count(*) from accident where mine_id = ? and is_injury(degree_injury_cd)"
                    " and contractor_id is null and accident_date between '2025-07-01' and '2026-06-30'", M2) == (2,)
    assert one(con, "select hours_worked_12m, injury_rate_12m from mart_mine_scorecard where mine_id = ?", M2) == (
        10_000.0, 0.0)


def test_every_named_query_states_its_question_and_runs(warehouse):
    target, _, _ = warehouse
    assert len(query_names()) >= 8
    for name in query_names():
        assert question(name).endswith("?"), name
        columns, rows = run_query(target, name)
        assert markdown(columns, rows).startswith("| " + columns[0])
    columns, rows = run_query(target, "web_corpus_by_site_kind")
    assert sum(row[columns.index("urls")] for row in rows) == 10
    assert run_query(target, "fatal_accident_types")[1] == [("POWERED HAULAGE", 1, 0, 1, 1.0)]
    assert markdown(["year", "violations", "share", "sections"], [(2026, 12345, 0.25, ["75.400", "1.1"])]).split(
        "\n") == ["| year | violations | share | sections |", "|---:|---:|---:|---|",
                  "| 2026 | 12,345 | 0.25 | 75.400, 1.1 |"]
    with pytest.raises(KeyError):
        run_query(target, "../schema")


def test_the_export_writes_mine_profiles_cited_sections_and_a_manifest(warehouse, tmp_path):
    target, _, _ = warehouse
    manifest = export(target, tmp_path / "exports")
    profiles = pl.read_parquet(tmp_path / "exports" / "mine_profiles.parquet")
    assert profiles.height == manifest["files"]["mine_profiles.parquet"]["rows"] == 3
    first = profiles.filter(pl.col("mine_id") == M1)["top_sections_12m"].to_list()[0]
    assert first[0] == {"section": "75.400", "heading": "Accumulation of combustible materials."}
    assert first[-1] == {"section": "77.403", "heading": None}
    cited = pl.read_parquet(tmp_path / "exports" / "cited_sections.parquet")
    row = cited.filter(pl.col("section") == "75.400").row(0, named=True)
    assert (row["citations"], row["citations_12m"], row["in_current_cfr"], row["amended_on"]) == (2, 2, True,
                                                                                                    date(2015, 9, 2))
    assert row["text"].startswith("Coal dust")
    assert manifest == json.loads((tmp_path / "exports" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["window_12m"] == {"after": "2025-06-30", "through": "2026-06-30"}
    assert [item["dataset"] for item in manifest["sources"]["lake"]] == sorted(
        ["accidents", "inspections", "mines", "mines_prod_quarterly", "violations"])
    assert [(item["title"], item["amended"], item["sections"]) for item in manifest["sources"]["ecfr"]] == [
        (29, "2026-11-10", 4), (30, "2026-11-12", 4)]
    assert [(item["site"], item["urls"]) for item in manifest["sources"]["crawl"]] == [
        ("arlweb", 3), ("msha", 2), ("osha", 5)]
    assert sorted(path.name for path in (tmp_path / "exports").iterdir()) == [
        "cited_sections.parquet", "manifest.json", "mine_profiles.parquet"]


def test_a_failed_build_leaves_the_previous_warehouse_in_place(tmp_path):
    root = write_data_root(tmp_path / "data")
    target = tmp_path / "out" / "warehouse.duckdb"
    build(root, target)
    current = root / "lake" / "msha_ogd" / "violations"
    parquet = current / json.loads((current / "manifest.json").read_text(encoding="utf-8"))["current"]
    saved = parquet.read_bytes()
    parquet.unlink()
    with pytest.raises(FileNotFoundError):
        build(root, target)
    assert sorted(path.name for path in target.parent.iterdir()) == ["warehouse.duckdb", "warehouse.duckdb.lock"]
    with duckdb.connect(str(target), read_only=True) as con:
        assert one(con, "select count(*) from violation") == (9,)

    parquet.write_bytes(saved)
    (root / "state" / "crawl-arlweb.sqlite").unlink()
    summary = build(root, target)
    assert summary["rows"]["document"] == 7
    assert summary["checks"]["crawl_catalog_missing"] == {"rows": 1, "detail": "no crawl catalog for arlweb"}
    assert sorted(path.name for path in target.parent.iterdir()) == ["warehouse.duckdb", "warehouse.duckdb.lock"]


def test_a_build_first_removes_what_a_killed_build_left_behind(tmp_path):
    root = write_data_root(tmp_path / "data")
    out = tmp_path / "out"
    # A build killed without unwinding (an out-of-memory kill, a host reset) leaves its half-written file and its
    # staging folder with DuckDB's spill files. The staging folder of another warehouse beside it is not its own.
    killed = out / ".warehouse.duckdb-k1113d"
    killed.mkdir(parents=True)
    (killed / "duckdb_temp_storage-0.tmp").write_bytes(b"spilled")
    (out / "warehouse.duckdb.building").write_bytes(b"half written")
    (out / ".other.duckdb-a1b2c3").mkdir()
    build(root, out / "warehouse.duckdb")
    assert sorted(path.name for path in out.iterdir()) == [
        ".other.duckdb-a1b2c3", "warehouse.duckdb", "warehouse.duckdb.lock"]


ECFR_29 = "https://www.govinfo.gov/ECFR-title29.xml"  # the URL write_data_root records for title 29


def failed_check(kind: str):
    """govinfo answering the next check of a title with one kind of failure."""
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            if kind == "robots-unreachable":
                raise httpx.ConnectError("connection refused")
            return httpx.Response(404)
        if kind == "network-error":
            raise httpx.ConnectError("connection reset")
        if kind == "failed-verification":
            return httpx.Response(200, content=TITLE_29.encode("utf-8")[:300])  # cut off mid-file
        return httpx.Response(int(kind))
    return handler


@pytest.mark.parametrize(("kind", "state"), [("503", "error"), ("network-error", "error"), ("404", "not_found"),
                                             ("robots-unreachable", "error"), ("failed-verification", "error")])
def test_after_a_failed_check_of_an_ecfr_title_the_warehouse_builds_from_the_copy_held(tmp_path, kind, state):
    root = write_data_root(tmp_path / "data")
    held = hashlib.sha256(TITLE_29.encode("utf-8")).hexdigest()
    with httpx.Client(transport=httpx.MockTransport(failed_check(kind))) as client:
        acquirer = BulkAcquirer(root, client, sleep=lambda seconds: None)
        assert acquirer.download(BulkFile("ecfr-govinfo", ECFR_29))["state"] == state
        acquirer.store.close()
    bulk = sqlite3.connect(root / "state" / "bulk.sqlite")
    try:
        assert bulk.execute("select state, sha256, verified from files where url = ?", (ECFR_29,)).fetchone() == (
            state, held, 1)
    finally:
        bulk.close()
    target = tmp_path / "out" / "warehouse.duckdb"
    assert build(root, target)["rows"]["cfr_section"] == 8
    with duckdb.connect(str(target), read_only=True) as con:
        assert one(con, "select value from build_info where key = 'ecfr.title29.sha256'") == (held,)


def test_a_second_writer_fails_at_once_while_a_build_or_export_holds_the_lock(warehouse, tmp_path):
    target, _, _ = warehouse
    with warehouse_lock(target):
        with pytest.raises(RuntimeError, match="another process is building or exporting"):
            build(tmp_path / "unused", target)
        with pytest.raises(RuntimeError, match="another process is building or exporting"):
            export(target, tmp_path / "exports")
    assert not (tmp_path / "exports").exists()
    with warehouse_lock(target):  # released by the writer that failed
        pass


def test_federal_register_dates_and_headings_are_read_from_notes():
    notes = "[61 FR 55527, Oct. 25, 1996, as amended at 80 FR 52987, Sept. 2, 2015; 1 FR 1, June 31, 2000]"
    assert fr_dates(notes) == [date(1996, 10, 25), date(2015, 9, 2)]
    assert fr_dates("39 FR 23997, June 28, 1974, unless otherwise noted.") == [date(1974, 6, 28)]
    assert section_heading("§ 75.400   Accumulation of\n combustible materials.", "§ 75.400") == (
        "Accumulation of combustible materials.")
    assert section_heading("§§ 2520.104-2--2520.104-3 [Reserved]", "§§ 2520.104-2—2520.104-3") == "[Reserved]"
    assert section_heading(None, "§ 1.1") is None
