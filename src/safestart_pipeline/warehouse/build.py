"""The DuckDB warehouse: the lake's current versions, the eCFR text and the crawl catalogs as one modeled database.

Why DuckDB: the warehouse answers historical questions over millions of enforcement records on a 4 GB host. DuckDB
reads the lake's Parquet in place and streams it through SQL, so the build never holds a dataset in Python memory, and
the analytical queries run in seconds without a database server. PostgreSQL keeps only the live event stream.

The build writes a new file "<warehouse>.building" and replaces the warehouse with it only after every step succeeded,
so a reader (the dashboard, an export) sees the previous warehouse or the new one, never a half-built file.

Keys are PRIMARY KEY constraints. MSHA's files repeat a few keys (a violation listed once per contest docket, an
inspection once per controller); the build loads one copy of each, chosen deterministically, and writes every copy to
duplicate_row, so a repeated key is reported rather than dropped unseen. build_check records every rule the build
verified (unique keys, references between tables, records whose own coal/metal flag is not their mine's, citations
without regulation text, dropped coordinates) with the number of rows it affected.

The derived columns follow the rules every component shares (definitions.py): an accident's `serious` is its degree of
injury, a violation's `cfr_section` is the section its citation names, and the `coal_metal` of an inspection, a
violation, an accident or a production report is its mine's flag (its own only when the mine is missing), so a mine's
records all count under one commodity. The marts and the named queries are plain SQL files, so the build stores the
rules they need as SQL macros made from the same definitions.
"""

from __future__ import annotations

import itertools
import json
import logging
import os
import tempfile
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import polars as pl

from ..acquire.bulk import ECFR_TITLES
from ..config import setting
from ..definitions import FATAL_DEGREE, SIG_SUB, cfr_section_sql, commodity_sql, injury_sql, serious_sql
from .sources import CRAWL_SITES, catalog_rows, ecfr_amended, ecfr_file, ecfr_sections

log = logging.getLogger(__name__)
HERE = Path(__file__).parent
FORMAT = 2
TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
STAGE_BATCH = 20_000
LAKE_DATASETS = ("mines", "inspections", "violations", "accidents", "mines_prod_quarterly")
TABLES = ("mine", "inspection", "violation", "accident", "production_quarter", "cfr_section", "document",
          "dataset_version")
# A mine's coordinates are kept only within the U.S. and its territories as MSHA writes them (every longitude
# negative, Guam's included); the rest (a longitude of -0.0003, a latitude of 90.99) are typing errors.
PLAUSIBLE = "TRY_CAST(latitude AS DOUBLE) BETWEEN -15 AND 72 AND TRY_CAST(longitude AS DOUBLE) BETWEEN -180 AND -60"
# MSHA's S&S designation as a boolean by the shared mapping; null where no designation applies.
SIG_SUB_SQL = "CASE sig_sub " + " ".join(f"WHEN '{code}' THEN {value}" for code, value in SIG_SUB.items()) + " END"
# Joined to a mine's records, the mine's coal/metal flag as `mine_flag` (null when the mine is missing).
MINE_FLAG = "LEFT JOIN (SELECT mine_id, coal_metal AS mine_flag FROM mine) USING (mine_id)"
# The rules the marts and the named queries apply, stored in the warehouse as macros, so plain SQL (an analyst's too)
# states none of them again.
MACROS = {"is_fatal(degree)": f"coalesce(degree = '{FATAL_DEGREE}', false)", "is_injury(degree)": injury_sql("degree")}
SECTION_SCHEMA = {"section": pl.String, "title": pl.Int32, "part": pl.Int32, "heading": pl.String, "text": pl.String,
                  "amended_on": pl.Date}
DOCUMENT_SCHEMA = {"url": pl.String, "site": pl.String, "state": pl.String, "kind": pl.String,
                   "content_type": pl.String, "bytes": pl.Int64, "sha256": pl.String, "fetched_at": pl.String,
                   "in_sitemap": pl.Boolean}


@dataclass(frozen=True)
class Load:
    """How one table is filled: its source relation, its key (named alike in source and table), the SELECT list that
    turns a source row into a table row, and, for a mine's records, the source column with the record's own coal/metal
    flag (the table's `coal_metal` is the mine's flag, this one only when the mine is missing)."""

    table: str
    source: str
    key: tuple[str, ...]
    columns: str
    own_flag: str | None = None


LOADS = (
    Load("mine", "src_mines", ("mine_id",), f"""
        mine_id, current_mine_name AS mine_name, coal_metal_ind AS coal_metal, current_mine_type AS mine_type,
        current_mine_status AS status, current_status_dt AS status_date, state, fips_cnty_nm AS county, district,
        primary_sic, primary_canvass,
        CASE WHEN {PLAUSIBLE} THEN TRY_CAST(latitude AS DOUBLE) END AS latitude,
        CASE WHEN {PLAUSIBLE} THEN TRY_CAST(longitude AS DOUBLE) END AS longitude,
        current_controller_id AS controller_id, current_controller_name AS controller_name,
        current_operator_id AS operator_id, current_operator_name AS operator_name"""),
    Load("inspection", "src_inspections", ("event_no",), """
        event_no, mine_id, inspection_begin_dt AS begin_date, inspection_end_dt AS end_date, activity_code, activity""",
         own_flag="coal_metal_ind"),
    Load("violation", "src_violations", ("violation_no",), f"""
        violation_no, event_no, mine_id, violation_issue_dt AS issue_date, violation_occur_dt AS occur_date,
        part_section, {cfr_section_sql("part_section")} AS cfr_section, {SIG_SUB_SQL} AS sig_sub, likelihood,
        inj_illness AS injury_illness, negligence, proposed_penalty, amount_paid, termination_dt AS termination_date,
        mine_type""", own_flag="coal_metal_ind"),
    Load("accident", "src_accidents", ("document_no",), f"""
        document_no, mine_id, accident_dt AS accident_date, degree_injury_cd, degree_injury,
        {serious_sql("degree_injury_cd")} AS serious, classification, accident_type,
        no_injuries::INTEGER AS no_injuries, days_lost::INTEGER AS days_lost, days_restrict::INTEGER AS days_restrict,
        subunit, narrative, contractor_id""", own_flag="coal_metal_ind"),
    Load("production_quarter", "src_mines_prod_quarterly", ("mine_id", "cal_yr", "cal_qtr", "subunit_cd"), """
        mine_id, cal_yr::INTEGER AS cal_yr, cal_qtr::INTEGER AS cal_qtr, subunit_cd,
        avg_employee_cnt::DOUBLE AS avg_employees, hours_worked::DOUBLE AS hours_worked,
        coal_production::DOUBLE AS coal_production""", own_flag="coal_metal_ind"),
    Load("cfr_section", "src_cfr_section", ("title", "section"), "section, title, part, heading, text, amended_on"),
    Load("document", "src_document", ("url",), f"""
        url, site, state, kind, content_type, bytes, sha256,
        try_strptime(fetched_at, '{TIME_FORMAT}') AS fetched_at, in_sitemap"""),
)
# References between tables: (child, column, parent, parent column).
REFERENCES = (
    ("violation", "event_no", "inspection", "event_no"),
    ("violation", "mine_id", "mine", "mine_id"),
    ("inspection", "mine_id", "mine", "mine_id"),
    ("accident", "mine_id", "mine", "mine_id"),
    ("production_quarter", "mine_id", "mine", "mine_id"),
)
# Up to five distinct values of a relation, as text: the examples a check's detail names.
EXAMPLES = ("(SELECT string_agg(value, ', ' ORDER BY value) FROM (SELECT DISTINCT {value} AS value FROM {rows} "
            "ORDER BY value LIMIT 5))")
# Rules checked after loading: name -> (table, SQL returning the rows affected and a detail, detail when none).
CHECKS = {
    "cited_section_not_in_title_30": ("violation", """
        WITH unmatched AS (
            SELECT v.cfr_section FROM violation v ANTI JOIN cfr_section s ON s.title = 30 AND s.section = v.cfr_section
            WHERE v.cfr_section IS NOT NULL)
        SELECT count(*), concat(count(DISTINCT cfr_section), ' section numbers without text in title 30; most cited: ',
            (SELECT string_agg(cfr_section || ' (' || n || ')', ', ' ORDER BY n DESC, cfr_section)
             FROM (SELECT cfr_section, count(*) AS n FROM unmatched GROUP BY ALL ORDER BY n DESC, cfr_section LIMIT 5)))
        FROM unmatched""", "every cited section is in title 30"),
    "part_section_not_parsed": ("violation", """
        WITH unparsed AS (SELECT part_section FROM violation WHERE part_section IS NOT NULL AND cfr_section IS NULL)
        SELECT count(*), concat('e.g. ', """ + EXAMPLES.format(value="part_section", rows="unparsed") + """)
        FROM unparsed""", "every part_section starts with a section number"),
    "section_number_in_both_titles": ("cfr_section", """
        WITH shared AS (SELECT section FROM cfr_section GROUP BY section HAVING count(*) > 1)
        SELECT count(*), concat(
            (SELECT count(*) FROM violation v SEMI JOIN shared s ON s.section = v.cfr_section),
            ' violations cite such a number (MSHA cites title 30); e.g. ',
            """ + EXAMPLES.format(value="section", rows="shared") + """)
        FROM shared""", "no section number is in both titles"),
    "part_not_a_number": ("cfr_section", """
        WITH lettered AS (SELECT section FROM cfr_section WHERE part IS NULL)
        SELECT count(*), concat('sections of lettered parts (part is NULL); e.g. ',
            """ + EXAMPLES.format(value="section", rows="lettered") + """)
        FROM lettered""", "every part is a number"),
    "mine_coordinates_dropped": ("mine", f"""
        SELECT count(*) FILTER (WHERE NOT coalesce({PLAUSIBLE}, false)),
            concat('of ', count(*), ' mines with coordinates; kept only within the U.S. and its territories')
        FROM src_mines WHERE TRY_CAST(latitude AS DOUBLE) IS NOT NULL OR TRY_CAST(longitude AS DOUBLE) IS NOT NULL""",
        "every coordinate is within the U.S. and its territories"),
}


def sql_text(value: object) -> str:
    """A SQL string literal (paths as forward-slash text)."""
    text = value.as_posix() if isinstance(value, Path) else str(value)
    return "'" + text.replace("'", "''") + "'"


def one(con: duckdb.DuckDBPyConnection, sql: str) -> tuple:
    """The single row a query returns, read to its end (DuckDB 2 refuses other work while a result is still open)."""
    return con.execute(sql).fetchall()[0]


def utc_now() -> str:
    return datetime.now(UTC).strftime(TIME_FORMAT)


def read_manifest(folder: Path) -> dict:
    path = folder / "manifest.json"
    if not path.exists():
        raise FileNotFoundError(f"no lake manifest at {path}; run `ssp lake` first")
    return json.loads(path.read_text(encoding="utf-8"))


def current_parquet(lake: Path, dataset: str) -> Path:
    """The Parquet file of a dataset's current version."""
    path = lake / dataset / read_manifest(lake / dataset)["current"]
    if not path.exists():
        raise FileNotFoundError(path)
    return path


def dataset_versions(lake: Path) -> list[tuple]:
    """(dataset, version, rows, built_at, is_current) for every version in every lake manifest; a version is named
    after its Parquet file (the first 16 hex digits of the source file's SHA-256)."""
    rows = []
    for path in sorted(lake.glob("*/manifest.json")):
        manifest = json.loads(path.read_text(encoding="utf-8"))
        for version in manifest["versions"]:
            built = datetime.strptime(version["built_at"], TIME_FORMAT) if version.get("built_at") else None
            rows.append((manifest["dataset"], Path(version["parquet"]).stem, version.get("rows"), built,
                         version["parquet"] == manifest.get("current")))
    return rows


def stage(rows: Iterable[tuple], schema: dict[str, pl.DataType], folder: Path, name: str) -> int:
    """Write rows as Parquet parts <folder>/<name>-<n>.parquet, a batch at a time (at least one, possibly empty, part,
    so the parts can always be read). Returns the row count."""
    count = 0
    for number, batch in enumerate(itertools.batched(rows, STAGE_BATCH)):
        pl.DataFrame(batch, schema=schema, orient="row").write_parquet(folder / f"{name}-{number:05d}.parquet")
        count += len(batch)
    if not count:
        pl.DataFrame(schema=schema).write_parquet(folder / f"{name}-00000.parquet")
    return count


def table_rows(spec: Load, relation: str) -> str:
    """The SELECT that turns rows with the source's columns into the table's rows; a mine's records get the commodity
    of definitions.commodity_sql, the mine's flag before their own."""
    if spec.own_flag is None:
        return f"SELECT {spec.columns} FROM {relation}"
    flag = commodity_sql("mine_flag", spec.own_flag)
    return f"SELECT {spec.columns}, {flag} AS coal_metal FROM {relation} {MINE_FLAG}"


def load(con: duckdb.DuckDBPyConnection, spec: Load) -> list[dict]:
    """Fill one table with one row per key. Every copy of a repeated key goes to duplicate_row; the first copy in the
    order of its full source row (as JSON) is loaded. Returns the table's checks: its unique key and, for a mine's
    records, those whose own coal/metal flag is not their mine's."""
    key = ", ".join(spec.key)
    con.execute(f"CREATE OR REPLACE TEMP TABLE dup_keys AS SELECT {key} FROM {spec.source} GROUP BY ALL "
                "HAVING count(*) > 1")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE dup_rows AS
        SELECT s.*, row_number() OVER (PARTITION BY {key} ORDER BY to_json(s)::VARCHAR) AS copy_no,
               to_json(s)::VARCHAR AS source_row
        FROM {spec.source} s SEMI JOIN dup_keys USING ({key})""")
    con.execute(f"INSERT INTO duplicate_row SELECT {sql_text(spec.table)}, concat_ws('|', {key}), copy_no, "
                "copy_no = 1, source_row FROM dup_rows")
    # The source rows loaded: every row of a unique key and the first copy of a repeated one.
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW kept AS
        SELECT * FROM {spec.source} ANTI JOIN dup_keys USING ({key})
        UNION ALL BY NAME SELECT * EXCLUDE (copy_no, source_row) FROM dup_rows WHERE copy_no = 1""")
    con.execute(f"INSERT INTO {spec.table} BY NAME {table_rows(spec, 'kept')}")
    keys, copies = one(con, "SELECT count(*) FILTER (WHERE copy_no = 1), count(*) FILTER (WHERE copy_no > 1) "
                            "FROM dup_rows")
    differing, = one(con, f"SELECT count(*) FROM (SELECT {key} FROM (SELECT DISTINCT * FROM "
                          f"({table_rows(spec, 'dup_rows')})) GROUP BY ALL HAVING count(*) > 1)")
    detail = (f"{copies} extra copies of {keys} repeated keys not loaded (every copy is in duplicate_row); the copies "
              f"of {differing} keys differ in this table's columns") if keys else "every key is unique"
    checks = [{"name": f"unique_key:{spec.table}", "table_name": spec.table, "rows": keys, "detail": detail}]
    if spec.own_flag:
        checks.append(commodity_check(con, spec))
    return checks


def commodity_check(con: duckdb.DuckDBPyConnection, spec: Load) -> dict:
    """The loaded records whose own coal/metal flag is not their mine's (they count under the mine's), by flag."""
    rows, groups = one(con, f"""
        SELECT coalesce(sum(n), 0)::BIGINT,
               string_agg(concat(n, ' flagged ', own, ' at ', mine_flag, ' mines'), ', ' ORDER BY own)
        FROM (SELECT {spec.own_flag} AS own, mine_flag, count(*) AS n FROM kept {MINE_FLAG}
              WHERE {commodity_sql("mine_flag", spec.own_flag)} <> {spec.own_flag} GROUP BY ALL)""")
    detail = f"counted under their mine's flag: {groups}" if rows else "every record's own flag is its mine's"
    return {"name": f"commodity_differs_from_mine:{spec.table}", "table_name": spec.table, "rows": rows,
            "detail": detail}


def reference_check(con: duckdb.DuckDBPyConnection, child: str, column: str, parent: str, parent_column: str) -> dict:
    rows, examples = one(con, f"""
        WITH missing AS (
            SELECT c.{column} AS ref FROM {child} c ANTI JOIN {parent} p ON p.{parent_column} = c.{column}
            WHERE c.{column} IS NOT NULL)
        SELECT count(*), {EXAMPLES.format(value="ref", rows="missing")} FROM missing""")
    detail = f"rows whose {column} is not in {parent}; e.g. {examples}" if rows else f"every {column} is in {parent}"
    return {"name": f"reference:{child}.{column}", "table_name": child, "rows": rows, "detail": detail}


def stage_sources(con: duckdb.DuckDBPyConnection, data_root: Path, staging: Path) -> dict[str, str]:
    """Source views over the lake's current Parquet, and over the eCFR sections and crawl catalogs staged as Parquet.
    Returns the build information about the sources."""
    lake = data_root / "lake" / "msha_ogd"
    for dataset in LAKE_DATASETS:
        parquet = sql_text(current_parquet(lake, dataset))
        con.execute(f"CREATE TEMP VIEW src_{dataset} AS SELECT * FROM read_parquet({parquet})")
    info: dict[str, str] = {}
    titles = {}
    for title in ECFR_TITLES:
        path, digest = ecfr_file(data_root, title)
        titles[title] = path
        info |= {f"ecfr.title{title}.sha256": digest, f"ecfr.title{title}.amended": ecfr_amended(path) or ""}
    sections = stage(itertools.chain.from_iterable(ecfr_sections(path, title) for title, path in titles.items()),
                     SECTION_SCHEMA, staging, "cfr_section")
    log.info("staged %s eCFR sections", sections)
    catalogs = {site: data_root / "state" / f"crawl-{site}.sqlite" for site in CRAWL_SITES}
    found = [site for site, path in catalogs.items() if path.exists()]
    urls = stage(itertools.chain.from_iterable(catalog_rows(catalogs[site], site) for site in found), DOCUMENT_SCHEMA,
                 staging, "document")
    info["crawl.catalogs"] = ", ".join(found)
    info["crawl.missing"] = ", ".join(site for site in CRAWL_SITES if site not in found)
    log.info("staged %s URLs from the crawl catalogs of %s", urls, info["crawl.catalogs"] or "no site")
    for name in ("cfr_section", "document"):
        parts = sql_text(staging / f"{name}-*.parquet")
        con.execute(f"CREATE TEMP VIEW src_{name} AS SELECT * FROM read_parquet({parts})")
    return info


def fill(con: duckdb.DuckDBPyConnection, data_root: Path, staging: Path) -> dict:
    """Create and fill every table, check them, and create the macros and the marts."""
    con.execute((HERE / "schema.sql").read_text(encoding="utf-8"))
    info = {"built_at": utc_now(), "format": str(FORMAT)} | stage_sources(con, data_root, staging)
    checks = []
    for spec in LOADS:
        started = time.monotonic()
        checks += load(con, spec)
        log.info("loaded %s in %.1f s", spec.table, time.monotonic() - started)
    con.executemany("INSERT INTO dataset_version VALUES (?, ?, ?, ?, ?)",
                    dataset_versions(data_root / "lake" / "msha_ogd"))
    con.executemany("INSERT INTO build_info VALUES (?, ?)", sorted(info.items()))
    checks += [reference_check(con, *reference) for reference in REFERENCES]
    for name, (table, sql, none) in CHECKS.items():
        rows, detail = one(con, sql)
        checks.append({"name": name, "table_name": table, "rows": rows, "detail": detail if rows else none})
    if missing := [site for site in info["crawl.missing"].split(", ") if site]:
        checks.append({"name": "crawl_catalog_missing", "table_name": "document", "rows": len(missing),
                       "detail": f"no crawl catalog for {info['crawl.missing']}"})
    con.executemany("INSERT INTO build_check VALUES (?, ?, ?, ?)",
                    [(check["name"], check["table_name"], check["rows"], check["detail"]) for check in checks])
    for signature, body in MACROS.items():
        con.execute(f"CREATE MACRO {signature} AS {body}")
    con.execute((HERE / "marts.sql").read_text(encoding="utf-8"))
    counts = {table: one(con, f"SELECT count(*) FROM {table}")[0] for table in TABLES}
    return {"built_at": info["built_at"], "rows": counts,
            "checks": {check["name"]: {"rows": check["rows"], "detail": check["detail"]} for check in checks}}


def discard(path: Path) -> None:
    for item in (path, path.with_name(path.name + ".wal")):
        item.unlink(missing_ok=True)


def build(data_root: Path, target: Path) -> dict:
    """Build the warehouse from the data root's lake, bulk store and crawl catalogs (all read only) into `target`,
    replacing it atomically."""
    building = target.with_name(target.name + ".building")
    target.parent.mkdir(parents=True, exist_ok=True)
    discard(building)
    started = time.monotonic()
    try:
        # The connection closes before the staging folder (DuckDB's spill space too) is removed.
        with (tempfile.TemporaryDirectory(prefix=f".{target.name}-", dir=target.parent) as staging,
              duckdb.connect(str(building)) as con):
            con.execute(f"SET memory_limit = {sql_text(setting('warehouse_memory_limit', '1GB'))}")
            con.execute("SET preserve_insertion_order = false")
            con.execute("SET enable_progress_bar = false")
            con.execute(f"SET temp_directory = {sql_text(Path(staging))}")
            summary = fill(con, data_root, Path(staging))
            con.execute("CHECKPOINT")
    except BaseException:
        discard(building)
        raise
    try:
        os.replace(building, target)
    except PermissionError as problem:
        raise RuntimeError(f"{target} is open in another process; the new warehouse is at {building}") from problem
    log.info("warehouse built in %.0f s: %s", time.monotonic() - started, target)
    return {"warehouse": str(target), "seconds": round(time.monotonic() - started, 1)} | summary


def connect(path: Path) -> duckdb.DuckDBPyConnection:
    """A read-only connection to a built warehouse."""
    if not path.exists():
        raise FileNotFoundError(f"no warehouse at {path}; run `python -m safestart_pipeline.warehouse build` first")
    return duckdb.connect(str(path), read_only=True)
