"""SafeStart exports: the warehouse facts SafeStart's pre-shift assessment (JSA) and its regulation library use.

SafeStart is a separate application and should not need DuckDB or the warehouse's model, so the export gives it two
flat Parquet files and a manifest saying exactly which data they come from (docs/SAFESTART_EXPORTS.md is the contract):

- mine_profiles.parquet: the mine scorecard, one row per mine, with the mine's most cited sections of the last 12
  months and their headings.
- cited_sections.parquet: every section MSHA has cited, with its counts and, when today's Title 30 holds it, its
  heading, text and the date its text last changed.
- manifest.json: row counts, the source versions (lake datasets, eCFR editions, crawl catalogs) and the build time.

Each file is written under a temporary name and moved into place, and the manifest is written last, so a manifest
always describes complete files.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import duckdb

from ..acquire.bulk import ECFR_TITLES
from ..verify import sha256_of
from .build import LAKE_DATASETS, TIME_FORMAT, connect, one, sql_text, utc_now

FORMAT = 1
MINE_PROFILES = """
WITH scorecard AS MATERIALIZED (
    SELECT * FROM mart_mine_scorecard
), ranked AS (
    SELECT mine_id, unnest(list_transform(top_sections_12m, lambda section, rank: {'section': section, 'rank': rank}))
        AS top
    FROM scorecard
), named AS (
    SELECT r.mine_id, list({'section': r.top.section, 'heading': c.heading} ORDER BY r.top.rank) AS sections
    FROM ranked r
    LEFT JOIN cfr_section c ON c.title = 30 AND c.section = r.top.section
    GROUP BY r.mine_id
)
SELECT s.* REPLACE (coalesce(n.sections, []::STRUCT(section VARCHAR, heading VARCHAR)[]) AS top_sections_12m)
FROM scorecard s
LEFT JOIN named n ON n.mine_id = s.mine_id
ORDER BY s.mine_id
"""
CITED_SECTIONS = """
WITH recent AS (
    SELECT v.cfr_section, count(*) AS citations_12m
    FROM violation v, (SELECT max(issue_date) - INTERVAL 12 MONTH AS window_start FROM violation) b
    WHERE v.issue_date > b.window_start AND v.cfr_section IS NOT NULL
    GROUP BY v.cfr_section
)
SELECT
    t.cfr_section AS section, c.part, t.heading, c.text, c.amended_on, c.section IS NOT NULL AS in_current_cfr,
    t.citations, coalesce(r.citations_12m, 0) AS citations_12m, t.ss_citations, t.ss_share,
    t.proposed_penalty_total, t.last_cited
FROM mart_top_cited_sections t
LEFT JOIN cfr_section c ON c.title = 30 AND c.section = t.cfr_section
LEFT JOIN recent r ON r.cfr_section = t.cfr_section
ORDER BY t.citations DESC, t.cfr_section
"""
FILES = {"mine_profiles.parquet": MINE_PROFILES, "cited_sections.parquet": CITED_SECTIONS}


def records(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict]:
    cursor = con.execute(sql)
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def sources(con: duckdb.DuckDBPyConnection) -> dict:
    """The versions of every source the warehouse was built from."""
    info = dict(con.execute("SELECT key, value FROM build_info").fetchall())
    sections = dict(con.execute("SELECT title, count(*) FROM cfr_section GROUP BY title").fetchall())
    return {
        "lake": records(con, f"""
            SELECT dataset, version, rows, strftime(built_at, '{TIME_FORMAT}') AS built_at
            FROM dataset_version WHERE is_current AND dataset IN ({', '.join(map(sql_text, LAKE_DATASETS))})
            ORDER BY dataset"""),
        "ecfr": [{"title": title, "sha256": info.get(f"ecfr.title{title}.sha256"),
                  "amended": info.get(f"ecfr.title{title}.amended") or None, "sections": sections.get(title, 0)}
                 for title in ECFR_TITLES],
        "crawl": records(con, f"""
            SELECT site, count(*) AS urls, count(*) FILTER (WHERE state IN ('fetched', 'unchanged')) AS downloaded,
                   strftime(max(fetched_at), '{TIME_FORMAT}') AS last_fetched_at
            FROM document GROUP BY site ORDER BY site"""),
    }


def export(warehouse: Path, out_dir: Path) -> dict:
    """Write the SafeStart exports from a built warehouse into `out_dir`; returns the manifest."""
    out_dir.mkdir(parents=True, exist_ok=True)
    files = {}
    with connect(warehouse) as con:
        for name, sql in FILES.items():
            partial = out_dir / f".{name}.partial"
            rows, = one(con, f"COPY ({sql}) TO {sql_text(partial)} (FORMAT parquet, COMPRESSION zstd)")
            columns = con.execute(f"DESCRIBE SELECT * FROM read_parquet({sql_text(partial)})").fetchall()
            os.replace(partial, out_dir / name)
            files[name] = {"rows": rows, "sha256": sha256_of(out_dir / name),
                           "columns": {column[0]: column[1] for column in columns}}
        start, end = one(con, "SELECT max(issue_date) - INTERVAL 12 MONTH, max(issue_date) FROM violation")
        built_at, = one(con, "SELECT value FROM build_info WHERE key = 'built_at'")
        manifest = {"format": FORMAT, "built_at": built_at, "exported_at": utc_now(),
                    "window_12m": {"after": start.date().isoformat() if start else None,
                                   "through": end.isoformat() if end else None},
                    "files": files, "sources": sources(con)}
    partial = out_dir / ".manifest.json.partial"
    partial.write_text(json.dumps(manifest, indent=1), encoding="utf-8")
    os.replace(partial, out_dir / "manifest.json")
    return manifest
