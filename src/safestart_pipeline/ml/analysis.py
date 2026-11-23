"""A short analysis of the lake for the docs: the share of violations that were significant and substantial (S&S) by
year and commodity, injury rates per 200,000 hours worked, and the most cited sections of 30 CFR. Each table is written
as CSV and, together, as Markdown.

An injury is a record of degree 01 to 06 (fatal, permanent disability, days away or restricted, or no days lost) to
one of the operator's own employees: the quarterly reports hold only the operator's hours (contractors report theirs
per contractor, not per mine), so contractor injuries would inflate the rate. 200,000 hours is 100 full-time workers
for a year, so the rate reads as injuries per 100 workers a year. A serious injury is fatal or lost-time, as in the
model, and only quarters with production reports count, so a partial year compares like with like. A cited section is
the part and section without the paragraph ("56.14107(a)" is 56.14107).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from .features import cfr_section, quarter_of, reported_quarters, scan, serious_accident

INJURY_DEGREES = ("01", "02", "03", "04", "05", "06")
TOP_SECTIONS = 25
PER_HOURS = 200_000


def ss_share(lake: Path) -> pl.LazyFrame:
    return (
        scan(lake, "violations")
        .filter(pl.col("violation_issue_dt").dt.year() >= 2000)  # a few dozen stray earlier citations
        .group_by(pl.col("violation_issue_dt").dt.year().alias("year"), pl.col("coal_metal_ind").alias("coal_metal"))
        .agg(pl.len().alias("violations"), (pl.col("sig_sub") == "Y").sum().alias("ss_violations"))
        .with_columns((pl.col("ss_violations") / pl.col("violations")).round(4).alias("ss_share"))
        .sort("year", "coal_metal")
    )


def injury_rates(lake: Path) -> pl.LazyFrame:
    _, latest = reported_quarters(lake)
    injuries = (
        scan(lake, "accidents")
        .filter(pl.col("contractor_id").is_null(), pl.col("degree_injury_cd").is_in(INJURY_DEGREES),
                quarter_of("accident_dt") <= latest)
        .group_by(pl.col("accident_dt").dt.year().alias("year"), pl.col("coal_metal_ind").alias("coal_metal"))
        .agg(pl.len().alias("injuries"), serious_accident().sum().alias("serious_injuries"))
    )
    hours = (
        scan(lake, "mines_prod_quarterly")
        .group_by(pl.col("cal_yr").alias("year"), pl.col("coal_metal_ind").alias("coal_metal"))
        .agg(pl.col("hours_worked").sum())
    )
    return (
        hours.join(injuries, on=["year", "coal_metal"], how="left")
        .with_columns(pl.col("injuries", "serious_injuries").fill_null(0))
        .with_columns(
            (pl.col("injuries") * PER_HOURS / pl.col("hours_worked")).round(2).alias("injury_rate"),
            (pl.col("serious_injuries") * PER_HOURS / pl.col("hours_worked")).round(2).alias("serious_rate"),
        )
        .select("year", "coal_metal", "injuries", "serious_injuries", "hours_worked", "injury_rate", "serious_rate")
        .sort("year", "coal_metal")
    )


def top_sections(lake: Path, top: int = TOP_SECTIONS) -> pl.LazyFrame:
    return (
        scan(lake, "violations")
        .filter(pl.col("part_section").is_not_null())
        .group_by(cfr_section().alias("cfr_section"))
        .agg(
            pl.len().alias("citations"),
            (pl.col("sig_sub") == "Y").sum().alias("ss_citations"),
            pl.col("mine_id").n_unique().alias("mines"),
            pl.col("proposed_penalty").sum().round(0).cast(pl.Int64).alias("proposed_penalty"),
            pl.col("violation_issue_dt").max().alias("last_cited"),
        )
        .with_columns((pl.col("ss_citations") / pl.col("citations")).round(4).alias("ss_share"))
        .sort("citations", "cfr_section", descending=[True, False])
        .head(top)
    )


def markdown(frame: pl.DataFrame) -> str:
    with pl.Config(tbl_formatting="MARKDOWN", tbl_hide_column_data_types=True, tbl_hide_dataframe_shape=True,
                   tbl_rows=-1, tbl_cols=-1, tbl_width_chars=1000, fmt_str_lengths=200, thousands_separator=False):
        return str(frame)


def write_tables(lake: Path, out: Path) -> dict[str, int]:
    """Write each table as `<name>.csv` and all of them to tables.md; returns the rows of each."""
    tables = {
        "ss_share_by_year": ss_share(lake),
        "injury_rate_by_year": injury_rates(lake),
        "top_cited_sections": top_sections(lake),
    }
    out.mkdir(parents=True, exist_ok=True)
    sections, rows = [], {}
    for name, table in tables.items():
        frame = table.collect(engine="streaming")
        frame.write_csv(out / f"{name}.csv")
        sections.append(f"## {name}\n\n{markdown(frame)}\n")
        rows[name] = frame.height
    (out / "tables.md").write_text("\n".join(sections), encoding="utf-8")
    return rows
