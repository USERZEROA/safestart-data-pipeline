"""A short analysis of the lake for the docs: the share of violations that were significant and substantial (S&S) by
year and commodity, injury rates per 200,000 hours worked, and the most cited sections of 30 CFR. Each table is written
as CSV and, together, as Markdown.

Injuries, serious injuries, cited sections and the commodity (coal or metal/nonmetal) follow the rules every component
shares (`safestart_pipeline.definitions`): injuries and serious injuries by the degree of injury alone, a section as
the shared pattern's match in the citation (a citation that names no section is left out of the section table), and a
record's commodity as its mine's flag in the mines dataset, the record's own only for a mine missing there, so a mine's
violations, injuries and hours fall under one commodity.

Injuries are those of the operator's own employees: the quarterly reports hold only the operator's hours (contractors
report theirs per contractor, not per mine), so contractor injuries would inflate the rate. 200,000 hours is 100
full-time workers for a year, so the rate reads as injuries per 100 workers a year. Only quarters with production
reports count, so a partial year compares like with like.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from ..definitions import INJURY_DEGREES
from .features import cfr_section, coal_metal, quarter_of, reported_quarters, scan, serious_accident

TOP_SECTIONS = 25
PER_HOURS = 200_000


def with_coal_metal(lake: Path, dataset: str) -> pl.LazyFrame:
    """A dataset's rows with `coal_metal`, their mine's commodity by the shared rule."""
    mines = scan(lake, "mines").select("mine_id", pl.col("coal_metal_ind").alias("mine_coal_metal"))
    return scan(lake, dataset).join(mines, on="mine_id", how="left").with_columns(
        coal_metal("mine_coal_metal", "coal_metal_ind").alias("coal_metal")
    )


def ss_share(lake: Path) -> pl.LazyFrame:
    return (
        with_coal_metal(lake, "violations")
        .filter(pl.col("violation_issue_dt").dt.year() >= 2000)  # a few dozen stray earlier citations
        .group_by(pl.col("violation_issue_dt").dt.year().alias("year"), "coal_metal")
        .agg(pl.len().alias("violations"), (pl.col("sig_sub") == "Y").sum().alias("ss_violations"))
        .with_columns((pl.col("ss_violations") / pl.col("violations")).round(4).alias("ss_share"))
        .sort("year", "coal_metal")
    )


def injury_rates(lake: Path) -> pl.LazyFrame:
    _, latest = reported_quarters(lake)
    injuries = (
        with_coal_metal(lake, "accidents")
        .filter(pl.col("contractor_id").is_null(), pl.col("degree_injury_cd").is_in(INJURY_DEGREES),
                quarter_of("accident_dt") <= latest)
        .group_by(pl.col("accident_dt").dt.year().alias("year"), "coal_metal")
        .agg(pl.len().alias("injuries"), serious_accident().sum().alias("serious_injuries"))
    )
    hours = (
        with_coal_metal(lake, "mines_prod_quarterly")
        .group_by(pl.col("cal_yr").alias("year"), "coal_metal")
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
    # Penalties add up in whole cents: a float total depends on the order the threads add rows in, and rounding it to
    # dollars flipped some sections' totals by a dollar between runs.
    cents = (pl.col("proposed_penalty") * 100).round(0).cast(pl.Int64)
    return (
        scan(lake, "violations")
        .with_columns(cfr_section().alias("cfr_section"))
        .filter(pl.col("cfr_section").is_not_null())
        .group_by("cfr_section")
        .agg(
            pl.len().alias("citations"),
            (pl.col("sig_sub") == "Y").sum().alias("ss_citations"),
            pl.col("mine_id").n_unique().alias("mines"),
            ((cents.sum() + 50) // 100).alias("proposed_penalty"),
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
