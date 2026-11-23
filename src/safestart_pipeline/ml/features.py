"""A mine-quarter panel for the accident-risk model, built lazily from the lake's current versions.

Each row is one mine in one calendar quarter `t`. Its features summarize only the four quarters before it (t-4 to t-1):
inspections, violations and how many were significant and substantial (S&S), proposed penalties, distinct sections
cited, accidents and serious accidents, hours worked and average employees. Its target is whether the mine had a
serious accident in `t` itself. A source row of quarter `s` is spread to the target quarters s+1 to s+4 and never to
`s`, so no feature can see the quarter it predicts.

Every feature is dated before `t`: inspections, violations and accidents count by the day they happened, hours worked
by the quarter worked, and a penalty by the quarter MSHA billed it (a median of 49 days after the citation in
2015-2024), since a penalty not yet proposed was not known. The values, though, are the lake's current record, not
what MSHA held when a score for `t` is made, 15 days into `t` once the quarterly reports of hours are due (30 CFR
50.30): reports filed late, accidents entered after the fact and degrees of injury revised later are all in the
history. A score made at the time sees less, so the test metrics overstate its quality somewhat (docs/ML.md).

The population is every mine that reported hours worked in at least one of the four prior quarters, so it depends on
the past alone, and the model is trained on the population it later scores. The coal/metal flag, commodity, mine
type, state and district are MSHA's current values for the mine; its status is left out because it records the
future (a mine abandoned after an accident).

Serious accidents, S&S, cited sections and the coal/metal flag follow the rules every component shares
(`safestart_pipeline.definitions`): an accident is serious by its degree of injury alone, a violation is S&S by the
SIG_SUB mapping of its flag, a cited section is what the shared pattern matches in the citation, and a mine's flag is
the mines dataset's. A mine missing from that dataset takes the flag of its newest production report, the records that
put it in the population. As in the warehouse, every record counts once: `scan` keeps one copy of each key.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from ..definitions import CFR_SECTION_PATTERN, SERIOUS_DEGREES, SIG_SUB, commodity_sql

DATASETS = ("mines", "inspections", "violations", "accidents", "mines_prod_quarterly")  # what the panel reads
# The datasets whose exports repeat keys, each with the key the warehouse loads it by (`scan` keeps one copy of each).
# The warehouse build checks every table's key (`build_check`), so a repeat in another dataset would show there.
REPEATED_KEYS = {"inspections": "event_no", "violations": "violation_no"}
WINDOW = 4  # quarters of history behind each target quarter
COUNTS = ("inspections", "violations", "ss_violations", "accidents", "serious_accidents")
AMOUNTS = ("proposed_penalty", "hours_worked", "avg_employees")
NUMERIC = (
    "inspections_4q", "violations_4q", "ss_violations_4q", "proposed_penalty_4q", "cited_sections_4q", "accidents_4q",
    "serious_accidents_4q", "hours_worked_4q", "avg_employees_4q", "hours_worked_1q", "active_quarters_4q",
)
REAL = ("proposed_penalty_4q", "hours_worked_4q", "avg_employees_4q", "hours_worked_1q")  # the others are counts
# The target quarter's year, known in advance: a trend term, since the share of mine-quarters with a serious accident
# fell from 9 percent in 2001 to 4 percent in the 2020s and a model without it overestimates later quarters.
TREND = ("year",)
CATEGORICAL = ("coal_metal", "commodity", "mine_type", "state", "district", "calendar_quarter")
FEATURES = NUMERIC + TREND + CATEGORICAL
TARGET = "serious_accident"
KEY = ["mine_id", "quarter"]


def current_parquet(lake: Path, dataset: str) -> Path:
    """The Parquet file of a dataset's current version, as its manifest names it."""
    manifest = json.loads((lake / dataset / "manifest.json").read_text(encoding="utf-8"))
    return lake / dataset / manifest["current"]


def scan(lake: Path, dataset: str) -> pl.LazyFrame:
    """A dataset's current version, one copy of each key. MSHA's inspections and violations repeat a few keys, with
    copies that differ only in columns read nowhere here (in November 2026, 51 violations listed once per contest
    docket and one inspection once per controller); the warehouse loads one copy of each, and so does every read here,
    or the model and the analysis would count those records twice. The repeated keys are found first, in a pass over
    the key alone, because deduplicating every row at once raised the panel's peak memory by about a quarter. Every row
    of another key, or of none, passes through; of a repeated key the copy kept is the first in the order of all its
    columns, nulls last, so the choice depends neither on the file's order nor on the reader."""
    rows = pl.scan_parquet(current_parquet(lake, dataset))
    if dataset not in REPEATED_KEYS:
        return rows
    key = REPEATED_KEYS[dataset]
    repeated = rows.group_by(key).len().filter(pl.col("len") > 1).select(key).collect(engine="streaming").lazy()
    kept = rows.join(repeated, on=key, how="semi").sort(pl.all(), nulls_last=True).unique(subset=key, keep="first")
    return pl.concat([rows.join(repeated, on=key, how="anti"), kept.collect(engine="streaming").lazy()])


def quarter_of(column: str) -> pl.Expr:
    """A date's calendar quarter as one integer (year * 4 + quarter - 1), so quarters subtract across years."""
    return (pl.col(column).dt.year() * 4 + pl.col(column).dt.quarter() - 1).cast(pl.Int32)


def report_quarter() -> pl.Expr:
    """The quarter of a quarterly production report."""
    return (pl.col("cal_yr") * 4 + pl.col("cal_qtr") - 1).cast(pl.Int32)


def quarter_label(quarter: int) -> str:
    return f"{quarter // 4}Q{quarter % 4 + 1}"


def period(column: str = "quarter") -> pl.Expr:
    """`quarter_label` of a column: "2026Q4"."""
    return pl.format("{}Q{}", pl.col(column) // 4, pl.col(column) % 4 + 1)


def serious_accident() -> pl.Expr:
    """An accident whose degree of injury is one of the shared SERIOUS_DEGREES; the day counts play no part."""
    return pl.col("degree_injury_cd").is_in(SERIOUS_DEGREES).fill_null(False)


def ss_violation() -> pl.Expr:
    """A violation's S&S designation by the shared SIG_SUB mapping: true, false, or null where none applies, so that a
    sum counts the S&S violations."""
    return pl.col("sig_sub").replace_strict(SIG_SUB, default=None, return_dtype=pl.Boolean)


def cfr_section(column: str = "part_section") -> pl.Expr:
    """The section a citation names, by the shared CFR_SECTION_PATTERN, or null when it names none: "56.14107(a)" is
    56.14107, "75.1100-2(e)(2)" is 75.1100-2 and "104(d)(1)" has none."""
    return pl.col(column).str.strip_chars().str.extract(CFR_SECTION_PATTERN, 0)


def coal_metal(mine_flag: str, own_flag: str) -> pl.Expr:
    """The shared commodity rule as a Polars expression: the mine's coal/metal flag, else the record's own. It is the
    rule's SQL form (`commodity_sql`), which Polars' SQL reads as well as DuckDB, so the rule stays stated once."""
    return pl.sql_expr(commodity_sql(mine_flag, own_flag))


def spread(frame: pl.LazyFrame) -> pl.LazyFrame:
    """Each row of source quarter `quarter` once for each of the WINDOW target quarters after it, with `lag` (1 to
    WINDOW) the distance back from that target. A row never reaches its own quarter."""
    return (
        frame.with_columns(pl.int_ranges(1, WINDOW + 1, dtype=pl.Int32).alias("lag"))
        .explode("lag")
        .with_columns(pl.col("quarter") + pl.col("lag"))
    )


def quarterly_activity(lake: Path) -> pl.LazyFrame:
    """Per mine and quarter: inspections begun, violations issued (and S&S ones), penalties billed, accidents (and
    serious ones), hours worked and average employees, zero where a source has no row."""
    violations = scan(lake, "violations")
    frames = [
        scan(lake, "inspections").group_by("mine_id", quarter_of("inspection_begin_dt").alias("quarter"))
        .agg(pl.len().alias("inspections")),
        violations.group_by("mine_id", quarter_of("violation_issue_dt").alias("quarter"))
        .agg(pl.len().alias("violations"), ss_violation().sum().alias("ss_violations")),
        violations.group_by("mine_id", quarter_of("bill_print_dt").alias("quarter"))
        .agg(pl.col("proposed_penalty").sum()),
        scan(lake, "accidents").group_by("mine_id", quarter_of("accident_dt").alias("quarter"))
        .agg(pl.len().alias("accidents"), serious_accident().sum().alias("serious_accidents")),
        scan(lake, "mines_prod_quarterly").group_by("mine_id", report_quarter().alias("quarter"))
        .agg(pl.col("hours_worked").sum(), pl.col("avg_employee_cnt").sum().alias("avg_employees")),
    ]
    return (
        pl.concat(frames, how="diagonal")
        .filter(pl.col("quarter").is_not_null())
        .group_by(KEY)
        .agg(pl.all().sum())
        .with_columns(pl.col(*COUNTS).cast(pl.Int32), pl.col(*AMOUNTS).cast(pl.Float64))
    )


def history(quarterly: pl.LazyFrame) -> pl.LazyFrame:
    """The window features of every target quarter that any source quarter reaches."""
    return spread(quarterly).group_by(KEY).agg(
        *[pl.col(name).sum().alias(f"{name}_4q") for name in (*COUNTS, "proposed_penalty", "hours_worked")],
        (pl.col("avg_employees").sum() / WINDOW).alias("avg_employees_4q"),
        pl.col("hours_worked").filter(pl.col("lag") == 1).sum().alias("hours_worked_1q"),
        (pl.col("hours_worked") > 0).sum().cast(pl.Int32).alias("active_quarters_4q"),
    )


def cited_sections(lake: Path) -> pl.LazyFrame:
    """Distinct sections cited at each mine in each target quarter's window (a section cited in several of the four
    quarters counts once)."""
    cited = (
        scan(lake, "violations")
        .select("mine_id", quarter_of("violation_issue_dt").alias("quarter"), cfr_section().alias("section"))
        .drop_nulls()
        .unique()
    )
    return spread(cited).group_by(KEY).agg(pl.col("section").n_unique().cast(pl.Int32).alias("cited_sections_4q"))


def reported_quarters(lake: Path) -> tuple[int, int]:
    """The first and the newest quarter with hours worked in the operators' quarterly reports."""
    bounds = (
        scan(lake, "mines_prod_quarterly")
        .filter(pl.col("hours_worked") > 0)
        .select(report_quarter().min().alias("first"), report_quarter().max().alias("latest"))
        .collect()
    )
    return bounds.item(0, "first"), bounds.item(0, "latest")


def mine_attributes(lake: Path) -> pl.LazyFrame:
    """MSHA's current values for every mine with production reports (the panel's mines): the coal/metal flag by the
    shared rule, with the newest report's own flag standing in for a mine missing from the mines dataset."""
    reported = scan(lake, "mines_prod_quarterly").group_by("mine_id").agg(
        pl.col("coal_metal_ind").sort_by(report_quarter(), "subunit_cd").last().alias("reported_flag")
    )
    return reported.join(scan(lake, "mines"), on="mine_id", how="left").select(
        "mine_id",
        coal_metal("coal_metal_ind", "reported_flag").alias("coal_metal"),
        pl.col("primary_canvass").alias("commodity"),
        pl.col("current_mine_type").alias("mine_type"),
        "state",
        "district",
    )


def mine_details(lake: Path) -> pl.LazyFrame:
    """For the risk scores, not features: each reporting mine's name and current status (the status records the
    future), and whether its report for the newest quarter is missing, not filed yet or no longer filed, which makes the
    mine look idle in `hours_worked_1q` either way."""
    _, latest = reported_quarters(lake)
    reports = scan(lake, "mines_prod_quarterly").group_by("mine_id").agg(
        (report_quarter() != latest).all().alias("newest_report_missing"))
    names = scan(lake, "mines").select(
        "mine_id", pl.col("current_mine_name").alias("mine_name"), pl.col("current_mine_status").alias("status"))
    return reports.join(names, on="mine_id", how="left").select(
        "mine_id", "mine_name", "status", "newest_report_missing")


def build_panel(lake: Path) -> pl.LazyFrame:
    """The mine-quarter panel from the first quarter with WINDOW quarters of reports before it to the quarter after the
    newest report. That last quarter is still open: its rows are the ones to score, with a null target."""
    first, latest = reported_quarters(lake)
    quarterly = quarterly_activity(lake)
    label = quarterly.select(*KEY, pl.col("serious_accidents").alias("serious_in_quarter"))
    return (
        history(quarterly)
        .filter(pl.col("active_quarters_4q") > 0, pl.col("quarter").is_between(first + WINDOW, latest + 1))
        .join(cited_sections(lake), on=KEY, how="left")
        .join(label, on=KEY, how="left")
        .join(mine_attributes(lake), on="mine_id", how="left")
        .with_columns(calendar_quarter=pl.format("Q{}", pl.col("quarter") % 4 + 1))
        .select(
            *KEY,
            (pl.col("quarter") // 4).cast(pl.Int16).alias("year"),
            *[pl.col(name).fill_null(0).cast(pl.Float64 if name in REAL else pl.Int32) for name in NUMERIC],
            *[pl.col(name).fill_null("Unknown") for name in CATEGORICAL],
            pl.when(pl.col("quarter") <= latest).then(pl.col("serious_in_quarter").fill_null(0) > 0).alias(TARGET),
        )
        .sort("quarter", "mine_id")
    )
