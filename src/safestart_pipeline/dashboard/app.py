"""SafeStart's dashboard: the live event stream, the enforcement record, the standards it cites and each mine's brief.

Five pages: Live (events as they arrive), Enforcement (counts per day with forecasts and anomalies, and each
district's history), Standards (the most cited 30 CFR sections, their text and the S&S trend), Mine brief (one mine
as a crew sees it before a shift) and Sources (what the pipeline holds and how fresh it is). Pages read only through
`data`, which turns a missing store into a sentence, so a panel shows either its data or what is missing.

The tabs run lazily and follow the URL (`?page=Mine+brief&mine=4609612`): only the open page queries its stores, and
a mine's brief can be shared as a link. Warehouse results stay cached until a new build is swapped in. Stream results
are cached for a few seconds, shared by every viewer, and the per-kind totals, which count the whole event table, a
little longer, so more viewers do not mean more load on the small host.

Run: streamlit run src/safestart_pipeline/dashboard/app.py
"""

from __future__ import annotations

import re
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import pandas as pd
import streamlit as st

from safestart_pipeline.dashboard import charts, data
from safestart_pipeline.dashboard.charts import (
    COMMODITY_LABELS,
    KIND_LABELS,
    KIND_ORDER,
    NO_DISTRICT,
)
from safestart_pipeline.stream.events import ENFORCEMENT_KINDS

PAGES = ("Live", "Enforcement", "Standards", "Mine brief", "Sources")
REFRESH_SECONDS = 5
FRESH_TTL_SECONDS = 4
TOTALS_TTL_SECONDS = 15
MODES = {"all": "All", "replay": "Replay", "live": "Live"}
ENFORCEMENT_ORDER = [kind for kind in KIND_ORDER if kind in ENFORCEMENT_KINDS]
PERIODS = {"90 days": (90, "day"), "1 year": (365, "day"), "5 years": (1826, "week"), "All": (36_500, "month")}
FORECAST_DAYS = 365
MEASURES = {"violations": "Violations", "ss_violations": "S&S violations", "accidents": "Accidents",
            "inspections": "Inspections"}
TILE_WIDTH = 170  # two tiles side by side on a 390 px phone
CREW_USE = (
    "Before a shift, the crew lead opens the mine's brief: the standards inspectors cite most here and the latest "
    "accidents become the hazards the crew checks first in SafeStart's pre-shift assessment."
)
MARKDOWN_SPECIAL = re.compile(r"([\\`*_\[\]<>#$~|])")  # emphasis, code, links, HTML, headings, LaTeX, tables
LINE_MARKER = re.compile(r"^(\s*)(?:([-+=])|(\d+)([.)]))", re.MULTILINE)  # list items and rules at a line start
SECTION_COLUMNS = {
    "cfr_section": st.column_config.TextColumn("Section", width="small"),
    "heading": st.column_config.TextColumn("Heading", width="large"),
    "citations": st.column_config.NumberColumn("Citations", format="localized"),
    "ss_share": st.column_config.ProgressColumn("S&S share", format="percent", min_value=0.0, max_value=1.0),
    "ss_citations": st.column_config.NumberColumn("S&S", format="localized"),
    "proposed_penalty_total": st.column_config.NumberColumn("Penalties proposed (USD)", format="compact"),
    "proposed_penalty": st.column_config.NumberColumn("Penalties proposed (USD)", format="compact"),
    "last_cited": st.column_config.DateColumn("Last cited"),
}


# Cached reads


@st.cache_data(show_spinner=False, max_entries=512)
def _warehouse_cached(query: str, stamp: tuple[str, int] | None, args: tuple[Any, ...]) -> data.Table:
    return getattr(data, query)(*args)


@st.cache_data(show_spinner=False, ttl=FRESH_TTL_SECONDS, max_entries=256)
def _fresh_cached(query: str, args: tuple[Any, ...]) -> data.Table:
    return getattr(data, query)(*args)


@st.cache_data(show_spinner=False, ttl=TOTALS_TTL_SECONDS, max_entries=16)
def _totals_cached(query: str, args: tuple[Any, ...]) -> data.Table:
    return getattr(data, query)(*args)


def warehouse(query: Callable[..., data.Table], *args: Any) -> data.Table:
    """A warehouse query, cached until a new build replaces the file."""
    return _warehouse_cached(query.__name__, data.warehouse_stamp(), args)


def stream(query: Callable[..., data.Table], *args: Any) -> data.Table:
    """A streaming-store query, cached for a few seconds."""
    return _fresh_cached(query.__name__, args)


# Small helpers


def shown(table: data.Table, empty: str) -> bool:
    """Whether `table` has rows to show; otherwise says why not, in place of the panel."""
    if table.problem:
        st.info(table.problem, icon=":material/info:")
        return False
    if table.empty:
        st.caption(empty)
        return False
    return True


def md(text: Any) -> str:
    """Text shown literally in Markdown: titles, names and narratives hold `*`, `$`, `#`, brackets and "1." lines."""
    escaped = MARKDOWN_SPECIAL.sub(r"\\\1", str(text))
    return LINE_MARKER.sub(lambda found: found[1] + (f"\\{found[2]}" if found[2] else f"{found[3]}\\{found[4]}"),
                           escaped)


def plain(value: Any) -> str:
    """A text value, or "" for a missing one (pandas holds missing text as None or NaN)."""
    return "" if value is None or pd.isna(value) else str(value)


def count(value: Any) -> str:
    """A whole number that fits a phone-width tile: 12,345, or 2.14M from a million on."""
    if pd.isna(value):
        return "–"
    return f"{value / 1e6:,.2f}M" if abs(value) >= 1e6 else f"{value:,.0f}"


def money(value: Any) -> str:
    if pd.isna(value):
        return "–"
    return f"${value / 1e6:,.1f}M" if value >= 1e6 else f"${value:,.0f}"


def ago(moment: Any, now: datetime) -> str:
    if pd.isna(moment):
        return "never"
    seconds = max(0.0, (now - moment).total_seconds())
    for size, unit in ((86_400, "day"), (3_600, "hour"), (60, "minute")):
        if seconds >= size:
            number = int(seconds // size)
            return f"{number} {unit}{'' if number == 1 else 's'} ago"
    return "just now"


def tile(label: str, value: str, **options: Any) -> None:
    st.metric(label, value, border=True, width=TILE_WIDTH, **options)


def subject(section: str, heading: Any) -> str:
    """A section's heading without the "§ 75.400" the eCFR writes before it, which the section column already shows."""
    return re.sub(rf"^§*\s*{re.escape(section)}\s*", "", plain(heading)).strip()


def section_picker(frame: pd.DataFrame, key: str, height: int | str = "auto") -> str | None:
    """A table of sections in which a row can be selected; returns the selected section, or the first one."""
    if "heading" in frame:
        frame = frame.assign(heading=[subject(*pair) for pair in zip(frame["cfr_section"], frame["heading"])])
    columns = [column for column in SECTION_COLUMNS if column in frame]
    event = st.dataframe(frame[columns], hide_index=True, column_config=SECTION_COLUMNS, on_select="rerun",
                         selection_mode="single-row", key=key, height=height)
    rows = list(event.selection.rows) if event is not None else []
    row = rows[0] if rows and rows[0] < len(frame) else 0
    return str(frame.iloc[row]["cfr_section"]) if len(frame) else None


def paragraphs(text: str) -> str:
    return "\n\n".join(md(line.strip()) for line in text.splitlines() if line.strip())


def show_section(section: str | None) -> None:
    """A section's heading and text, as the warehouse holds them from the eCFR; a long text scrolls in its box."""
    if section is None:
        return
    found = warehouse(data.section_text, section)
    if not shown(found, f"30 CFR {section} is not in the warehouse's eCFR text (removed or renumbered since cited)."):
        return
    row = found.frame.iloc[0]
    title = f"{int(row['title'])} CFR" if not pd.isna(row["title"]) else "CFR"
    facts = [f"Part {int(row['part'])}" if not pd.isna(row["part"]) else "",
             f"last amended {pd.Timestamp(row['amended_on']):%Y-%m-%d}" if not pd.isna(row["amended_on"]) else ""]
    text = paragraphs(plain(row["text"]))
    with st.container(border=True):
        st.markdown(f"**{md(f'{title} {section}')}** {md(subject(section, row['heading']))}")
        if any(facts):
            st.caption(", ".join(fact for fact in facts if fact))
        with st.container(height=300 if len(text) > 1_200 else "content", border=False):
            st.markdown(text or "_No text._")


# Pages


def live_page() -> None:
    mode = st.segmented_control("Events", list(MODES), default="all", required=True, format_func=MODES.get,
                                key="live_mode")
    live_panel(None if mode == "all" else mode)


@st.fragment(run_every=REFRESH_SECONDS)
def live_panel(mode: str | None) -> None:
    counts = _totals_cached("event_counts", (mode,))
    if not shown(counts, "No events yet: start the replay producer or the live watchers."):
        return
    totals = {kind: 0 for kind in KIND_ORDER} | {
        str(kind): int(events) for kind, events in zip(counts.frame["kind"], counts.frame["events"])}
    baseline = st.session_state.setdefault("live_baseline", {}).setdefault(mode or "all", totals)
    with st.container(horizontal=True, gap="small"):
        for kind in KIND_ORDER:
            new = totals[kind] - baseline.get(kind, 0)
            tile(KIND_LABELS[kind], f"{totals[kind]:,}", delta=f"{new:,}" if new > 0 else None, delta_color="off",
                 delta_description="since opened" if new > 0 else None)
    arrivals = stream(data.arrivals, mode)
    if shown(arrivals, "Nothing received in the last 15 minutes."):
        st.altair_chart(charts.arrivals(arrivals.frame), width="stretch")
    events = stream(data.latest_events, mode)
    if shown(events, "No events yet."):
        feed = events.frame.assign(kind=events.frame["kind"].map(KIND_LABELS))
        st.dataframe(
            feed[["kind", "title", "occurred_at", "district", "state", "mode", "received_at"]], hide_index=True,
            column_config={
                "kind": st.column_config.TextColumn("Kind"),
                "title": st.column_config.TextColumn("Event", width="large"),
                "occurred_at": st.column_config.DateColumn("Occurred"),
                "district": st.column_config.TextColumn("District", width="small"),
                "state": st.column_config.TextColumn("State", width="small"),
                "mode": st.column_config.TextColumn("Mode", width="small"),
                "received_at": st.column_config.DatetimeColumn("Received (UTC)", format="HH:mm:ss"),
            },
        )
    st.caption(f"Updated {datetime.now(UTC):%H:%M:%S} UTC; refreshes every {REFRESH_SECONDS} seconds.")


def enforcement_page() -> None:
    kind = st.segmented_control("Records", ENFORCEMENT_ORDER, default="violation", required=True,
                                format_func=KIND_LABELS.get, key="enforcement_kind")
    period = st.segmented_control("Period", list(PERIODS), default="1 year", required=True, key="enforcement_period")
    days, grain = PERIODS[period]
    label = KIND_LABELS[kind]
    st.subheader(f"{label} by {grain}", anchor=False)
    counts = stream(data.daily_counts, kind, days, grain)
    if shown(counts, f"No {label.lower()} counted yet: the analytics consumer keeps these counts."):
        st.altair_chart(charts.counts_over_time(counts.frame, kind, grain), width="stretch")
        span = "all days" if period == "All" else f"last {period}"
        st.altair_chart(charts.counts_by_district(counts.frame, kind, span), width="stretch")

    st.subheader("Observed and forecast", anchor=False)
    districts = stream(data.forecast_districts, kind)
    if shown(districts, "No forecasts yet: the analytics consumer writes one for each complete day."):
        options = list(districts.frame["district"])
        district = st.selectbox("District", options, format_func=lambda name: name or NO_DISTRICT)
        series = stream(data.forecasts, kind, district, min(days, FORECAST_DAYS))
        if shown(series, "No forecasts for this district in the period."):
            st.altair_chart(charts.forecast(series.frame, kind), width="stretch")
            methods = ", ".join(sorted(set(series.frame["method"].astype(str))))
            st.caption(f"One-step-ahead forecast of each day ({md(methods)}); red points are the days the analytics "
                       "consumer flagged as anomalies.")
        anomalies = stream(data.recent_anomalies, kind)
        if shown(anomalies, f"No anomalies flagged for {label.lower()}."):
            st.markdown("**Latest anomalies, all districts**")
            st.dataframe(anomalies.frame.assign(district=anomalies.frame["district"].replace("", NO_DISTRICT)),
                         hide_index=True, column_order=["day", "district", "observed", "predicted", "residual"],
                         column_config={
                             "day": st.column_config.DateColumn("Day"),
                             "district": "District",
                             "observed": st.column_config.NumberColumn("Observed", format="localized"),
                             "predicted": st.column_config.NumberColumn("Forecast", format="%.1f"),
                             "residual": st.column_config.NumberColumn("Residual", format="%+.1f")})

    st.subheader("Districts by year", anchor=False)
    history = warehouse(data.district_years)
    if shown(history, "No district history in the warehouse."):
        measure = st.segmented_control("Measure", list(MEASURES), default="violations", required=True,
                                       format_func=MEASURES.get, key="history_measure")
        st.altair_chart(charts.district_heatmap(history.frame, measure, f"{MEASURES[measure]} per year"),
                        width="stretch")


def standards_page() -> None:
    st.subheader("Most cited standards", anchor=False)
    top = warehouse(data.top_cited_sections, 100)
    if shown(top, "No citations in the warehouse."):
        st.altair_chart(charts.top_sections(top.frame.head(15)), width="stretch")
        st.caption("The 100 most cited sections of 30 CFR. Select a row to read the section.")
        show_section(section_picker(top.frame, key="standards_sections", height=320))

    st.subheader("S&S share of violations", anchor=False)
    trend = warehouse(data.ss_trend)
    if shown(trend, "No violations in the warehouse."):
        st.altair_chart(charts.ss_trend(trend.frame), width="stretch")
        st.caption("S&S (significant and substantial): a violation reasonably likely to lead to a serious injury or "
                   f"illness. Years with fewer than {charts.TREND_MIN_VIOLATIONS} violations are left out.")


def mine_page() -> None:
    st.caption(CREW_USE)
    text = st.text_input("Mine name or MSHA mine ID", key="mine", bind="query-params", type="search",
                         placeholder="For example 4609612 or Highwall").strip()
    if not text:
        return
    matches = warehouse(data.find_mines, text)
    if not shown(matches, f"No mine name or ID matches {md(text)}."):
        return
    frame = matches.frame
    if len(frame) == 1:
        mine_id = str(frame.iloc[0]["mine_id"])
    else:
        labels = {row.mine_id: " · ".join(part for part in map(plain, (row.mine_name, row.mine_id, row.state,
                                                                        row.status)) if part)
                  for row in frame.itertuples()}
        mine_id = st.selectbox(f"{len(frame)} mines match", list(labels), format_func=labels.get)
    mine_brief(mine_id)


def mine_brief(mine_id: str) -> None:
    card = warehouse(data.mine_scorecard, mine_id)
    if not shown(card, f"Mine {mine_id} has no scorecard in the warehouse."):
        return
    mine = card.frame.iloc[0]
    details = warehouse(data.mine_details, mine_id)
    extra = details.frame.iloc[0] if not details.empty else pd.Series(dtype=object)
    st.subheader(plain(mine["mine_name"]) or f"Mine {mine_id}", anchor=False)
    place = ", ".join(part for part in (plain(extra.get("county")), plain(mine["state"])) if part)
    district = plain(mine["district"])
    facts = [f"ID {mine_id}", COMMODITY_LABELS.get(plain(mine["coal_metal"]), ""), plain(mine["mine_type"]),
             plain(mine["status"]), place, f"district {district}" if district else "",
             plain(extra.get("operator_name"))]
    st.caption(" · ".join(md(fact) for fact in facts if fact))
    window = warehouse(data.twelve_months)
    if not window.problem and not window.empty and not pd.isna(window.frame.iloc[0]["until"]):
        since, until = window.frame.iloc[0]["since"], window.frame.iloc[0]["until"]
        st.caption(f"Figures cover the 12 months from {since:%Y-%m-%d} to {until:%Y-%m-%d}, the newest records.")

    with st.container(horizontal=True, gap="small"):
        tile("Inspections", count(mine["inspections_12m"]))
        tile("Violations", count(mine["violations_12m"]))
        tile("S&S violations", count(mine["ss_violations_12m"]))
        tile("Penalties proposed", money(mine["proposed_penalty_12m"]))
        tile("Accidents", count(mine["accidents_12m"]))
        tile("Serious accidents", count(mine["serious_accidents_12m"]),
             help="Fatal, or with days away from work or restricted work.")
        tile("Hours worked", count(mine["hours_worked_12m"]))
        injury_rate_tile(mine)

    st.markdown("**Standards cited most here**")
    sections = warehouse(data.mine_sections, mine_id)
    if shown(sections, "No citations at this mine in the 12 months."):
        show_section(section_picker(sections.frame, key=f"mine_sections_{mine_id}"))

    st.markdown("**Recent accidents**")
    accidents = warehouse(data.mine_accidents, mine_id)
    if shown(accidents, "No accidents on record for this mine."):
        for accident in accidents.frame.itertuples():
            day = f"{accident.accident_date:%Y-%m-%d}" if not pd.isna(accident.accident_date) else "Undated"
            parts = [day, plain(accident.classification), plain(accident.degree_injury)]
            with st.expander(" · ".join(md(part) for part in parts if part)):
                facts = [plain(accident.accident_type), plain(accident.subunit)] + [
                    f"{count(number)} {what}" for number, what in ((accident.no_injuries, "injured"),
                                                                    (accident.days_lost, "days lost"),
                                                                    (accident.days_restrict, "days restricted"))
                    if not pd.isna(number)]
                st.caption(" · ".join(md(fact) for fact in facts if fact))
                st.markdown(paragraphs(plain(accident.narrative)) or "_No narrative._")


def injury_rate_tile(mine: pd.Series) -> None:
    """Injuries per 200,000 hours worked, compared with the newest national rate for the mine's commodity."""
    rate, commodity = mine["injury_rate_12m"], plain(mine["coal_metal"])
    national = warehouse(data.national_injury_rate, commodity) if commodity else data.Table()
    help_text = "Injuries per 200,000 hours worked (about 100 full-time workers for a year)."
    if pd.isna(rate) or national.problem or national.empty:
        tile("Injury rate", "–" if pd.isna(rate) else f"{rate:.2f}", help=help_text)
        return
    year, average = int(national.frame.iloc[0]["year"]), float(national.frame.iloc[0]["rate_per_200k"])
    tile("Injury rate", f"{rate:.2f}", delta=f"{rate - average:+.2f}", delta_color="inverse",
         delta_description="vs. US",
         help=f"{help_text} The national rate for {COMMODITY_LABELS.get(commodity, 'these').lower()} mines in {year} "
              f"was {average:.2f}.")


def sources_page() -> None:
    st.subheader("Datasets", anchor=False)
    versions = warehouse(data.dataset_versions)
    if shown(versions, "No dataset versions recorded in the warehouse."):
        st.dataframe(versions.frame, hide_index=True, column_config={
            "dataset": "Dataset",
            "versions": st.column_config.NumberColumn("Versions kept", format="localized"),
            "version": "Current version",
            "rows": st.column_config.NumberColumn("Rows", format="localized"),
            "built_at": st.column_config.DatetimeColumn("Built (UTC)", format="YYYY-MM-DD HH:mm"),
            "is_current": st.column_config.CheckboxColumn("Current"),
        })

    st.subheader("Downloaded documents", anchor=False)
    documents = warehouse(data.document_counts)
    if shown(documents, "No downloaded documents recorded in the warehouse."):
        total, size = int(documents.frame["documents"].sum()), float(documents.frame["bytes"].sum())
        st.caption(f"{total:,} documents ({size / 1e9:,.1f} GB) downloaded from the MSHA and OSHA websites.")
        st.altair_chart(charts.documents(documents.frame), width="stretch")

    st.subheader("Live source checks", anchor=False)
    checks = stream(data.source_freshness)
    if shown(checks, "No live checks yet: start the watchers."):
        now = datetime.now(UTC)
        failing = checks.frame.loc[checks.frame["status"] == "error", "source"]
        if len(failing):
            st.warning("The newest check failed for " + ", ".join(md(source) for source in failing) + ".",
                       icon=":material/warning:")
        st.dataframe(checks.frame.assign(age=checks.frame["checked_at"].map(lambda moment: ago(moment, now))),
                     hide_index=True,
                     column_order=["source", "age", "status", "found", "checks_24h", "errors_24h", "found_24h", "note"],
                     column_config={
                         "source": "Source",
                         "age": "Last checked",
                         "status": "Status",
                         "found": st.column_config.NumberColumn("Found", format="localized"),
                         "checks_24h": st.column_config.NumberColumn("Checks (24 h)", format="localized"),
                         "errors_24h": st.column_config.NumberColumn("Errors (24 h)", format="localized"),
                         "found_24h": st.column_config.NumberColumn("Found (24 h)", format="localized"),
                         "note": "Note"})


def header() -> None:
    st.title("SafeStart safety data", anchor=False)
    stamp = data.warehouse_stamp()
    built = (f"warehouse built {datetime.fromtimestamp(stamp[1] / 1e9, UTC):%Y-%m-%d %H:%M} UTC" if stamp
             else "no warehouse yet")
    st.caption(f"MSHA enforcement records, the 30 CFR standards they cite, and the live event stream; {built}.")


def main() -> None:
    st.set_page_config(page_title="SafeStart safety data", page_icon=":material/health_and_safety:", layout="wide")
    header()
    tabs = st.tabs(PAGES, key="page", on_change="rerun", bind="query-params")
    for tab, page in zip(tabs, (live_page, enforcement_page, standards_page, mine_page, sources_page)):
        if tab.open is not False:
            with tab:
                page()


if __name__ == "__main__":
    main()
