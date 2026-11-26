"""SafeStart's dashboard: the live event stream and what the pipeline is doing now, the enforcement record,
the standards it cites, each mine's brief and the sources behind it all.

Five pages: Live (the seven event counts, what moves them and when, the daily update of the official files while it
runs, the feed for SafeStart, the watchers' and flows' next times, events received over time, the newest events, and
every check and run of the last two days on one time line), Enforcement (counts per day with forecasts and anomalies,
and each district's history), Standards (the most cited 30 CFR sections, their text and the S&S trend), Mine brief (one
mine as a crew sees it before a shift) and Sources (the official files' daily update and its feed first, then the live
checks, the datasets and the downloaded documents). Pages read only through `data`, which turns a missing store into a
sentence so a panel shows either its data or what is missing.

The Live page shows only what really changes. Every event counts once, so the totals grow only when a source publishes
something new; each tile says what the last day brought, shows its kind's last 14 days, and names the check that can
move it and when that check is next due. The first available total counts up on entry. After that its digits roll
and the tile glows on a real change. A separate indicator reports connectivity to the stream store.

The tabs run lazily and follow the URL (`?page=Mine+brief&mine=4609612`): only the open page queries its stores, and
a mine's brief can be shared as a link. Warehouse answers stay cached until a new build is swapped in, but a query that
failed is asked again on the next run, so a passing failure (memory pressure, another process holding the file) does
not stay on a panel until the next build. Stream results are cached for a few seconds, shared by every viewer, the
per-kind totals, which count the whole event table, a little longer, and the hourly and daily views a minute, so more
viewers do not mean more load on the small host.

Tables format their numbers with printf formats, which Streamlit applies alike in every browser. Its named formats
("localized", "compact", "percent") follow the viewer's language instead: 148,461 becomes 148.461 in a German browser
and 1,48,461 in a Hindi one, and "compact" writes 156 million as "156 Mio." in German, unlike the tiles and chart
tooltips beside them, which read the same everywhere.

Run: streamlit run src/safestart_pipeline/dashboard/app.py
"""

from __future__ import annotations

import base64
import re
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import altair as alt
import pandas as pd
import streamlit as st

from safestart_pipeline.dashboard import charts, data, view
from safestart_pipeline.dashboard.charts import (
    COMMODITY_LABELS,
    KIND_COLORS,
    KIND_LABELS,
    KIND_ORDER,
    NO_DISTRICT,
)
from safestart_pipeline.definitions import SERIOUS_DEGREES
from safestart_pipeline.flows.schedule import SCHEDULES
from safestart_pipeline.official.feed import STALE_AFTER
from safestart_pipeline.stream.events import ENFORCEMENT_KINDS, TOPIC_ENFORCEMENT, TOPIC_SOURCES
from safestart_pipeline.watch.schedule import BUSY_RETRY, SCHEDULE

PAGES = ("Live", "Enforcement", "Standards", "Mine brief", "Sources")
REFRESH_SECONDS = 5  # the tiles and the newest events
NOW_SECONDS = 10  # the Now strip: the official run's requests come about every 10 s while it runs
SLOW_SECONDS = 60  # events per hour or day, and the heartbeat
FRESH_TTL_SECONDS = 4
TOTALS_TTL_SECONDS = 15
SLOW_TTL_SECONDS = 55
MODES = {"all": "All", "replay": "Replay", "live": "Live"}
ENFORCEMENT_ORDER = [kind for kind in KIND_ORDER if kind in ENFORCEMENT_KINDS]
SOURCE_ORDER = [kind for kind in KIND_ORDER if kind not in ENFORCEMENT_KINDS]
PERIODS = {"90 days": (90, "day"), "1 year": (365, "day"), "5 years": (1826, "week"), "All": (36_500, "month")}
FORECAST_DAYS = 365
MEASURES = {"violations": "Violations", "ss_violations": "S&S violations", "accidents": "Accidents",
            "inspections": "Inspections"}
TREND_DAYS = 14
FRESH_BADGE = timedelta(seconds=60)  # how long a tile says "+N new" after this viewer saw its count grow
ASKING = timedelta(seconds=60)  # a source asked this recently is being asked now: it pulses
ACTIVITY = {"48 h": ("hour", 48 * 60), "14 d": ("day", 14 * 24 * 60), "15 min": ("minute", 15)}
HEARTBEAT_HOURS, AHEAD_HOURS = 48, 12
CHECKS = {"federal-register": "Federal Register", "versions": "Dataset versions", "sitemaps-msha": "MSHA sitemaps",
          "sitemaps-osha": "OSHA sitemaps", "feed-official": "Official feed"}
CHECK_TONES = {"ok": "good", "unchanged": "muted", "refused": "warning", "skipped": "dim", "error": "critical"}
# The check that can move each tile: MSHA's records arrive with a new dataset version, page changes with the official
# files' feed (and the sitemaps where the feed has not been read yet), regulations from the Federal Register.
KIND_CHECKS = {**dict.fromkeys(ENFORCEMENT_KINDS, "versions"), "dataset_version": "versions",
               "regulation": "federal-register", "page_change": "feed-official"}
OFFICIAL_SOURCES = {"msha": "www.msha.gov probe", "ecfr": "eCFR editions", "federal_register": "Federal Register rules",
                    "arlweb": "MSHA legacy host re-check", "osha": "OSHA re-check"}
RUN_TONES = {"done": "good", "errors": "serious", "interrupted": "warning"}
CHANGE_TONES = {"new": "good", "changed": "accent", "gone": "critical", "restored": "good", "moved": "warning"}
CREW_USE = (
    "Before a shift, the crew lead opens the mine's brief: the standards inspectors cite most here and the latest "
    "accidents become the hazards the crew checks first in SafeStart's pre-shift assessment."
)
SERIOUS_HELP = (f"Degree of injury {SERIOUS_DEGREES[0]} to {SERIOUS_DEGREES[-1]}: a death, a permanent disability, or "
                "days away from work or of restricted work.")
MARKDOWN_SPECIAL = re.compile(r"([\\`*_\[\]<>#$~|])")  # emphasis, code, links, HTML, headings, LaTeX, tables
LINE_MARKER = re.compile(r"^(\s*)(?:([-+=])|(\d+)([.)]))", re.MULTILINE)  # list items and rules at a line start
COUNT_FORMAT = "%,d"  # 148,461 in every browser language (see the module docstring)
SECTION_COLUMNS = {
    "cfr_section": st.column_config.TextColumn("Section", width="small", pinned=True),
    "heading": st.column_config.TextColumn("Heading", width="large"),
    "citations": st.column_config.NumberColumn("Citations", format=COUNT_FORMAT),
    "ss_share": st.column_config.ProgressColumn("S&S share", format="%.1f%%", min_value=0, max_value=100),
    "ss_citations": st.column_config.NumberColumn("S&S", format=COUNT_FORMAT),
    "proposed_penalty_total": st.column_config.NumberColumn("Penalties proposed", format="$%,.1fM"),
    "proposed_penalty": st.column_config.NumberColumn("Penalties proposed", format="$%,.0f"),
    "last_cited": st.column_config.DateColumn("Last cited"),
}
# A printf format cannot scale a number, so these columns are shown in the units their formats state: a share in
# percent, and the nation's penalty totals in millions (a mine's stay in dollars, which millions would round to $0.0M).
SECTION_SCALES = {"ss_share": 100, "proposed_penalty_total": 1e-6}
OFFICIAL_LABELS = {"new": "New", "changed": "Changed", "gone": "Gone", "restored": "Restored", "moved": "Moved",
                   "rejected": "Refused files", "checked": "Confirmed unchanged"}
STALE_HOURS = int(STALE_AFTER.total_seconds() // 3600)
THEME_CSS = (Path(__file__).with_name("theme.css")).read_text(encoding="utf-8")
TERRAIN_IMAGE = base64.b64encode(Path(__file__).with_name("terrain.svg").read_bytes()).decode("ascii")


# Cached reads


class _Unanswered(Exception):
    """A warehouse query's problem, raised out of its cached function: st.cache_data keeps no exception, so only
    answers are cached."""

    def __init__(self, table: data.Table) -> None:
        super().__init__(table.problem)
        self.table = table


@st.cache_data(show_spinner=False, max_entries=512)
def _warehouse_cached(query: str, stamp: tuple[str, int] | None, args: tuple[Any, ...]) -> data.Table:
    table = getattr(data, query)(*args)
    if table.problem:
        raise _Unanswered(table)
    return table


@st.cache_data(show_spinner=False, ttl=FRESH_TTL_SECONDS, max_entries=256)
def _fresh_cached(query: str, args: tuple[Any, ...]) -> data.Table:
    return getattr(data, query)(*args)


@st.cache_data(show_spinner=False, ttl=TOTALS_TTL_SECONDS, max_entries=16)
def _totals_cached(query: str, args: tuple[Any, ...]) -> data.Table:
    return getattr(data, query)(*args)


@st.cache_data(show_spinner=False, ttl=SLOW_TTL_SECONDS, max_entries=64)
def _slow_cached(query: str, args: tuple[Any, ...]) -> data.Table:
    return getattr(data, query)(*args)


def warehouse(query: Callable[..., data.Table], *args: Any) -> data.Table:
    """A warehouse query, cached until a new build replaces the file; a problem is asked again on the next run."""
    try:
        return _warehouse_cached(query.__name__, data.warehouse_stamp(), args)
    except _Unanswered as unanswered:
        return unanswered.table


def stream(query: Callable[..., data.Table], *args: Any) -> data.Table:
    """A streaming-store query, cached for a few seconds."""
    return _fresh_cached(query.__name__, args)


def slow(query: Callable[..., data.Table], *args: Any) -> data.Table:
    """A query of the last hours or days, cached for about a minute."""
    return _slow_cached(query.__name__, args)


def files(query: Callable[..., data.Table], *args: Any) -> data.Table:
    """A read of the data root's own records (the official files' run, feed and purge), cached for a few seconds."""
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


def moment(value: Any) -> datetime | None:
    """A time from a table, or None for a missing one."""
    return None if value is None or pd.isna(value) else pd.Timestamp(value).to_pydatetime()


def count(value: Any) -> str:
    """A whole number that fits a phone-width tile: 12,345, or 2.14M from a million on."""
    if pd.isna(value):
        return "–"
    return f"{value / 1e6:,.2f}M" if abs(value) >= 1e6 else f"{value:,.0f}"


def money(value: Any) -> str:
    if pd.isna(value):
        return "–"
    return f"${value / 1e6:,.1f}M" if value >= 1e6 else f"${value:,.0f}"


def quarter_label(quarter: int) -> str:
    """A quarter numbered year * 4 + quarter - 1, as people write it: 2026 Q3."""
    return f"{quarter // 4} Q{quarter % 4 + 1}"


def ago(moment: Any, now: datetime) -> str:
    if moment is None or pd.isna(moment):
        return "never"
    seconds = max(0.0, (now - moment).total_seconds())
    for size, unit in ((86_400, "day"), (3_600, "hour"), (60, "minute")):
        if seconds >= size:
            number = int(seconds // size)
            return f"{number} {unit}{'' if number == 1 else 's'} ago"
    return "just now"


def tile(label: str, value: str, **options: Any) -> None:
    st.metric(label, value, border=True, **options)


def tiles(columns: int, minimum: int = 150) -> Any:
    """A responsive grid of tiles: `columns` across on a wide screen, as many as fit at `minimum` px on a narrow one."""
    return st.grid(columns, min_column_width=minimum, gap="xsmall")


def show_chart(chart: alt.Chart | alt.LayerChart) -> None:
    """Style a complete Vega-Lite figure after its layers are composed; keep native theme text colors."""
    figure = (
        chart.configure_axis(labelFont="monospace", labelFontSize=11, titleFontSize=12, gridDash=[2, 4],
                             gridOpacity=0.55)
        .configure_legend(labelFont="monospace", labelFontSize=11, symbolStrokeWidth=2)
        .configure_view(strokeOpacity=0)
    )
    st.altair_chart(figure, width="stretch")


def section_picker(frame: pd.DataFrame, key: str, height: int | str = "auto") -> str | None:
    """A table of sections in which a row can be selected; returns the selected section, or the first one."""
    scaled = frame.assign(**{column: frame[column] * scale for column, scale in SECTION_SCALES.items()
                             if column in frame})
    columns = [column for column in SECTION_COLUMNS if column in scaled]
    event = st.dataframe(scaled[columns], hide_index=True, column_config=SECTION_COLUMNS, on_select="rerun",
                         selection_mode="single-row", key=key, height=height)
    rows = list(event.selection.rows) if event is not None else []
    row = rows[0] if rows and rows[0] < len(frame) else 0
    return str(frame.iloc[row]["cfr_section"]) if len(frame) else None


def paragraphs(text: str) -> str:
    return "\n\n".join(md(line.strip()) for line in text.splitlines() if line.strip())


def provenance(text: str) -> None:
    """Where a panel's numbers come from, in small print under it (`backticks` mark a store or table)."""
    st.html(view.source(text))


def show_section(section: str | None) -> None:
    """A section's heading and text, as the warehouse holds them from the eCFR; a long text scrolls in its box."""
    if section is None:
        return
    found = warehouse(data.section_text, section)
    if not shown(found, f"30 CFR {section} is not in the warehouse's eCFR text (removed or renumbered since cited)."):
        return
    row = found.frame.iloc[0]
    name = f"{int(row['title'])} CFR {section}"
    facts = [f"Part {int(row['part'])}" if not pd.isna(row["part"]) else "",
             f"last amended {pd.Timestamp(row['amended_on']):%Y-%m-%d}" if not pd.isna(row["amended_on"]) else ""]
    text = paragraphs(plain(row["text"]))
    with st.container(border=True):
        st.markdown(f"**{md(name)}** {md(plain(row['heading']))}")
        if any(facts):
            st.caption(", ".join(fact for fact in facts if fact))
        with st.container(height=300 if len(text) > 1_200 else "content", border=False):
            st.markdown(text or "_No text._")


# The Live page


def live_page() -> None:
    mode = st.segmented_control("Events", list(MODES), default="all", required=True, format_func=MODES.get,
                                key="live_mode", label_visibility="collapsed")
    selected = None if mode == "all" else mode
    live_tiles(selected)
    now_strip()
    left, right = st.columns([3, 2], gap="medium")
    with left:
        activity_panel(selected)
    with right:
        latest_panel(selected)
    heartbeat_panel()


def newest_checks(checks: data.Table) -> dict[str, dict[str, Any]]:
    """Each watcher check's newest record (status, time, note) and when it is next due, by its schedule."""
    found: dict[str, dict[str, Any]] = {}
    if checks.problem or checks.empty:
        return found
    for row in checks.frame.itertuples():
        checked = moment(row.checked_at)
        interval = SCHEDULE.get(str(row.source))
        if (str(row.source).startswith("sitemaps-") and row.status == "skipped"
                and plain(row.note).endswith(f"asked again in {BUSY_RETRY}")):
            interval = BUSY_RETRY
        found[str(row.source)] = {"status": str(row.status), "checked_at": checked, "note": plain(row.note),
                                  "next": checked + interval if checked and interval else None}
    return found


def next_words(check: dict[str, Any], now: datetime) -> str:
    due = check.get("next")
    if due is None:
        return "not scheduled here"
    return "due now" if due <= now else f"next in about {view.span((due - now).total_seconds())}"


def kind_tile(kind: str, row: pd.Series | None, mode: str | None, previous: dict[str, int] | None,
              fresh: dict[str, int], days: dict[str, list[tuple[str, int]]], checks: dict[str, dict[str, Any]],
              now: datetime) -> view.Tile:
    """The tile of one kind: its total for the mode shown, what the last day brought, its last days and its check."""
    label = KIND_LABELS[kind]
    value = int(row["events"]) if row is not None else 0
    recent = int(row["last_24h"]) if row is not None else 0
    last = moment(row["last_received"]) if row is not None else None
    delta = f"+{recent:,} in 24 h"
    tone = "accent" if recent else "muted"
    name = KIND_CHECKS[kind]
    check = checks.get(name, {})
    foot = [f"{CHECKS[name]} · {view.ago(check['checked_at'], now)}", next_words(check, now)] if check else [
        f"{CHECKS[name]} · no check time"]
    if not recent and last:
        foot.insert(0, f"Last arrival {view.ago(last, now)}")
    if kind == "page_change":
        foot.append("Feed + sitemaps")
    if mode == "replay":
        foot = ["Historical replay" if kind in ENFORCEMENT_KINDS else "Live sources only"]
    shown_mode = MODES[mode or "all"]
    note = (f"{value:,} stored ({shown_mode}). {delta}. Last arrival: {view.stamp(last) or 'none'}. "
            f"{CHECKS[name]} last checked: {view.stamp(check.get('checked_at')) or 'unknown'}. "
            f"Next check (about): {view.stamp(check.get('next')) or 'unknown'}.")
    return view.Tile(kind=kind, label=label, color=KIND_COLORS[kind], value=value,
                     previous=None if previous is None else previous.get(kind, 0), delta=delta, delta_tone=tone,
                     days=days.get(kind, []), foot=foot, fresh=fresh.get(kind, 0), note=note)


def trend_days(table: data.Table, now: datetime) -> dict[str, list[tuple[str, int]]]:
    """Each kind's events per day over the last TREND_DAYS days, oldest first, the days without any included."""
    if table.problem:
        return {}
    days = [(now.date() - timedelta(days=offset)).isoformat() for offset in range(TREND_DAYS - 1, -1, -1)]
    counts: dict[tuple[str, str], int] = {}
    if not table.empty:
        counts = {(str(pd.Timestamp(day).date()), str(kind)): int(events)
                  for day, kind, events in zip(table.frame["day"], table.frame["kind"], table.frame["events"])}
    return {kind: [(day, counts.get((day, kind), 0)) for day in days] for kind in KIND_ORDER}


@st.fragment(run_every=REFRESH_SECONDS)
def live_tiles(mode: str | None) -> None:
    now = datetime.now(UTC)
    counts = _totals_cached("event_counts", (mode,))
    if counts.problem:
        st.info(counts.problem, icon=":material/info:")
    rows = {str(row["kind"]): row for _, row in counts.frame.iterrows()}
    totals = {kind: int(rows[kind]["events"]) if kind in rows else 0 for kind in KIND_ORDER}
    memory = st.session_state.setdefault("live_seen", {}).setdefault(mode or "all", {"totals": None, "fresh": {}})
    previous = memory["totals"]
    if counts.problem:
        previous = None
    if previous is not None:
        for kind, value in totals.items():
            if value > previous[kind]:
                memory["fresh"][kind] = (value - previous[kind], now)
            elif value < previous[kind]:
                memory["fresh"].pop(kind, None)
    fresh = {kind: grown for kind, (grown, seen) in memory["fresh"].items() if now - seen < FRESH_BADGE}
    daily = slow(data.daily_by_kind, TREND_DAYS)
    days = trend_days(daily, now)
    checks = newest_checks(stream(data.source_freshness))
    if counts.problem:
        built = {kind: view.Tile(kind, KIND_LABELS[kind], KIND_COLORS[kind], None, None, "Unavailable", "muted",
                                 days.get(kind, []), (), note=counts.problem) for kind in KIND_ORDER}
    else:
        built = {kind: kind_tile(kind, rows.get(kind), mode, previous, fresh, days, checks, now) for kind in KIND_ORDER}
    feeds = {None: "replay + live", "replay": "replay", "live": "live watchers"}[mode]
    groups = [("enforcement", TOPIC_ENFORCEMENT, feeds, [built[kind] for kind in ENFORCEMENT_ORDER]),
              ("sources", TOPIC_SOURCES, "live watchers", [built[kind] for kind in SOURCE_ORDER])]
    status = (view.chip("Stream counts unavailable", "warning", dot="warning") if counts.problem else
              view.chip("Stream store reachable", "good", dot="good"))
    updated = view.chip(f"Updated {now:%H:%M:%S} UTC · every {REFRESH_SECONDS} s", title=view.stamp(now))
    sweep = "ss-radar" if counts.problem else "ss-radar online"
    st.html(f'<div class="ss-monitor" style="--phase:-{now.timestamp() % 6:.2f}s">'
            f'<i class="{sweep}" aria-hidden="true"></i>'
            f'<div class="ss-row">{status}{updated}</div></div>' + view.kpi_row(groups))
    provenance("Totals and 24 h arrivals count distinct stored events in the selected mode (`stream.events`, "
               f"cached {TOTALS_TTL_SECONDS} s). "
               "Bars show 14 UTC source days across both modes (`stream.daily_counts`).")
    if daily.problem and daily.problem != counts.problem:
        st.caption(daily.problem)
    if not counts.problem:
        memory["totals"] = totals


@st.fragment(run_every=NOW_SECONDS)
def now_strip() -> None:
    now = datetime.now(UTC)
    st.html(view.cards([official_card(now, "Live"), feed_card(now, "Live"), schedule_card(now)]))
    provenance("Run and requests: `state/official.sqlite` and crawl stores. Feed: `exports/official/latest.json`. "
               "Checks: `stream.source_checks`. Next checks use the recorded start and scheduled interval.")


def watched_number(key: str, value: int, identity: str = "") -> str:
    """Compare consecutive observations of the same counter on this page, within this viewer's session."""
    memory = st.session_state.setdefault("official_seen", {})
    old_identity, previous = memory.get(key, (None, None))
    memory[key] = (identity, value)
    return view.odometer(value, previous if identity == old_identity else None)


def official_card(now: datetime, page: str) -> str:
    """The daily update of the official files: its progress while it runs (from the crawl store it writes), else its
    last run and its next start."""
    upcoming = SCHEDULES["official"].next_after(now)
    run = files(data.official_run)
    if run.problem or run.empty:
        missing = run.problem is None or run.problem.startswith("No daily run")
        message = "No daily run on this host yet." if missing else run.problem
        body = view.text(f"{message} Scheduled at **{view.clock(upcoming, now)} UTC** "
                         f"({view.until(upcoming, now)}), until 04:30 UTC.")
        badge = view.chip("not run yet" if missing else "unavailable")
        return view.card("Official files", body, badge=badge, lead=view.dot("dim"))
    frame = run.frame
    first = frame.iloc[0]
    started, deadline = moment(first["run_started_at"]), moment(first["run_until"])
    ended = moment(first["run_ended_at"])
    missing = frame.loc[frame["requests"].isna(), "source"]
    asked = int(frame["requests"].sum()) if missing.empty else None
    unavailable = "Crawl progress unavailable for " + ", ".join(missing.dropna())
    if ended is None and deadline is not None and deadline > now:
        live = frame.loc[frame["status"] == "running"]
        current = live.iloc[-1] if len(live) else None
        last_request = moment(live["last_request"].max()) if len(live) and live["last_request"].notna().any() else None
        asking = last_request is not None and timedelta(0) <= now - last_request < ASKING
        what = OFFICIAL_SOURCES.get(current["source"], current["source"]) if current is not None else "Between sources"
        share = (now - started).total_seconds() / max((deadline - started).total_seconds(), 1)
        body = view.text(what)
        if asked is None:
            body += view.text(unavailable)
        else:
            number = watched_number(f"{page}-requests", asked, first["run_id"])
            body += (f'<div class="ss-big" aria-label="{asked:,} recorded requests">{number}'
                     '<small>recorded requests this run</small></div>')
            if last_request is not None:
                body += view.text(f"Last request {view.ago(last_request, now)} ({view.clock(last_request, now)} UTC)")
            breakdown = ", ".join(f"{int(frame[name].sum()):,} {name}" for name in data.OFFICIAL_COUNTS[:5]
                                  if frame[name].sum()) or "0 changes"
            body += view.text(f"**{breakdown}** so far. The feed check announces entries after a successful publish.")
        body += view.meter(share, label="Official run window elapsed")
        body += view.text(f"Window {view.clock(started, now)} to {view.clock(deadline, now)} UTC "
                          f"({view.until(deadline, now)})")
        return view.card("Official files", body, badge=view.chip("running", "accent"), live=True,
                         lead=view.pulse(now) if asking else view.dot("accent"))
    if ended is None:
        badge = view.chip("stopped unrecorded", "warning")
        stop = "stopped without recording its end"
    else:
        reason = plain(first["run_stop"])
        badge = view.chip(reason or "ended", RUN_TONES.get(reason, "muted"))
        stop = f"{view.clock(started, now)} to {view.clock(ended, now)} UTC"
    summary = unavailable
    if asked is not None:
        changed = int(frame[list(data.OFFICIAL_COUNTS[:5])].sum().sum())
        checked = int(frame["checked"].sum())
        summary = f"**{asked:,}** requests, **{changed:,}** changed, **{checked:,}** confirmed unchanged"
    lines = [f"Last run {stop}: {summary}",
             f"Next run **{view.clock(upcoming, now)} UTC** ({view.until(upcoming, now)}), until 04:30 UTC"]
    return view.card("Official files", "".join(view.text(line) for line in lines), badge=badge,
                     lead=view.dot(RUN_TONES.get(plain(first["run_stop"]), "muted")))


def feed_card(now: datetime, page: str) -> str:
    """The feed for SafeStart: its newest manifest, the latest attempts to publish one, and a warning tone when it is
    stale or the last attempt failed."""
    feed = files(data.official_feed)
    history = slow(data.feed_history)
    if feed.problem or feed.empty:
        body = view.text(feed.problem or "No feed on this host yet.")
        return view.card("Feed for SafeStart", body, badge=view.chip("unavailable"), lead=view.dot("dim"))
    row = feed.frame.iloc[0]
    attempts = history.frame if not history.problem else pd.DataFrame()
    failed = row["outcome"] in ("error", "unsigned")
    parts = []
    if not pd.isna(row["sequence"]):
        generated = moment(row["generated_at"])
        sequence = int(row["sequence"])
        number = watched_number(f"{page}-manifest", sequence)
        parts.append(f'<div class="ss-big" aria-label="Manifest {sequence:,}">#{number}'
                     f'<small title="{view.stamp(generated)}">manifest · generated {view.ago(generated, now)}'
                     '</small></div>')
        window = window_words(row["window_start"], row["window_end"], now)
        parts.append(view.text(f"Window {window} · {int(row['changes']):,} changes, "
                               f"{int(row['checked']):,} confirmed unchanged"))
        tone, word = ("warning", "stale") if row["stale"] else ("good", "fresh")
    else:
        parts.append(view.text(plain(row["problem"]) or "The feed's newest manifest cannot be read."))
        tone, word = "warning", "unreadable"
    if failed:
        tone, word = ("critical", "publish failed") if row["outcome"] == "error" else ("warning", "unsigned")
        attempt = moment(row["attempted_at"])
        when = f"{view.ago(attempt, now)} ({view.stamp(attempt)})"
        parts.append(view.text(f"Latest attempt {when}: {plain(row['note'])}"))
        if row["stale"] and not pd.isna(row["sequence"]):
            parts.append(view.chip(f"Manifest older than {STALE_HOURS} hours", "warning"))
    if history.problem:
        parts.append(view.text(history.problem))
    if len(attempts):
        parts.append(view.text(f"Changes per publish · last {len(attempts)} attempts"))
        classes = [("failed" if outcome == "error" else "unsigned" if outcome == "unsigned" else "")
                   for outcome in attempts["outcome"]]
        classes[-1] = classes[-1] or "last"
        titles = [f"{view.stamp(moment(row.attempted_at))} · {row.outcome}"
                  + (f" · {row.changes:,} changes" if row.outcome == "published" else "")
                  for row in attempts.itertuples()]
        parts.append(view.bars([int(value) for value in attempts["changes"]], titles, classes=classes,
                               label=f"Changes listed by the last {len(attempts)} publishes"))
    return view.card("Feed for SafeStart", "".join(parts), badge=view.chip(word, tone), lead=view.dot(tone))


def window_words(start: Any, end: Any, now: datetime) -> str:
    """A feed window as people read it: 16:30 to 04:30 UTC, with dates when not today."""
    begins, ends = moment(pd.Timestamp(start)), moment(pd.Timestamp(end))
    if begins is None or ends is None:
        return ""
    if begins == ends:
        return f"{view.clock(begins, now)} UTC (empty)"
    return f"{view.clock(begins, now)} to {view.clock(ends, now)} UTC"


def schedule_card(now: datetime) -> str:
    """When each watcher check last ran and is next due, and when each flow runs next."""
    table = stream(data.source_freshness)
    checks = newest_checks(table)
    items = []
    for name in SCHEDULE:
        check = checks.get(name)
        if check is None:
            status = "unavailable" if table.problem else "not checked yet"
            items.append(view.Line("dim", f"{CHECKS[name]} · {status}", ""))
            continue
        status = check["status"]
        when = check["next"]
        items.append(view.Line(
            CHECK_TONES.get(status, "muted"), f"{CHECKS[name]} · {status} {view.ago(check['checked_at'], now)}",
            next_words(check, now),
            title=f"Last {view.stamp(check['checked_at'])}. Next about {view.stamp(when)}. {check['note']}",
            recent=timedelta(0) <= now - check["checked_at"] < ASKING, soon=when is not None and when >= now))
    stamp = data.warehouse_stamp()
    built = datetime.fromtimestamp(stamp[1] / 1e9, UTC) if stamp else None
    items.append(view.Line("dim", "Prefect flows", "", separator=True))
    for name, what, last in (("batch", "Batch: warehouse built", built), ("model", "Model training", None),
                             ("official", "Official files", None)):
        upcoming = SCHEDULES[name].next_after(now)
        label = f"{what} {view.ago(last, now)}" if last else what
        items.append(view.Line("muted", label, f"{upcoming:%a %H:%M} UTC", title=f"Next run {view.stamp(upcoming)}"))
    body = (view.text(table.problem) if table.problem else "") + view.lines(items, now)
    return view.card("Next checks and runs", body, badge=view.chip(f"{now:%H:%M} UTC", "muted"),
                     lead=view.dot("accent"))


@st.fragment(run_every=SLOW_SECONDS)
def activity_panel(mode: str | None) -> None:
    st.subheader("Events received", anchor=False)
    span = st.segmented_control("Window", list(ACTIVITY), default="48 h", required=True, key="activity_span",
                                label_visibility="collapsed")
    grain, minutes = ACTIVITY[span]
    table = (stream if grain == "minute" else slow)(data.arrivals, mode, minutes, grain)
    if table.problem:
        st.info(table.problem, icon=":material/info:")
        return
    now = pd.Timestamp.now(tz="UTC")
    start = now - pd.Timedelta(minutes=minutes)
    modes = ("replay", "live") if mode is None else (mode,)
    for shown_mode in modes:
        rows = table.frame if table.empty else table.frame[table.frame["mode"] == shown_mode]
        if rows.empty:
            st.caption(f"No {shown_mode} events received in the last {span}.")
        else:
            show_chart(charts.activity(rows, grain, start, now))
    provenance(f"Events received per {grain} (UTC), from `stream.events` in PostgreSQL. The replay sends the history "
               "once per start of the stack; live events come from the watchers.")


@st.fragment(run_every=REFRESH_SECONDS)
def latest_panel(mode: str | None) -> None:
    st.subheader("Newest events", anchor=False)
    events = stream(data.latest_events, mode)
    if not shown(events, "No events yet."):
        if not events.problem:
            st.session_state.setdefault("feed_seen", {})[mode or "all"] = datetime.min.replace(tzinfo=UTC)
        return
    now = datetime.now(UTC)
    frame = events.frame
    seen = st.session_state.setdefault("feed_seen", {})
    last_seen = seen.get(mode or "all")
    rows = []
    for row in frame.head(12).itertuples():
        received = moment(row.received_at)
        occurred = moment(row.occurred_at)
        facts = [f"{occurred:%Y-%m-%d}" if occurred else "", plain(row.district), plain(row.state)]
        rows.append(view.EventRow(
            label=KIND_LABELS.get(str(row.kind), str(row.kind)), color=KIND_COLORS.get(str(row.kind), "#8a93a0"),
            title=plain(row.title), facts=[fact for fact in facts if fact], mode=str(row.mode),
            received=view.ago(received, now), received_at=view.stamp(received),
            fresh=last_seen is not None and received is not None and received > last_seen))
    st.html(view.event_list(rows))
    newest = moment(frame["received_at"].max())
    if newest is not None:
        seen[mode or "all"] = newest
    with st.expander(f"Table view ({len(frame)} newest)"):
        table = frame.assign(kind=frame["kind"].map(KIND_LABELS),
                             district=frame["district"].fillna(""), state=frame["state"].fillna(""))
        st.dataframe(
            table[["kind", "title", "occurred_at", "district", "state", "mode", "received_at"]], hide_index=True,
            column_config={
                "kind": st.column_config.TextColumn("Kind", pinned=True),
                "title": st.column_config.TextColumn("Event", width="large"),
                "occurred_at": st.column_config.DateColumn("Occurred"),
                "district": st.column_config.TextColumn("District", width="small"),
                "state": st.column_config.TextColumn("State", width="small"),
                "mode": st.column_config.TextColumn("Mode", width="small"),
                "received_at": st.column_config.DatetimeColumn("Received (UTC)", format="YYYY-MM-DD HH:mm:ss",
                                                              timezone="UTC"),
            },
        )
    provenance(f"The newest events by arrival, from `stream.events`; refreshed every {REFRESH_SECONDS} s.")


PUBLISH_TONES = {"published": "good", "unsigned": "warning", "error": "critical"}


def heartbeat_marks(now: datetime) -> tuple[pd.DataFrame, pd.DataFrame, str | None]:
    """The heartbeat's marks and bars (charts.heartbeat), and the streaming store's problem, if any."""
    start, end = now - timedelta(hours=HEARTBEAT_HOURS), now + timedelta(hours=AHEAD_HOURS)
    marks: list[dict[str, Any]] = []
    spans: list[dict[str, Any]] = []
    checks = slow(data.check_history, HEARTBEAT_HOURS)
    if not checks.problem:
        for row in checks.frame.itertuples():
            if row.source in CHECKS:
                found = f", {int(row.found):,} new" if row.found else ""
                marks.append({"row": CHECKS[row.source], "at": row.checked_at, "shape": "check",
                              "tone": CHECK_TONES.get(row.status, "muted"), "what": f"{CHECKS[row.source]} check",
                              "when": f"{moment(row.checked_at):%Y-%m-%d %H:%M}", "status": f"{row.status}{found}",
                              "note": plain(row.note)})
    for name, check in newest_checks(stream(data.source_freshness)).items():
        due = check["next"]
        if name in CHECKS and due is not None and start <= due <= end:
            marks.append({"row": CHECKS[name], "at": due, "shape": "due", "tone": "accent",
                          "what": f"{CHECKS[name]} check, next", "when": f"{due:%Y-%m-%d %H:%M}",
                          "status": "due" if due <= now else "estimated", "note": f"every {SCHEDULE[name]}"})
    runs = files(data.official_runs, HEARTBEAT_HOURS)
    if not runs.problem:
        for row in runs.frame.itertuples():
            begins, ended, until = moment(row.started_at), moment(row.ended_at), moment(row.until)
            running = ended is None and until is not None and until > now
            if ended is None and not running:
                if start <= begins <= now:
                    marks.append({"row": "Official run", "at": begins, "shape": "start", "tone": "warning",
                                  "what": "Official run started", "when": view.stamp(begins),
                                  "status": "no end recorded", "note": f"Window closed {view.stamp(until)}"})
                continue
            finish = ended or now
            spans.append({"row": "Official run", "begins": max(begins, start), "ends": max(finish, begins),
                          "tone": "accent" if running else RUN_TONES.get(plain(row.stop_reason), "warning"),
                          "what": "Daily update of the official files",
                          "when": f"{begins:%Y-%m-%d %H:%M} to {finish:%H:%M}",
                          "status": "running" if running else plain(row.stop_reason) or "no end recorded"})
    publishes = slow(data.feed_history, None, HEARTBEAT_HOURS)
    if not publishes.problem:
        for row in publishes.frame.itertuples():
            at = moment(row.attempted_at)
            if at is not None and at >= start:
                manifests = (f"manifest {int(row.first_sequence)}" if row.first_sequence == row.last_sequence else
                             f"manifests {int(row.first_sequence)} to {int(row.last_sequence)}"
                             ) if not pd.isna(row.first_sequence) else "nothing written"
                marks.append({"row": "Feed manifests", "at": at, "shape": "publish",
                              "tone": PUBLISH_TONES.get(row.outcome, "muted"), "what": "Feed publish",
                              "when": f"{at:%Y-%m-%d %H:%M}", "status": f"{row.outcome}: {manifests}",
                              "note": f"{int(row.changes):,} changed, {int(row.checked):,} confirmed unchanged"})
    stamp = data.warehouse_stamp()
    if stamp:
        built = datetime.fromtimestamp(stamp[1] / 1e9, UTC)
        if start <= built <= now:
            marks.append({"row": "Batch flow", "at": built, "shape": "build", "tone": "good",
                          "what": "Warehouse built", "when": f"{built:%Y-%m-%d %H:%M}", "status": "built",
                          "note": "the batch flow's last build"})
    for name, row in (("batch", "Batch flow"), ("official", "Official run")):
        upcoming = SCHEDULES[name].next_after(now)
        if upcoming <= end:
            marks.append({"row": row, "at": upcoming, "shape": "due", "tone": "accent", "what": f"{row}, next",
                          "when": f"{upcoming:%Y-%m-%d %H:%M}", "status": "scheduled",
                          "note": f"cron {SCHEDULES[name].cron} (UTC)"})
    columns = ["row", "at", "shape", "tone", "what", "when", "status", "note"]
    frame = pd.DataFrame(marks, columns=columns)
    if len(frame):
        frame["at"] = pd.to_datetime(frame["at"], utc=True)
    bars = pd.DataFrame(spans, columns=["row", "begins", "ends", "tone", "what", "when", "status"])
    for column in ("begins", "ends"):
        if len(bars):
            bars[column] = pd.to_datetime(bars[column], utc=True)
    problems = [checks.problem]
    for table in (runs, publishes):
        if table.problem and not table.problem.startswith(("No daily run", "No feed for")):
            problems.append(table.problem)
    return frame, bars, " ".join(problem for problem in problems if problem) or None


@st.fragment(run_every=SLOW_SECONDS)
def heartbeat_panel() -> None:
    st.subheader("Pipeline heartbeat", anchor=False)
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    marks, spans, problem = heartbeat_marks(now)
    if problem:
        st.info(problem, icon=":material/info:")
    if spans.empty and not (marks["shape"] != "due").any():
        st.caption("No activity available for the last 48 hours." if problem else
                   "No check or run recorded in the last two days.")
    if marks.empty and spans.empty:
        return
    stamp = pd.Timestamp(now)
    show_chart(charts.heartbeat(marks, spans, stamp - pd.Timedelta(hours=HEARTBEAT_HOURS), stamp,
                                stamp + pd.Timedelta(hours=AHEAD_HOURS)))
    legend = (view.chip("found new or done", "good", dot="good") + view.chip("unchanged", "muted", dot="muted")
              + view.chip("refused or unsigned", "warning", dot="warning")
              + view.chip("error", "critical", dot="critical") + view.chip("skipped", "muted", dot="dim")
              + view.chip("running, or next due", "accent", dot="accent"))
    st.html(f'<div class="ss-row">{legend}</div>')
    provenance("The last 48 hours and the next 12 (UTC): every watcher check from `stream.source_checks`, the official "
               "runs and the feed's publishes from `state/official.sqlite`, the warehouse's build time, and the next "
               "times from the watchers' and Prefect flows' schedules.")


# The other pages


def enforcement_page() -> None:
    records, periods = st.columns(2)
    kind = records.segmented_control("Records", ENFORCEMENT_ORDER, default="violation", required=True,
                                     format_func=KIND_LABELS.get, key="enforcement_kind")
    period = periods.segmented_control("Period", list(PERIODS), default="1 year", required=True,
                                       key="enforcement_period")
    days, grain = PERIODS[period]
    label = KIND_LABELS[kind]
    st.subheader(f"{label} by {grain}", anchor=False)
    counts = stream(data.daily_counts, kind, days, grain)
    if shown(counts, f"No {label.lower()} counted yet: the analytics consumer keeps these counts."):
        show_chart(charts.counts_over_time(counts.frame, kind, grain))
        span = "all days" if period == "All" else f"last {period}"
        show_chart(charts.counts_by_district(counts.frame, kind, span))
        provenance("Records per day they happened, from `stream.daily_counts`, which the analytics consumer keeps "
                   "from the Kafka stream, each event counted once.")

    st.subheader("Observed and forecast", anchor=False)
    districts = stream(data.forecast_districts, kind)
    if shown(districts, "No forecasts yet: the analytics consumer writes one for each complete day."):
        options = list(districts.frame["district"])
        district = st.selectbox("District", options, format_func=lambda name: name or NO_DISTRICT)
        series = stream(data.forecasts, kind, district, min(days, FORECAST_DAYS))
        if shown(series, "No forecasts for this district in the period."):
            show_chart(charts.forecast(series.frame, kind))
            methods = ", ".join(sorted(set(series.frame["method"].astype(str))))
            st.caption(f"One-step-ahead forecast of each day ({md(methods)}); red points are the days the analytics "
                       "consumer flagged as anomalies.")
        anomalies = stream(data.recent_anomalies, kind)
        if shown(anomalies, f"No anomalies flagged for {label.lower()}."):
            st.markdown("**Latest anomalies, all districts**")
            st.dataframe(anomalies.frame.assign(district=anomalies.frame["district"].replace("", NO_DISTRICT)),
                         hide_index=True, column_order=["day", "district", "observed", "predicted", "residual"],
                         column_config={
                             "day": st.column_config.DateColumn("Day", pinned=True),
                             "district": "District",
                             "observed": st.column_config.NumberColumn("Observed", format=COUNT_FORMAT),
                             "predicted": st.column_config.NumberColumn("Forecast", format="%.1f"),
                             "residual": st.column_config.NumberColumn("Residual", format="%+.1f")})
        provenance("Forecasts and anomalies from `stream.forecasts` (an EWMA model the analytics consumer fits).")

    st.subheader("Districts by year", anchor=False)
    history = warehouse(data.district_years)
    if shown(history, "No district history in the warehouse."):
        measure = st.segmented_control("Measure", list(MEASURES), default="violations", required=True,
                                       format_func=MEASURES.get, key="history_measure")
        show_chart(charts.district_heatmap(history.frame, measure, f"{MEASURES[measure]} per year"))
        provenance("From `mart_district_year` in the DuckDB warehouse.")


def standards_page() -> None:
    st.subheader("Most cited standards", anchor=False)
    top = warehouse(data.top_cited_sections, 100)
    if shown(top, "No citations in the warehouse."):
        show_chart(charts.top_sections(top.frame.head(15)))
        st.caption("The 100 most cited sections of 30 CFR. Select a row to read the section.")
        show_section(section_picker(top.frame, key="standards_sections", height=320))
        provenance("From `mart_top_cited_sections` joined to `cfr_section` (the eCFR text) in the DuckDB warehouse.")

    st.subheader("S&S share of violations", anchor=False)
    trend = warehouse(data.ss_trend)
    if shown(trend, "No violations in the warehouse."):
        show_chart(charts.ss_trend(trend.frame))
        st.caption("S&S (significant and substantial): a violation reasonably likely to lead to a serious injury or "
                   f"illness. Years with fewer than {charts.TREND_MIN_VIOLATIONS} violations are left out.")
        provenance("From `mart_ss_trend` in the DuckDB warehouse.")


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
        label = (f"The first {len(frame)} matches; type more of the name to narrow them"
                 if len(frame) >= data.MINE_MATCHES else f"{len(frame)} mines match")
        mine_id = st.selectbox(label, list(labels), format_func=labels.get)
    mine_brief(mine_id)


def mine_brief(mine_id: str) -> None:
    card = warehouse(data.mine_scorecard, mine_id)
    if not shown(card, f"Mine {mine_id} has no scorecard in the warehouse."):
        return
    mine = card.frame.iloc[0]
    details = warehouse(data.mine_details, mine_id)
    extra = details.frame.iloc[0] if not details.empty else pd.Series(dtype=object)
    st.subheader(md(plain(mine["mine_name"])) or f"Mine {mine_id}", anchor=False)
    place = ", ".join(part for part in (plain(extra.get("county")), plain(mine["state"])) if part)
    district = plain(mine["district"])
    facts = [f"ID {mine_id}", COMMODITY_LABELS.get(plain(mine["coal_metal"]), ""), plain(mine["mine_type"]),
             plain(mine["status"]), place, f"district {district}" if district else "",
             plain(extra.get("operator_name"))]
    st.caption(" · ".join(md(fact) for fact in facts if fact))
    window = warehouse(data.twelve_months)
    if not window.problem and not window.empty and not pd.isna(window.frame.iloc[0]["until"]):
        row = window.frame.iloc[0]
        text = (f"Inspections, violations, penalties and accidents cover the 12 months from {row['since']:%Y-%m-%d} "
                f"to {row['until']:%Y-%m-%d}, the newest records")
        if not pd.isna(row["rate_last_quarter"]):
            last = int(row["rate_last_quarter"])
            text += (f"; hours worked and the injury rate cover {quarter_label(last - 3)} to {quarter_label(last)}, "
                     "the newest quarters with reported hours")
        st.caption(text + ".")

    with tiles(4):
        tile("Inspections", count(mine["inspections_12m"]))
        tile("Violations", count(mine["violations_12m"]))
        tile("S&S violations", count(mine["ss_violations_12m"]))
        tile("Penalties proposed", money(mine["proposed_penalty_12m"]))
        tile("Accidents", count(mine["accidents_12m"]))
        tile("Serious accidents", count(mine["serious_accidents_12m"]), help=SERIOUS_HELP)
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
    provenance("From `mart_mine_scorecard`, `violation`, `cfr_section` and `accident` in the DuckDB warehouse.")


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
    official_files()

    st.subheader("Live source checks", anchor=False)
    checks = stream(data.source_freshness)
    if shown(checks, "No live checks yet: start the watchers."):
        now = datetime.now(UTC)
        failing = checks.frame.loc[checks.frame["status"] == "error", "source"]
        if len(failing):
            st.warning("The newest check failed for " + ", ".join(md(source) for source in failing) + ".",
                       icon=":material/warning:")
        refused = [md(source) for source in checks.frame.loc[checks.frame["status"] == "refused", "source"]]
        if refused:
            st.info((f"The newest check of {refused[0]} was refused by the host" if len(refused) == 1 else
                     f"The newest checks of {', '.join(refused)} were refused by their hosts")
                    + "; refusals are recorded, never worked around.", icon=":material/info:")
        latest = newest_checks(checks)
        frame = checks.frame.assign(
            age=checks.frame["checked_at"].map(lambda at: f"{view.clock(at, now)} UTC · {ago(at, now)}"),
            next=[f"{view.clock(due, now)} UTC · {view.until(due, now)}" if due else ""
                  for due in (latest[name]["next"] for name in checks.frame["source"])])
        st.dataframe(frame, hide_index=True, row_height=72,
                     column_order=["source", "age", "next", "status", "found", "checks_24h", "errors_24h", "found_24h",
                                   "note"],
                     column_config={
                         "source": st.column_config.TextColumn("Source", pinned=True),
                         "age": "Last checked",
                         "next": "Next check (about)",
                         "status": "Status",
                         "found": st.column_config.NumberColumn("Found", format=COUNT_FORMAT),
                         "checks_24h": st.column_config.NumberColumn("Checks (24 h)", format=COUNT_FORMAT),
                         "errors_24h": st.column_config.NumberColumn("Errors (24 h)", format=COUNT_FORMAT),
                         "found_24h": st.column_config.NumberColumn("Found (24 h)", format=COUNT_FORMAT),
                         "note": st.column_config.TextColumn("Note", width="large")})
        provenance("Each watcher's newest check and its last 24 hours, from `stream.source_checks`; the Live page's "
                   "heartbeat draws every check of the last two days.")

    st.subheader("Datasets", anchor=False)
    versions = warehouse(data.dataset_versions)
    if shown(versions, "No dataset versions recorded in the warehouse."):
        st.dataframe(versions.frame, hide_index=True, column_config={
            "dataset": st.column_config.TextColumn("Dataset", pinned=True),
            "versions": st.column_config.NumberColumn("Versions kept", format=COUNT_FORMAT),
            "version": "Current version",
            "rows": st.column_config.NumberColumn("Rows", format=COUNT_FORMAT),
            "built_at": st.column_config.DatetimeColumn("Built (UTC)", format="YYYY-MM-DD HH:mm"),
            "is_current": st.column_config.CheckboxColumn("Current"),
        })
        provenance("MSHA's 20 open datasets as the Parquet lake keeps them, current version and versions kept, "
                   "from `dataset_version`.")

    st.subheader("Downloaded documents", anchor=False)
    documents = warehouse(data.document_counts)
    if shown(documents, "No downloaded documents recorded in the warehouse."):
        total = int(documents.frame["documents"].sum())
        held = warehouse(data.document_files)
        distinct = ("" if held.problem or held.empty else
                    f": {int(held.frame.iloc[0]['files']):,} distinct files, "
                    f"{float(held.frame.iloc[0]['bytes']) / 1e9:,.1f} GB")
        st.caption(f"{total:,} URLs downloaded from the MSHA and OSHA websites{distinct}.")
        show_chart(charts.documents(documents.frame))
        provenance("From `document` in the DuckDB warehouse: the crawls' inventories, one row per URL.")


@st.fragment(run_every=NOW_SECONDS)
def official_files() -> None:
    """The daily update of the official files: when it last ran, what it found, the hosts that refused it, its newest
    changes, and the feed for SafeStart, with a warning when its newest manifest is older than STALE_HOURS or the last
    publish failed."""
    st.subheader("Official files", anchor=False)
    now = datetime.now(UTC)
    st.html(view.cards([official_card(now, "Sources"), feed_card(now, "Sources")], columns=2))
    run = files(data.official_run)
    if shown(run, "The latest run of the official files asked no source."):
        frame = run.frame
        first = frame.iloc[0]
        started = first["run_started_at"]
        if not pd.isna(first["run_ended_at"]):
            ended = f"ended {ago(first['run_ended_at'], now)} ({md(first['run_stop'])})"
        elif first["run_until"] < now:  # past its deadline with no end recorded: its process stopped or died
            ended = "stopped without recording its end (the next run goes on from what it recorded)"
        else:
            ended = "is still running"
        found = "It has found so far:" if ended == "is still running" else "It found:"
        st.caption(f"The daily update of the official files last started {ago(started, now)} "
                   f"({started:%Y-%m-%d %H:%M} UTC) and {ended}. {found}")
        active = frame["live"] & (frame["run_until"] > now) & frame["run_ended_at"].isna()
        labels = dict(OFFICIAL_LABELS)
        if ended == "is still running" and not frame["ended_at"].notna().any():
            labels.pop("checked")
        if active.any():
            labels.pop("checked", None)
            labels["not_modified"] = "Answered 304 (active source)"
        with tiles(len(labels), minimum=110):
            for name, label in labels.items():
                tile(label, count(frame[name].sum(skipna=False)))
        if active.any() and frame["ended_at"].notna().any():
            st.caption(f"Completed sources confirmed {int(frame['checked'].sum()):,} unchanged.")
        refused = [md(host) for host in frame.loc[frame["status"] == "refused", "host"]]
        if refused:
            st.info(f"{', '.join(refused)} refused the run's requests; refusals are recorded, never worked around.",
                    icon=":material/info:")
        sources = frame.loc[frame["source"].notna()].copy()
        sources["status"] = [f"● {status}" if status != "running" or ended == "is still running"
                             else "○ no end recorded" for status in sources["status"]]
        if not sources.empty:
            st.dataframe(sources, hide_index=True,
                         column_order=["source", "status", "requests", "started_at", "ended_at", "note"],
                         column_config={"source": st.column_config.TextColumn("Source", pinned=True),
                                        "status": "Status",
                                        "requests": st.column_config.NumberColumn("Recorded requests",
                                                                                  format=COUNT_FORMAT),
                                        "started_at": st.column_config.DatetimeColumn("Started (UTC)", timezone="UTC"),
                                        "ended_at": st.column_config.DatetimeColumn("Ended (UTC)", timezone="UTC"),
                                        "note": st.column_config.TextColumn("Note", width="large")})
        newest = files(data.official_changes)
        if newest.problem:
            st.info(newest.problem, icon=":material/info:")
        elif not newest.empty:
            st.markdown("**Newest changes this run found**")
            st.html(view.event_list(view.EventRow(
                label=change.change, color=charts.TONES[CHANGE_TONES.get(change.change, "muted")],
                title=change.url, facts=[OFFICIAL_SOURCES.get(change.source, change.source),
                                         plain(change.detected_type)],
                mode="", received=view.ago(moment(change.observed_at), now),
                received_at=view.stamp(moment(change.observed_at)), link=change.url)
                for change in newest.frame.itertuples()))
        provenance("From `state/official.sqlite` (the runs and their sources) and the crawl stores it writes "
                   "(`state/crawl-<site>.sqlite`: its requests and changes).")
    feed = files(data.official_feed)
    if shown(feed, ""):
        row = feed.frame.iloc[0]
        if not pd.isna(row["sequence"]):
            st.caption(f"Feed for SafeStart (/feed/official/latest.json): manifest {int(row['sequence']):,}, "
                       f"generated {ago(row['generated_at'], now)}, for "
                       f"{window_words(row['window_start'], row['window_end'], now)}.")
            if row["stale"]:
                st.warning(f"The feed's newest manifest is {row['age_hours']:,.0f} hours old, more than {STALE_HOURS}: "
                           "no daily run has published since. SafeStart's planned importer is to treat the feed as "
                           "stale until one does.", icon=":material/warning:")
        elif isinstance(row["problem"], str):
            st.warning(md(row["problem"]), icon=":material/warning:")
        if row["outcome"] in ("unsigned", "error"):
            what = "published nothing" if row["outcome"] == "unsigned" else "failed"
            st.warning(f"The latest attempt to publish the feed, {ago(row['attempted_at'], now)}, {what}: "
                       f"{md(row['note'])}.", icon=":material/warning:")
    purges = files(data.official_purges)
    if not purges.problem and not purges.empty:
        frame = purges.frame
        when = ago(frame["purged_at"].max(), now)
        # A site with nothing to purge here (no inventory, one of the first version such as www.msha.gov's, or its
        # crawl holding the lock) is skipped; the others say whether the purge deleted or only reported.
        acted = frame.loc[frame["outcome"] != "skipped"]
        if acted.empty:
            st.caption(f"Latest purge of retired originals on this host ({when}): every site skipped ("
                       + "; ".join(f"{md(row.site)}: {md(row.note)}" for row in frame.itertuples()) + ").")
        elif (acted["outcome"] == "dry_run").all():
            st.caption(f"Latest purge of retired originals on this host ({when}): a dry run, nothing deleted; only the "
                       "server that signs the feed deletes them.")
        else:
            st.caption(f"Latest purge of retired originals on this host ({when}): {int(frame['deleted'].sum()):,} "
                       f"files deleted, {frame['deleted_bytes'].sum() / 1e6:,.1f} MB, 30 days after every URL that "
                       "referenced them was gone.")
        for failed in frame.loc[frame["outcome"] == "error"].itertuples():
            st.warning(f"The latest purge of {failed.site} failed: {md(failed.note)}.", icon=":material/warning:")


def header() -> None:
    stamp = data.warehouse_stamp()
    built = (f"Warehouse built {datetime.fromtimestamp(stamp[1] / 1e9, UTC):%Y-%m-%d %H:%M} UTC" if stamp
             else "No warehouse yet")
    st.html(f'<header class="ss-mast"><img class="ss-terrain" alt="" width="450" height="184" '
            f'src="data:image/svg+xml;base64,{TERRAIN_IMAGE}"><div class="ss-mast-copy">'
            '<p class="ss-domain">MSHA enforcement / 30 CFR / Official files</p>'
            f'<h1>SafeStart safety data</h1><p class="ss-build">{built}</p></div></header>')


def main() -> None:
    st.set_page_config(page_title="SafeStart safety data", page_icon=":material/health_and_safety:", layout="wide")
    st.html(f"<style>{THEME_CSS}</style>")
    header()
    tabs = st.tabs(PAGES, key="page", on_change="rerun", bind="query-params")
    for tab, page in zip(tabs, (live_page, enforcement_page, standards_page, mine_page, sources_page)):
        if tab.open is not False:
            with tab:
                page()


if __name__ == "__main__":
    main()
