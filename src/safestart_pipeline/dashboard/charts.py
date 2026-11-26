"""Altair charts for the dashboard, drawn from one palette so that an entity keeps its color on every page.

Each event kind keeps its hue across tiles, trends and tooltips. Midtone series colors work on the ice and navy
chart surfaces. Forecasts have a dashed stroke, anomalies have point markers, and the heatmap has a quantitative
legend. Streamlit supplies theme-aware axis and legend text. Activity and heartbeat use UTC in every browser.
"""

from __future__ import annotations

import altair as alt
import pandas as pd

BLUE, ORANGE, AQUA, YELLOW, MAGENTA, GREEN, VIOLET = (
    "#348cd1", "#d17329", "#119881", "#ab891b", "#c562ab", "#739533", "#8f76d4")
CATEGORICAL = (BLUE, ORANGE, AQUA, YELLOW, MAGENTA, GREEN, VIOLET)
SERIOUS = "#ca556d"
CRITICAL = "#d54c54"
MUTED = "#76869a"
SEQUENTIAL = ("#233951", "#23798a", "#75dfce")
SURFACE = "#102434"  # fine keyline between adjacent marks
ACCENT = "#188e99"  # the current time on the timeline
# Status colors with the words that go with them (the dashboard always prints the status beside its color).
TONES = {"good": AQUA, "muted": MUTED, "warning": YELLOW, "serious": SERIOUS, "critical": CRITICAL,
         "dim": "#607389", "accent": ACCENT}

KIND_ORDER = ("inspection", "violation", "accident", "order_107a", "dataset_version", "page_change", "regulation")
KIND_LABELS = {
    "inspection": "Inspections", "violation": "Violations", "accident": "Accidents", "order_107a": "107(a) orders",
    "dataset_version": "Dataset versions", "page_change": "Page changes", "regulation": "Regulations",
}
KIND_COLORS = dict(zip(KIND_ORDER, CATEGORICAL))
SERIOUS_LABELS = {"violation": "S&S violations", "accident": "Serious accidents"}
COMMODITY_LABELS = {"C": "Coal", "M": "Metal/nonmetal"}
COMMODITY_COLORS = {"Coal": BLUE, "Metal/nonmetal": ORANGE}
NO_DISTRICT = "No district"
HEIGHT = 300
ROW_HEIGHT = 28  # per bar or heatmap row, leaving room for every label
TREND_MIN_VIOLATIONS = 100  # a yearly share needs at least this many violations behind it


def _top_legend(columns: int | None = None) -> alt.Legend:
    """A legend above the chart; `columns` wraps a long one so that it fits a phone's width."""
    return alt.Legend(orient="top", title=None, direction="horizontal", **({"columns": columns} if columns else {}))


def _every_label(title: str | None) -> alt.Axis:
    """A category axis that labels every bar or row, however narrow the screen."""
    return alt.Axis(title=title, labelOverlap=False)


def _fixed_colors(domain: list[str], colors: dict[str, str]) -> alt.Scale:
    """A color scale for the entities present, each keeping its own color whatever else is shown."""
    return alt.Scale(domain=domain, range=[colors[name] for name in domain])


STEPS = {"minute": pd.Timedelta(minutes=1), "hour": pd.Timedelta(hours=1), "day": pd.Timedelta(days=1)}
PERIOD_FORMATS = {"minute": "%Y-%m-%d %H:%M", "hour": "%Y-%m-%d %H:00", "day": "%Y-%m-%d"}


def _time_axis(start: pd.Timestamp, end: pd.Timestamp) -> alt.Axis:
    """A UTC time axis: the date at each midnight, the hour between, as wide as the screen allows."""
    days = (end - start) / pd.Timedelta(days=1)
    if days <= 0.25:
        return alt.Axis(title=None, format="%H:%M", labelAngle=0, grid=False)
    return alt.Axis(title=None, labelAngle=0, grid=False, labelOverlap=True, labelExpr=(
        "utchours(datum.value) == 0 && utcminutes(datum.value) == 0 ? utcFormat(datum.value, '%b %d') "
        ": utcFormat(datum.value, '%H:%M')"), tickCount=8 if days < 3 else 7)


def activity(frame: pd.DataFrame, grain: str, start: pd.Timestamp, end: pd.Timestamp) -> alt.Chart:
    """One mode, stacked by kind on the shared UTC window, with its own linear count scale."""
    present = [kind for kind in KIND_ORDER if kind in set(frame["kind"])]
    labels = [KIND_LABELS[kind] for kind in present]
    begins = pd.to_datetime(frame["at"], utc=True)
    data = frame.assign(kind=frame["kind"].map(KIND_LABELS),
                        begins=begins.clip(lower=start), ends=(begins + STEPS[grain]).clip(upper=end),
                        period=begins.dt.strftime(PERIOD_FORMATS[grain]) + " UTC")
    domain = [start.isoformat(), end.isoformat()]
    stacked = alt.Chart(data).transform_stack(
        stack="events", groupby=["begins", "ends"], sort=[alt.SortField(field="kind")], as_=["bottom", "top"])
    return stacked.mark_rect(clip=True, stroke=SURFACE, strokeWidth=0.5).encode(
        x=alt.X("begins:T", scale=alt.Scale(type="utc", domain=domain), axis=_time_axis(start, end)),
        x2="ends:T",
        y=alt.Y("top:Q", title=f"Events per {grain}",
                axis=alt.Axis(format="~s", tickCount=4, minExtent=40)),
        y2="bottom:Q",
        color=alt.Color("kind:N", title=None, legend=_top_legend(columns=2),
                        scale=alt.Scale(domain=labels, range=[KIND_COLORS[kind] for kind in present])),
        tooltip=[alt.Tooltip("period:N", title="Received (UTC)"), alt.Tooltip("kind:N", title="Kind"),
                 alt.Tooltip("events:Q", title="Events", format=",")],
    ).properties(height=255, title=frame["mode"].iloc[0].capitalize())


HEARTBEAT_ROWS = ("Federal Register", "Dataset versions", "MSHA sitemaps", "OSHA sitemaps", "Official feed",
                  "Official run", "Feed manifests", "Batch flow")


def heartbeat(marks: pd.DataFrame, spans: pd.DataFrame, start: pd.Timestamp, now: pd.Timestamp,
              end: pd.Timestamp) -> alt.LayerChart:
    """The pipeline's checks and runs on one UTC time axis from `start` to `end`, one row each (HEARTBEAT_ROWS): a tick
    per watcher check and a diamond per publish of the feed, colored by outcome; the official runs as bars; the
    warehouse's build; a hollow circle where each is next due; a line at `now`, the future shaded.

    `marks` has the columns row, at (UTC), shape (check, publish, build or due), tone (a key of TONES), and what, when,
    status and note for its tooltip; `spans` has row, begins, ends, tone, what, when and status."""
    domain = [start.isoformat(), end.isoformat()]
    rows = alt.Y("row:N", title=None, scale=alt.Scale(domain=list(HEARTBEAT_ROWS)),
                 axis=alt.Axis(labelOverlap=False, ticks=False, domain=False, labelPadding=8, labelLimit=140))
    x = alt.X("at:T", scale=alt.Scale(type="utc", domain=domain), axis=_time_axis(start, end))
    tones = alt.Scale(domain=list(TONES), range=list(TONES.values()))
    tooltip = [alt.Tooltip("what:N", title="What"), alt.Tooltip("when:N", title="When (UTC)"),
               alt.Tooltip("status:N", title="Status"), alt.Tooltip("note:N", title="Note")]
    base = alt.Chart(marks).encode(x=x, y=rows, color=alt.Color("tone:N", scale=tones, legend=None), tooltip=tooltip)
    scale = alt.Scale(type="utc", domain=domain)
    future = alt.Chart(pd.DataFrame({"begins": [now], "ends": [end]})).mark_rect(
        color=ACCENT, opacity=0.06).encode(x=alt.X("begins:T", scale=scale), x2="ends:T")
    layers = [future]
    if len(spans):
        layers.append(alt.Chart(spans).mark_bar(height=10, cornerRadius=3).encode(
            x=alt.X("begins:T", scale=scale), x2="ends:T", y=rows,
            color=alt.Color("tone:N", scale=tones, legend=None),
            tooltip=[alt.Tooltip("what:N", title="What"), alt.Tooltip("when:N", title="When (UTC)"),
                     alt.Tooltip("status:N", title="Status")]))
    layers += [
        base.transform_filter(alt.FieldOneOfPredicate(field="shape", oneOf=["check", "start"])).mark_tick(
            thickness=2, size=16, opacity=0.95),
        base.transform_filter(alt.datum.shape == "publish").mark_point(shape="diamond", filled=True, size=90,
                                                                         opacity=1),
        base.transform_filter(alt.datum.shape == "build").mark_point(shape="square", filled=True, size=70,
                                                                       opacity=1),
        base.transform_filter(alt.datum.shape == "due").mark_point(filled=False, size=70, strokeWidth=2,
                                                                     opacity=1),
        alt.Chart(pd.DataFrame({"at": [now]})).mark_rule(color=ACCENT, strokeWidth=1.5).encode(
            x=alt.X("at:T", scale=scale)),
    ]
    return alt.layer(*layers).properties(height=26 * len(HEARTBEAT_ROWS), background="transparent")


def counts_over_time(frame: pd.DataFrame, kind: str, grain: str) -> alt.Chart:
    """All and serious records of one kind per period, summed over districts (columns period, events, serious)."""
    label = KIND_LABELS[kind]
    totals = frame.groupby("period", as_index=False)[["events", "serious"]].sum()
    names = {"events": f"All {label.lower()}"}
    if kind in SERIOUS_LABELS:
        names["serious"] = SERIOUS_LABELS[kind]
    long = totals.melt("period", value_vars=list(names), var_name="series", value_name="count")
    long["series"] = long["series"].map(names)
    domain = list(names.values())
    legend = _top_legend() if len(domain) > 1 else None
    return alt.Chart(long).mark_line(strokeWidth=2).encode(
        x=alt.X("period:T", title=None),
        y=alt.Y("count:Q", title=f"{label} per {grain}"),
        color=alt.Color("series:N", title=None, legend=legend,
                        scale=alt.Scale(domain=domain, range=[KIND_COLORS[kind], SERIOUS][:len(domain)])),
        tooltip=[alt.Tooltip("period:T", title=grain.capitalize()), alt.Tooltip("series:N", title="Series"),
                 alt.Tooltip("count:Q", title="Records", format=",")],
    ).properties(height=HEIGHT)


def counts_by_district(frame: pd.DataFrame, kind: str, period: str) -> alt.Chart:
    """Records of one kind per district over the whole period (columns district, events, serious)."""
    by_district = frame.groupby("district", as_index=False)[["events", "serious"]].sum()
    by_district["district"] = by_district["district"].replace("", NO_DISTRICT)
    tooltip = [alt.Tooltip("district:N", title="District"), alt.Tooltip("events:Q", title="Records", format=",")]
    if kind in SERIOUS_LABELS:
        tooltip.append(alt.Tooltip("serious:Q", title=SERIOUS_LABELS[kind], format=","))
    return alt.Chart(by_district).mark_bar(color=KIND_COLORS[kind], cornerRadiusEnd=3).encode(
        x=alt.X("events:Q", title=f"{KIND_LABELS[kind]}, {period}"),
        y=alt.Y("district:N", sort="-x", axis=_every_label("District")),
        tooltip=tooltip,
    ).properties(height=max(120, ROW_HEIGHT * len(by_district)))


def forecast(frame: pd.DataFrame, kind: str) -> alt.LayerChart:
    """Observed counts, a dashed one-step-ahead forecast and anomaly markers on one color scale."""
    domain, colors = ["Observed", "Forecast", "Anomaly"], [KIND_COLORS[kind], MUTED, CRITICAL]
    color_scale = alt.Scale(domain=domain, range=colors)
    legend = _top_legend()
    legend.symbolOpacity = 1  # Keep the legend keys solid when Vega merges the translucent layers.
    lines = frame.melt("day", value_vars=["observed", "predicted"], var_name="series", value_name="count")
    lines["series"] = lines["series"].map({"observed": "Observed", "predicted": "Forecast"})
    y_title = f"{KIND_LABELS[kind]} per day"
    line = alt.Chart(lines).mark_line(strokeWidth=2).encode(
        x=alt.X("day:T", title=None),
        y=alt.Y("count:Q", title=y_title),
        color=alt.Color("series:N", title=None, scale=color_scale, legend=legend),
        strokeDash=alt.condition(alt.datum.series == "Forecast", alt.value([6, 4]), alt.value([1, 0])),
        tooltip=[alt.Tooltip("day:T", title="Day"), alt.Tooltip("series:N", title="Series"),
                 alt.Tooltip("count:Q", title="Records", format=",.1f")],
    )
    anomalies = frame[frame["anomaly"].astype(bool)].assign(series="Anomaly")
    points = alt.Chart(anomalies).mark_point(filled=True, size=64, opacity=1).encode(
        x="day:T",
        y=alt.Y("observed:Q", title=y_title),
        color=alt.Color("series:N", title=None, scale=color_scale, legend=legend),
        tooltip=[alt.Tooltip("day:T", title="Day"), alt.Tooltip("observed:Q", title="Observed", format=","),
                 alt.Tooltip("predicted:Q", title="Forecast", format=",.1f"),
                 alt.Tooltip("residual:Q", title="Residual", format="+,.1f")],
    )
    # Translucent underlays follow the same coordinates, dashes and scale as their sharp foreground marks.
    forecast_glow = line.transform_filter(alt.datum.series == "Forecast").encode(tooltip=alt.value(None))
    anomaly_glow = points.encode(tooltip=alt.value(None))
    return alt.layer(
        forecast_glow.mark_line(strokeWidth=11, strokeCap="round", opacity=0.10, aria=False),
        forecast_glow.mark_line(strokeWidth=6, strokeCap="round", opacity=0.22, aria=False),
        line,
        anomaly_glow.mark_point(filled=True, size=360, opacity=0.10, aria=False),
        anomaly_glow.mark_point(filled=True, size=180, opacity=0.25, aria=False),
        points,
    ).properties(height=HEIGHT)


def district_heatmap(frame: pd.DataFrame, measure: str, title: str) -> alt.Chart:
    """One measure per district and year (columns district, year and the mart's measures); cells parted by the
    surface's color."""
    return alt.Chart(frame).mark_rect(cornerRadius=2, stroke=SURFACE, strokeWidth=2).encode(
        x=alt.X("year:O", title=None, axis=alt.Axis(labelAngle=0)),
        y=alt.Y("district:N", axis=_every_label("District")),
        color=alt.Color(f"{measure}:Q", title=title, scale=alt.Scale(range=list(SEQUENTIAL)),
                        legend=alt.Legend(orient="top", gradientLength=180)),
        tooltip=[alt.Tooltip("district:N", title="District"), alt.Tooltip("year:O", title="Year"),
                 alt.Tooltip("inspections:Q", title="Inspections", format=","),
                 alt.Tooltip("violations:Q", title="Violations", format=","),
                 alt.Tooltip("ss_violations:Q", title="S&S violations", format=","),
                 alt.Tooltip("accidents:Q", title="Accidents", format=",")],
    ).properties(height=max(120, ROW_HEIGHT * frame["district"].nunique()))


def top_sections(frame: pd.DataFrame) -> alt.Chart:
    """Citations of the most cited sections (columns from mart_top_cited_sections)."""
    return alt.Chart(frame).mark_bar(color=BLUE, cornerRadiusEnd=3).encode(
        x=alt.X("citations:Q", title="Citations (violations)"),
        y=alt.Y("cfr_section:N", sort="-x", axis=_every_label("30 CFR section")),
        tooltip=[alt.Tooltip("cfr_section:N", title="Section"), alt.Tooltip("heading:N", title="Heading"),
                 alt.Tooltip("citations:Q", title="Citations", format=","),
                 alt.Tooltip("ss_share:Q", title="S&S share", format=".1%"),
                 alt.Tooltip("proposed_penalty_total:Q", title="Proposed penalties (USD)", format="$,.0f"),
                 alt.Tooltip("last_cited:T", title="Last cited")],
    ).properties(height=ROW_HEIGHT * len(frame))


def ss_trend(frame: pd.DataFrame) -> alt.Chart:
    """The share of violations that were S&S, per year and commodity (columns from mart_ss_trend). Years with fewer
    than TREND_MIN_VIOLATIONS violations are left out: MSHA's records begin in 2000, and the few earlier stragglers
    (1 to 33 a year) would plot shares of 0% or 100% that say nothing."""
    data = frame.assign(commodity=frame["coal_metal"].map(COMMODITY_LABELS).fillna("Unknown"))
    data = data[data["violations"] >= TREND_MIN_VIOLATIONS]
    domain = [name for name in COMMODITY_COLORS if name in set(data["commodity"])]
    return alt.Chart(data[data["commodity"].isin(domain)]).mark_line(
        strokeWidth=2, point=alt.OverlayMarkDef(size=36, filled=True)).encode(
        x=alt.X("year:Q", title=None, axis=alt.Axis(format="d", tickMinStep=1),
                scale=alt.Scale(zero=False, nice=False)),
        y=alt.Y("ss_share:Q", title="S&S share of violations (%)", axis=alt.Axis(format="%")),
        color=alt.Color("commodity:N", title=None, legend=_top_legend(), scale=_fixed_colors(domain, COMMODITY_COLORS)),
        tooltip=[alt.Tooltip("year:Q", title="Year", format="d"), alt.Tooltip("commodity:N", title="Mines"),
                 alt.Tooltip("violations:Q", title="Violations", format=","),
                 alt.Tooltip("ss_violations:Q", title="S&S violations", format=","),
                 alt.Tooltip("ss_share:Q", title="S&S share", format=".1%")],
    ).properties(height=HEIGHT)


def documents(frame: pd.DataFrame, kinds: int = 6) -> alt.Chart:
    """Downloaded documents per site, stacked by kind; kinds beyond the `kinds` largest fold into "other"."""
    totals = frame.groupby("kind")["documents"].sum().sort_values(ascending=False)
    shown = sorted(totals.index[:kinds])  # colors by name, so a kind keeps its color as counts change
    data = frame.assign(kind=frame["kind"].where(frame["kind"].isin(shown), "other"))
    data = data.groupby(["site", "kind"], as_index=False)[["documents", "bytes"]].sum()
    data["gigabytes"] = data["bytes"] / 1e9
    domain = shown + (["other"] if "other" in set(data["kind"]) and "other" not in shown else [])
    colors = [*CATEGORICAL[:len(shown)], MUTED][:len(domain)]
    return alt.Chart(data).mark_bar().encode(
        x=alt.X("sum(documents):Q", title="Documents downloaded"),
        y=alt.Y("site:N", sort="-x", axis=_every_label(None)),
        color=alt.Color("kind:N", title=None, legend=_top_legend(columns=2),
                        scale=alt.Scale(domain=domain, range=colors)),
        tooltip=[alt.Tooltip("site:N", title="Site"), alt.Tooltip("kind:N", title="Kind"),
                 alt.Tooltip("sum(documents):Q", title="Documents", format=","),
                 alt.Tooltip("sum(gigabytes):Q", title="Size (GB)", format=",.2f")],
    ).properties(height=120 + 2 * ROW_HEIGHT * data["site"].nunique())
