"""Altair charts for the dashboard, drawn from one palette so that an entity keeps its color on every page.

Colors follow the entity, never its rank: each event kind and each commodity has a fixed hue from a categorical set
whose neighbors stay distinguishable under color-vision deficiency, and severity uses reserved status colors (serious
for S&S violations and serious accidents, critical for anomalies) that never stand for a series. A chart with one
series uses the first hue and no legend; a chart with more carries a legend, so color never carries meaning alone.
Every axis names its unit and every mark has a tooltip.
"""

from __future__ import annotations

import altair as alt
import pandas as pd

BLUE, ORANGE, AQUA, YELLOW, MAGENTA, GREEN, VIOLET = (
    "#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6")
CATEGORICAL = (BLUE, ORANGE, AQUA, YELLOW, MAGENTA, GREEN, VIOLET)
SERIOUS = "#ec835a"  # status color: S&S violations and serious accidents (definitions.py)
CRITICAL = "#d03b3b"  # status color: anomalies
MUTED = "#898781"  # forecasts and folded "other" categories
SEQUENTIAL = ("#cde2fb", "#104281")  # one hue, light to dark, for magnitudes

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


def arrivals(frame: pd.DataFrame, minutes: int = 15) -> alt.Chart:
    """Events received per minute over the last `minutes` minutes, stacked by kind (columns minute as UTC
    timestamps, kind, events). Every minute keeps its place, so a quiet minute shows as a gap."""
    present = [kind for kind in KIND_ORDER if kind in set(frame["kind"])]
    labels = {kind: KIND_LABELS[kind] for kind in present}
    newest = frame["minute"].max()
    clock = [moment.strftime("%H:%M") for moment in pd.date_range(newest - pd.Timedelta(minutes=minutes - 1), newest,
                                                                     freq="min")]
    data = frame.assign(kind=frame["kind"].map(labels), clock=frame["minute"].dt.strftime("%H:%M"))
    return alt.Chart(data[data["clock"].isin(clock)]).mark_bar(cornerRadiusTopLeft=2, cornerRadiusTopRight=2).encode(
        x=alt.X("clock:O", title="Received (UTC)", scale=alt.Scale(domain=clock),
                axis=alt.Axis(values=clock[::-5][::-1], labelAngle=0)),
        y=alt.Y("sum(events):Q", title="Events received per minute"),
        color=alt.Color("kind:N", title=None, legend=_top_legend(columns=4),
                        scale=_fixed_colors(list(labels.values()),
                                            {labels[kind]: KIND_COLORS[kind] for kind in present})),
        order=alt.Order("kind:N"),
        tooltip=[alt.Tooltip("clock:O", title="Minute (UTC)"), alt.Tooltip("kind:N", title="Kind"),
                 alt.Tooltip("sum(events):Q", title="Events", format=",")],
    ).properties(height=220)


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
        color=alt.Color("series:N", title=None, legend=legend, scale=alt.Scale(domain=domain, range=[BLUE, SERIOUS])),
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
    return alt.Chart(by_district).mark_bar(color=BLUE, cornerRadiusEnd=3).encode(
        x=alt.X("events:Q", title=f"{KIND_LABELS[kind]}, {period}"),
        y=alt.Y("district:N", sort="-x", axis=_every_label("District")),
        tooltip=tooltip,
    ).properties(height=max(120, ROW_HEIGHT * len(by_district)))


def forecast(frame: pd.DataFrame, kind: str) -> alt.LayerChart:
    """Observed counts against the one-step-ahead forecast (gray), anomalies marked (columns day, observed,
    predicted, residual, anomaly). Both layers share one color scale, so they draw one legend."""
    domain, colors = ["Observed", "Forecast", "Anomaly"], [BLUE, MUTED, CRITICAL]
    color_scale = alt.Scale(domain=domain, range=colors)
    lines = frame.melt("day", value_vars=["observed", "predicted"], var_name="series", value_name="count")
    lines["series"] = lines["series"].map({"observed": "Observed", "predicted": "Forecast"})
    y_title = f"{KIND_LABELS[kind]} per day"
    line = alt.Chart(lines).mark_line(strokeWidth=2).encode(
        x=alt.X("day:T", title=None),
        y=alt.Y("count:Q", title=y_title),
        color=alt.Color("series:N", title=None, scale=color_scale, legend=_top_legend()),
        tooltip=[alt.Tooltip("day:T", title="Day"), alt.Tooltip("series:N", title="Series"),
                 alt.Tooltip("count:Q", title="Records", format=",.1f")],
    )
    anomalies = frame[frame["anomaly"].astype(bool)].assign(series="Anomaly")
    points = alt.Chart(anomalies).mark_point(filled=True, size=64, opacity=1).encode(
        x="day:T",
        y=alt.Y("observed:Q", title=y_title),
        color=alt.Color("series:N", title=None, scale=color_scale, legend=_top_legend()),
        tooltip=[alt.Tooltip("day:T", title="Day"), alt.Tooltip("observed:Q", title="Observed", format=","),
                 alt.Tooltip("predicted:Q", title="Forecast", format=",.1f"),
                 alt.Tooltip("residual:Q", title="Residual", format="+,.1f")],
    )
    return alt.layer(line, points).properties(height=HEIGHT)


def district_heatmap(frame: pd.DataFrame, measure: str, title: str) -> alt.Chart:
    """One measure per district and year (columns district, year and the mart's measures)."""
    return alt.Chart(frame).mark_rect(cornerRadius=2).encode(
        x=alt.X("year:O", title=None),
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
        color=alt.Color("kind:N", title=None, legend=_top_legend(columns=4),
                        scale=alt.Scale(domain=domain, range=colors)),
        tooltip=[alt.Tooltip("site:N", title="Site"), alt.Tooltip("kind:N", title="Kind"),
                 alt.Tooltip("sum(documents):Q", title="Documents", format=","),
                 alt.Tooltip("sum(gigabytes):Q", title="Size (GB)", format=",.2f")],
    ).properties(height=120 + 2 * ROW_HEIGHT * data["site"].nunique())
