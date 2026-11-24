"""The dashboard's own markup, as plain strings for `st.html`: the Live page's tiles with their rolling digits and trend
bars, status chips, the Now strip's cards, the newest events and the official files' latest changes. theme.css styles
it. Nothing here imports Streamlit, so the tests read it as text.

Every text that comes from a source (titles, notes, URLs, hosts) goes through `html.escape`. The markup carries no
script and no inline SVG: Streamlit's sanitizer keeps HTML and CSS only, so bars are elements sized by a CSS variable.

Motion follows one rule: only a real change moves. A tile's digits roll and the tile glows once, on the refresh where
its value differs from what this viewer saw last (the caller passes `previous`); a newly arrived event slides in once;
a pulse marks a source being asked right now, with its phase taken from the clock, so a redraw does not restart it;
everything else is still. theme.css stops all of it for a viewer who asks for reduced motion.
"""

from __future__ import annotations

import html
import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime

PULSE_SECONDS = 1.8  # the pulse's cycle in theme.css
DIGITS = "".join(f"<b>{digit}</b>" for digit in range(10))


def esc(value: object) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def whole(value: float | None) -> str:
    """A count as a tile shows it: exact up to 999,999, so that one more is visible, then 3.12M."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "–"
    return f"{value / 1e6:,.2f}M" if abs(value) >= 1_000_000 else f"{int(value):,}"


def span(seconds: float) -> str:
    """A length of time in short words: 8 s, 14 min, 3 h 5 min, 2 d 4 h."""
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds} s"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} min"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours} h {minutes} min" if minutes and hours < 10 else f"{hours} h"
    days, hours = divmod(hours, 24)
    return f"{days} d {hours} h" if hours and days < 3 else f"{days} d"


def ago(moment: datetime | None, now: datetime) -> str:
    if moment is None:
        return "never"
    seconds = (now - moment).total_seconds()
    return "just now" if seconds < 5 else f"{span(seconds)} ago"


def until(moment: datetime, now: datetime) -> str:
    seconds = (moment - now).total_seconds()
    return "due now" if seconds <= 0 else f"in {span(seconds)}"


def clock(moment: datetime | None, now: datetime) -> str:
    """A UTC time as people read it here: 13:57 today, else 2026-11-23 23:34."""
    if moment is None:
        return "–"
    return f"{moment:%H:%M}" if moment.date() == now.date() else f"{moment:%Y-%m-%d %H:%M}"


def stamp(moment: datetime | None) -> str:
    """The full UTC time, for a tooltip beside a relative one."""
    return "" if moment is None else f"{moment:%Y-%m-%d %H:%M:%S} UTC"


# Small pieces ---------------------------------------------------------------------------------------------------------


def chip(text: str, tone: str = "muted", *, dot: str | None = None, title: str | None = None) -> str:
    """A small rounded label; `tone` is accent, good, warning, serious, critical or muted (theme.css)."""
    mark = f'<span class="ss-dot {esc(dot)}"></span>' if dot else ""
    hint = f' title="{esc(title)}"' if title else ""
    return f'<span class="ss-chip {esc(tone)}"{hint}>{mark}{esc(text)}</span>'


def dot(tone: str = "muted", color: str | None = None) -> str:
    style = f' style="--c:{esc(color)}"' if color else ""
    return f'<span class="ss-dot {esc(tone)}"{style} aria-hidden="true"></span>'


def pulse(now: datetime) -> str:
    """A pulsing dot for a source being asked now. Its animation starts as far into its cycle as the clock is, so the
    markup redrawn every few seconds continues the pulse instead of restarting it."""
    phase = now.timestamp() % PULSE_SECONDS
    return (f'<span class="ss-pulse ss-anim" style="animation-delay:-{phase:.2f}s" role="img"'
            ' aria-label="being asked now"></span>')


def meter(share: float, *, label: str) -> str:
    """A progress bar for a share from 0 to 1; the value is the elapsed share of the window."""
    share = min(max(share, 0.0), 1.0)
    return (f'<div class="ss-meter" role="progressbar" aria-valuemin="0" aria-valuemax="100"'
            f' aria-valuenow="{share * 100:.0f}" aria-label="{esc(label)}"><i style="--p:{share:.4f}"></i></div>')


def bars(values: Sequence[int], titles: Sequence[str], *, classes: Sequence[str] = (), label: str) -> str:
    """Small bars, each as tall as its share of the largest; `classes` marks bars apart (last, failed, unsigned)."""
    top = max(values, default=0)
    cells = []
    for index, (value, title) in enumerate(zip(values, titles)):
        extra = classes[index] if index < len(classes) else ""
        share = value / top if top else 0.0
        cells.append(f'<i class="{esc(extra)}" style="--h:{share:.3f}" title="{esc(title)}"></i>')
    return f'<div class="ss-bars" role="img" aria-label="{esc(label)}">{"".join(cells)}</div>'


def odometer(value: int | None, previous: int | None) -> str:
    """A tile's value. When it differs from what this viewer saw last, each changed digit rolls from its old figure to
    its new one, once; otherwise it is plain text. Every figure keeps its own width (an invisible copy holds it), so the
    rolled number and the plain one that replaces it at the next refresh are the same size."""
    text = whole(value)
    if value is None or previous is None or whole(previous) == text:
        return esc(text)
    old = whole(previous).rjust(len(text))[-len(text):]
    cells = []
    for new, was in zip(text, old):
        if not new.isdigit() or new == was:
            cells.append(esc(new))
            continue
        start = int(was) if was.isdigit() else 0
        cells.append(f'<span class="ss-d" style="--from:{start};--to:{new}"><span class="w">{new}</span>'
                     f'<span class="r">{DIGITS}</span></span>')
    return f'<span class="ss-odo ss-anim" aria-hidden="true">{"".join(cells)}</span>'


# The Live page's tiles ------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Tile:
    """One kind of event: its total, what this viewer saw last, the recent arrivals, its days and its check."""

    kind: str
    label: str
    color: str
    value: int | None
    previous: int | None
    delta: str  # the chip under the value: "+2,150 new in 24 h", "last new 14 h ago"
    delta_tone: str
    days: Sequence[tuple[str, int]]  # (YYYY-MM-DD, events that happened at the source that day), oldest first
    foot: Sequence[str]  # the check that can move it: ("versions check 3 h ago", "next in about 3 h")
    fresh: int = 0  # how many arrived since this viewer's earlier look (a "+N new" badge for a minute)
    note: str = ""  # the tile's full description, for screen readers and the tooltip


def sparkline(tile: Tile) -> str:
    if not tile.days:
        return '<div class="ss-foot">Trend unavailable</div>'
    values = [count for _, count in tile.days]
    top = max(values, default=0)
    cells = []
    for index, (day, count) in enumerate(tile.days):
        classes = " ".join(filter(None, ["some" if count else "", "today" if index == len(tile.days) - 1 and count
                                         else ""]))
        share = count / top if top else 0.0
        cells.append(f'<i class="{classes}" style="--h:{share:.3f}" title="{esc(day)}: {count:,}"></i>')
    totals = ", ".join(f"{value:,}" for value in values)
    label = f"{tile.label} per UTC source day across both modes, last {len(values)} days: {totals}"
    return f'<div class="ss-spark" role="img" aria-label="{esc(label)}">{"".join(cells)}</div>'


def tile(item: Tile) -> str:
    changed = item.previous is not None and item.previous != item.value
    classes = "ss-tile ss-anim changed" if changed else "ss-tile"
    badge = f'<span class="ss-new">+{item.fresh:,} new</span>' if item.fresh > 0 else ""
    foot = "".join(f"<span>{esc(part)}</span>" for part in item.foot)
    return (f'<article class="{classes}" style="--c:{esc(item.color)}" data-kind="{esc(item.kind)}"'
            f' title="{esc(item.note)}" aria-label="{esc(item.label)}: {esc(whole(item.value))}. {esc(item.note)}">'
            f'<div class="ss-tile-head">{dot(color=item.color)}<span class="ss-tile-label">{esc(item.label)}</span>'
            '</div>'
            f'<div class="ss-val">{odometer(item.value, item.previous)}{badge}</div>'
            f'<div class="ss-delta">{chip(item.delta, item.delta_tone)}</div>'
            f'{sparkline(item)}<div class="ss-foot">{foot}</div></article>')


def kpi_row(groups: Iterable[tuple[str, str, str, Sequence[Tile]]]) -> str:
    """The tiles in groups, one per Kafka topic: (css name, topic, what feeds it, tiles). One row on a wide screen,
    the groups stacked on a narrow one."""
    parts = []
    for name, topic, feeds, tiles in groups:
        head = (f'<div class="ss-group-head"><span class="ss-kicker">Kafka topic <code>{esc(topic)}</code></span>'
                f'<span class="ss-kicker">{esc(feeds)}</span></div>')
        parts.append(f'<section class="ss-group {esc(name)}" style="--n:{len(tiles)}" aria-label="{esc(topic)}">'
                     f'{head}{"".join(tile(item) for item in tiles)}</section>')
    return f'<div class="ss-kpis">{"".join(parts)}</div>'


# Cards ----------------------------------------------------------------------------------------------------------------


def card(title: str, body: str, *, badge: str = "", lead: str = "", live: bool = False) -> str:
    """A card with a title line (a leading dot or pulse, the title, a chip at the right) over its body."""
    return (f'<section class="ss-card{" live" if live else ""}"><div class="ss-card-head">{lead}'
            f'<span class="ss-card-title">{esc(title)}</span>{badge}</div>{body}</section>')


def cards(items: Sequence[str], columns: int | None = None) -> str:
    return f'<div class="ss-cards" style="--n:{columns or len(items)}">{"".join(items)}</div>'


@dataclass(frozen=True)
class Line:
    """A line of a card's list: a status dot, what it is, and when (`soon` marks a time to come in the accent)."""

    tone: str
    what: str
    when: str
    title: str = ""
    soon: bool = False
    separator: bool = False
    recent: bool = False


def lines(items: Iterable[Line], now: datetime) -> str:
    parts = []
    for item in items:
        if item.separator:
            parts.append(f'<li class="sep" aria-hidden="true"></li><li>{dot("dim")}<span class="what ss-kicker">'
                         f'{esc(item.what)}</span><span class="when"></span></li>')
            continue
        hint = f' title="{esc(item.title)}"' if item.title else ""
        mark = pulse(now) if item.recent else dot(item.tone)
        parts.append(f'<li{hint}>{mark}<span class="what">{esc(item.what)}</span>'
                     f'<span class="when{" soon" if item.soon else ""}">{esc(item.when)}</span></li>')
    return f'<ul class="ss-list">{"".join(parts)}</ul>'


def text(line: str) -> str:
    """A line of a card: escaped text, with **bold** spans."""
    parts = esc(line).split("**")
    return '<div class="ss-line">' + "".join(f"<b>{part}</b>" if index % 2 else part
                                             for index, part in enumerate(parts)) + "</div>"


def source(line: str) -> str:
    """Where a panel's numbers come from, in small print: "stream.events (PostgreSQL) · every 5 s"."""
    parts = esc(line).split("`")
    return '<div class="ss-source">' + "".join(f"<code>{part}</code>" if index % 2 else part
                                               for index, part in enumerate(parts)) + "</div>"


# The newest events ----------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class EventRow:
    label: str
    color: str
    title: str
    facts: Sequence[str]  # the source day, the district, the state: only those present
    mode: str
    received: str  # "14 h ago"
    received_at: str  # the full UTC time, for the tooltip
    fresh: bool = False
    link: str | None = None


def event_list(rows: Iterable[EventRow]) -> str:
    items = []
    for row in rows:
        title = esc(row.title)
        if row.link and row.link.startswith(("https://", "http://")):
            title = f'<a href="{esc(row.link)}">{title}</a>'
        facts = "".join(f"<span>{esc(fact)}</span>" for fact in row.facts)
        items.append(f'<li class="{"fresh ss-anim" if row.fresh else ""}">{dot(color=row.color)}'
                     f'<span class="title">{title}</span>'
                     f'<span class="when" title="{esc(row.received_at)}">{esc(row.received)}</span>'
                     f'<span class="meta"><span class="kind">{esc(row.label)}</span>{facts}'
                     f'<span>{esc(row.mode)}</span></span></li>')
    return f'<ul class="ss-feed">{"".join(items)}</ul>'
