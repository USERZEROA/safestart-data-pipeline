"""Read-only access to the warehouse and the streaming store, for the dashboard.

Every query returns a `Table`: its rows, or no rows and one short sentence saying what is missing (the warehouse has
not been built, a table or view is not there yet, PostgreSQL cannot be reached). A page shows that sentence where its
chart would be, so a store that is down or not built yet empties only the panels that need it.

The warehouse is opened read-only for each query. The build writes a new file and swaps it in, so a short-lived
connection always reads the newest complete warehouse and never holds an old one open. DuckDB would spill a query that
outgrows its memory limit into a folder beside the database file, which the containers mount read-only, so this
process gives it a folder of its own in the system's temporary folder. PostgreSQL sessions are
read-only too, with a connect timeout and a statement timeout, so an absent or slow store cannot stall a page or load
the small host; once PostgreSQL has refused a connection, it is reported unreachable for a few seconds without trying
again, so a page with several panels waits for one timeout, not one per panel. Messages name the store's host and
port, never the connection URL, which may carry a password.

"12 months" means the 12 months before the newest violation issue date in the warehouse, as in the marts, so a brief
built from an older snapshot still describes a full year.
"""

from __future__ import annotations

import os
import re
import tempfile
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import psycopg
from psycopg.conninfo import conninfo_to_dict

from safestart_pipeline import config
from safestart_pipeline.stream.events import ENFORCEMENT_KINDS, MODES

WAREHOUSE_BUILD = "python -m safestart_pipeline.warehouse build"
STREAM_INIT = "python -m safestart_pipeline.stream.init"
CONNECT_TIMEOUT_SECONDS = 3
STATEMENT_TIMEOUT_MS = 15_000
RETRY_AFTER_SECONDS = 10
# Under half of the dashboard container's 448 MiB (docker-compose.yml), the rest being Streamlit's. On the real
# warehouse (3.1 million violations) every page rendered in under a second and the process peaked at 300 MiB, DuckDB
# never coming near its limit or spilling.
WAREHOUSE_SETTINGS = {"memory_limit": "192MiB", "threads": 2,
                      "temp_directory": str(Path(tempfile.gettempdir()) / f"safestart-dashboard-{os.getpid()}")}
GRAINS = frozenset({"day", "week", "month"})


@dataclass(frozen=True)
class Table:
    """Rows for one panel, or no rows and a short sentence saying why."""

    frame: pd.DataFrame = field(default_factory=pd.DataFrame)
    problem: str | None = None

    @property
    def empty(self) -> bool:
        return self.frame.empty


def _first_line(problem: Exception) -> str:
    return (str(problem).strip().splitlines() or [type(problem).__name__])[0]


def _with_times(table: Table, *columns: str) -> Table:
    """Dates and timestamps as pandas datetimes (UTC for PostgreSQL's timestamptz), which every chart can encode."""
    frame = table.frame.copy()
    for column in columns:
        if column in frame:
            frame[column] = pd.to_datetime(frame[column], utc=frame[column].map(_aware).any())
    return Table(frame, table.problem)


def _aware(value: Any) -> bool:
    return getattr(value, "tzinfo", None) is not None


# The warehouse (DuckDB)


def warehouse_stamp() -> tuple[str, int] | None:
    """The warehouse file and its modification time: a cache key that changes when a new build is swapped in."""
    path = config.warehouse_path()
    try:
        return str(path), path.stat().st_mtime_ns
    except OSError:
        return None


def warehouse_query(sql: str, params: Mapping[str, Any] | None = None) -> Table:
    path: Path = config.warehouse_path()
    if not path.is_file():
        return Table(problem=f"No warehouse yet: build it with `{WAREHOUSE_BUILD}`.")
    try:
        with duckdb.connect(str(path), read_only=True, config=WAREHOUSE_SETTINGS) as connection:
            return Table(connection.execute(sql, dict(params or {})).df())
    except duckdb.CatalogException as problem:
        found = re.search(r"with name (\S+) does not exist", str(problem))
        missing = found.group(1) if found else "table this panel needs"
        return Table(problem=f"The warehouse has no {missing} yet: rebuild it with `{WAREHOUSE_BUILD}`.")
    except duckdb.Error as problem:
        return Table(problem=f"The warehouse could not answer: {_first_line(problem)}")


def mine_id_from(text: str) -> str | None:
    """MSHA's 7-digit mine ID from what a person types: "4609612", "46-09612" or "100003" (leading zeros dropped)."""
    digits = re.sub(r"[\s-]", "", text)
    return digits.zfill(7) if digits.isdigit() and len(digits) <= 7 else None


def find_mines(text: str, limit: int = 20) -> Table:
    """Mines whose ID is `text` or whose name contains it: the exact ID first, then names starting with it, active
    mines before closed ones."""
    return warehouse_query(
        """
        select mine_id, mine_name, state, county, coal_metal, mine_type, status, operator_name
        from mine
        where mine_id = $mine_id or contains(lower(mine_name), lower($text))
        order by mine_id = $mine_id desc nulls last, starts_with(lower(mine_name), lower($text)) desc,
                 status = 'Active' desc nulls last, mine_name, mine_id
        limit $limit
        """,
        {"mine_id": mine_id_from(text), "text": text.strip(), "limit": limit},
    )


def mine_scorecard(mine_id: str) -> Table:
    return warehouse_query(
        """
        select mine_id, mine_name, coal_metal, mine_type, status, state, district, latitude, longitude,
               inspections_12m, violations_12m, ss_violations_12m, proposed_penalty_12m, accidents_12m,
               serious_accidents_12m, hours_worked_12m, injury_rate_12m, top_sections_12m
        from mart_mine_scorecard
        where mine_id = $mine_id
        """,
        {"mine_id": mine_id},
    )


def mine_details(mine_id: str) -> Table:
    return warehouse_query(
        "select county, operator_name, controller_name, primary_canvass from mine where mine_id = $mine_id",
        {"mine_id": mine_id},
    )


def twelve_months() -> Table:
    """The window every "12 months" figure covers: up to the newest violation issue date in the warehouse."""
    return _with_times(warehouse_query(
        "select (max(issue_date) - interval 12 month)::date as since, max(issue_date) as until from violation"
    ), "since", "until")


def mine_sections(mine_id: str, limit: int = 10) -> Table:
    """The standards cited most at one mine in the 12 months, with their headings."""
    return warehouse_query(
        """
        with bounds as (select max(issue_date) as until from violation)
        select v.cfr_section, any_value(c.heading) as heading, count(*) as citations,
               count(*) filter (where v.sig_sub) as ss_citations, sum(v.proposed_penalty) as proposed_penalty,
               max(v.issue_date) as last_cited
        from violation as v
        cross join bounds
        left join cfr_section as c on c.section = v.cfr_section
        where v.mine_id = $mine_id and v.cfr_section is not null and v.issue_date > bounds.until - interval 12 month
        group by v.cfr_section
        order by citations desc, v.cfr_section
        limit $limit
        """,
        {"mine_id": mine_id, "limit": limit},
    )


def mine_accidents(mine_id: str, limit: int = 5) -> Table:
    return warehouse_query(
        """
        select accident_date, degree_injury, serious, classification, accident_type, no_injuries, days_lost,
               days_restrict, subunit, narrative
        from accident
        where mine_id = $mine_id
        order by accident_date desc nulls last, document_no desc
        limit $limit
        """,
        {"mine_id": mine_id, "limit": limit},
    )


def national_injury_rate(coal_metal: str) -> Table:
    """The newest year's injuries per 200,000 hours worked for one commodity (C or M), for comparison."""
    return warehouse_query(
        """
        select year, rate_per_200k from mart_injury_rate
        where coal_metal = $coal_metal and hours_worked > 0
        order by year desc
        limit 1
        """,
        {"coal_metal": coal_metal},
    )


def top_cited_sections(limit: int = 100) -> Table:
    return warehouse_query(
        """
        select cfr_section, heading, citations, ss_citations, ss_share, proposed_penalty_total, last_cited
        from mart_top_cited_sections
        order by citations desc, cfr_section
        limit $limit
        """,
        {"limit": limit},
    )


def section_text(section: str) -> Table:
    return warehouse_query(
        "select section, title, part, heading, text, amended_on from cfr_section where section = $section",
        {"section": section},
    )


def ss_trend() -> Table:
    return warehouse_query(
        "select year, coal_metal, violations, ss_violations, ss_share from mart_ss_trend order by year, coal_metal"
    )


def district_years(years: int = 10) -> Table:
    """The newest `years` years of each district's inspections, violations and accidents."""
    return warehouse_query(
        """
        select district, year, inspections, violations, ss_violations, accidents
        from mart_district_year
        where district is not null and year > (select max(year) from mart_district_year) - $years
        order by district, year
        """,
        {"years": years},
    )


def dataset_versions() -> Table:
    """Each dataset's current version (or its newest, when none is marked current) and how many versions are kept."""
    return _with_times(warehouse_query(
        """
        select dataset, versions, version, rows, built_at, is_current
        from (select *, count(*) over (partition by dataset) as versions,
                     row_number() over (partition by dataset order by is_current desc nulls last,
                                        built_at desc nulls last) as rank
              from dataset_version)
        where rank = 1
        order by dataset
        """
    ), "built_at")


def document_counts() -> Table:
    """Downloaded documents (those with a stored copy) by site and kind."""
    return warehouse_query(
        """
        select site, coalesce(kind, 'unknown') as kind, count(*) as documents, coalesce(sum(bytes), 0) as bytes
        from document
        where sha256 is not null
        group by all
        order by site, documents desc
        """
    )


# The streaming store (PostgreSQL)


def _where(url: str) -> str:
    try:
        parts = conninfo_to_dict(url)
    except psycopg.Error:
        return "the configured address"
    return f"{parts.get('host') or 'localhost'}:{parts.get('port') or 5432}"


_unreachable_until: dict[str, float] = {}  # URL -> monotonic time before which it is not tried again


def stream_query(sql: str, params: Mapping[str, Any] | None = None) -> Table:
    url = config.postgres_url()
    unreachable = Table(problem=f"The streaming store at {_where(url)} is not reachable.")
    if _unreachable_until.get(url, 0.0) > time.monotonic():
        return unreachable
    try:
        connection = psycopg.connect(
            url, connect_timeout=CONNECT_TIMEOUT_SECONDS, autocommit=True, application_name="safestart-dashboard",
            options=f"-c default_transaction_read_only=on -c statement_timeout={STATEMENT_TIMEOUT_MS}",
        )
    except psycopg.Error:  # its message can quote the URL, so it is not shown
        _unreachable_until[url] = time.monotonic() + RETRY_AFTER_SECONDS
        return unreachable
    _unreachable_until.pop(url, None)
    try:
        with connection:
            cursor = connection.execute(sql, dict(params or {}))
            columns = [column.name for column in cursor.description or ()]
            return Table(pd.DataFrame(cursor.fetchall(), columns=columns))
    except psycopg.errors.UndefinedTable:
        return Table(problem=f"The streaming store has no tables yet: create them with `{STREAM_INIT}`.")
    except psycopg.Error as problem:
        return Table(problem=f"The streaming store could not answer: {_first_line(problem)}")


def _mode_filter(mode: str | None) -> tuple[str, dict[str, str]]:
    """A where clause for one mode (`replay` or `live`), or none for both."""
    if mode is None:
        return "", {}
    if mode not in MODES:
        raise ValueError(f"mode must be replay, live or None, not {mode!r}")
    return "where mode = %(mode)s", {"mode": mode}


def event_counts(mode: str | None = None) -> Table:
    """Events stored by kind."""
    where, params = _mode_filter(mode)
    return _with_times(stream_query(
        f"select kind, count(*) as events, max(received_at) as last_received from stream.events {where} group by kind",
        params,
    ), "last_received")


def latest_events(mode: str | None = None, limit: int = 25) -> Table:
    """The newest events to arrive (by the time the storage consumer received them)."""
    where, params = _mode_filter(mode)
    return _with_times(stream_query(
        f"""
        select received_at, occurred_at, kind, mode, title, mine_id, district, state
        from stream.events {where}
        order by received_at desc, occurred_at desc, event_id
        limit %(limit)s
        """,
        {**params, "limit": limit},
    ), "received_at", "occurred_at")


def arrivals(mode: str | None = None, minutes: int = 15) -> Table:
    """Events received per minute and kind over the last `minutes` minutes (a range read of the received_at index)."""
    where, params = _mode_filter(mode)
    recent = "received_at > now() - make_interval(mins => %(minutes)s)"
    return _with_times(stream_query(
        f"""
        select date_trunc('minute', received_at) as minute, kind, count(*) as events
        from stream.events {f"{where} and {recent}" if where else f"where {recent}"}
        group by 1, 2
        order by 1, 2
        """,
        {**params, "minutes": minutes},
    ), "minute")


def _enforcement_kind(kind: str) -> str:
    if kind not in ENFORCEMENT_KINDS:
        raise ValueError(f"not an enforcement kind: {kind!r}")
    return kind


def daily_counts(kind: str, days: int, grain: str = "day") -> Table:
    """Counts of one kind per period (`day`, `week` or `month`) and district over the newest `days` days counted."""
    if grain not in GRAINS:
        raise ValueError(f"grain must be one of {sorted(GRAINS)}, not {grain!r}")
    return _with_times(stream_query(
        """
        select date_trunc(%(grain)s, day::timestamp)::date as period, district, sum(events)::bigint as events,
               sum(serious)::bigint as serious
        from stream.daily_counts
        where kind = %(kind)s
          and day > (select max(day) from stream.daily_counts where kind = %(kind)s) - %(days)s::integer
        group by 1, 2
        order by 1, 2
        """,
        {"kind": _enforcement_kind(kind), "days": days, "grain": grain},
    ), "period")


def forecast_districts(kind: str) -> Table:
    """The districts with forecasts for one kind, busiest first."""
    return stream_query(
        """
        select district, count(*) as days, count(*) filter (where anomaly) as anomalies
        from stream.forecasts
        where kind = %(kind)s
        group by district
        order by sum(observed) desc, district
        """,
        {"kind": _enforcement_kind(kind)},
    )


def forecasts(kind: str, district: str, days: int) -> Table:
    return _with_times(stream_query(
        """
        select day, observed, predicted, residual, anomaly, method
        from stream.forecasts
        where kind = %(kind)s and district = %(district)s
          and day > (select max(day) from stream.forecasts where kind = %(kind)s and district = %(district)s)
                    - %(days)s::integer
        order by day
        """,
        {"kind": _enforcement_kind(kind), "district": district, "days": days},
    ), "day")


def recent_anomalies(kind: str, limit: int = 10) -> Table:
    return _with_times(stream_query(
        """
        select day, district, observed, predicted, residual, method
        from stream.forecasts
        where kind = %(kind)s and anomaly
        order by day desc, district
        limit %(limit)s
        """,
        {"kind": _enforcement_kind(kind), "limit": limit},
    ), "day")


def source_freshness() -> Table:
    """Each live source's newest check, with the last day's checks, errors and new events."""
    return _with_times(stream_query(
        """
        select latest.source, latest.checked_at, latest.status, latest.found, latest.note,
               totals.checks_24h, totals.errors_24h, totals.found_24h
        from (select distinct on (source) source, checked_at, status, found, note
              from stream.source_checks
              order by source, checked_at desc) as latest
        join (select source,
                     count(*) filter (where checked_at > now() - interval '1 day') as checks_24h,
                     count(*) filter (where checked_at > now() - interval '1 day' and status = 'error') as errors_24h,
                     coalesce(sum(found) filter (where checked_at > now() - interval '1 day'), 0)::bigint as found_24h
              from stream.source_checks
              group by source) as totals using (source)
        order by latest.source
        """
    ), "checked_at")

