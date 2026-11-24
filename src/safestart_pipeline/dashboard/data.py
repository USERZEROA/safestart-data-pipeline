"""Read-only access to the warehouse, the streaming store and the official files' record and feed, for the dashboard.

Every query returns a `Table`: its rows, or no rows and one short sentence saying what is missing (the warehouse has
not been built, a table or view is not there yet, PostgreSQL cannot be reached, no official run has been recorded). A
page shows that sentence where its chart would be, so a store that is down or not built yet empties only the panels
that need it. The official files' record (state/official.sqlite) and the feed (exports/official/latest.json) are read
from the data root, which the dashboard's container mounts read-only.

The warehouse is opened read-only for each query. The build writes a new file and swaps it in, so a short-lived
connection always reads the newest complete warehouse and never holds an old one open. DuckDB would spill a query that
outgrows its memory limit into a folder beside the database file, which the containers mount read-only, so this
process gives it a folder of its own in the system's temporary folder. PostgreSQL sessions are
read-only too, with a connect timeout and a statement timeout, so an absent or slow store cannot stall a page or load
the small host; once PostgreSQL has refused a connection, it is reported unreachable for a few seconds without trying
again, so a page with several panels waits for one timeout, not one per panel. Messages name the store's host and
port, never the connection URL, which may carry a password. Sessions count days and hours in UTC.

The official run's progress is read from the crawl store it writes while it runs (the requests it has sent and the
changes it has found since its source began), through `verify.read_only`, the newest rows only, so a look at a store of
a gigabyte stays bounded.

"12 months" means the 12 months before the newest violation issue date in the warehouse, as in the marts, so a brief
built from an older snapshot still describes a full year.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import tempfile
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import psycopg
from psycopg.conninfo import conninfo_to_dict

from safestart_pipeline import config
from safestart_pipeline.official.feed import feed_status
from safestart_pipeline.official.purge import latest_purges
from safestart_pipeline.stream.events import ENFORCEMENT_KINDS, MODES
from safestart_pipeline.verify import read_only

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
ARRIVAL_GRAINS = frozenset({"minute", "hour", "day"})


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


MINE_MATCHES = 20  # how many matches the mine search lists


def find_mines(text: str, limit: int | None = None) -> Table:
    """Mines whose ID is `text` or whose name contains it, at most MINE_MATCHES: the exact ID first, then names starting
    with it, active mines before closed ones."""
    return warehouse_query(
        """
        select mine_id, mine_name, state, county, coal_metal, mine_type, status, operator_name
        from mine
        where mine_id = $mine_id or contains(lower(mine_name), lower($text))
        order by mine_id = $mine_id desc nulls last, starts_with(lower(mine_name), lower($text)) desc,
                 status = 'Active' desc nulls last, mine_name, mine_id
        limit $limit
        """,
        {"mine_id": mine_id_from(text), "text": text.strip(), "limit": limit or MINE_MATCHES},
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
    """The mine brief's two windows. Every "12 months" figure covers the 12 months up to the newest violation issue
    date; the hours worked and the injury rate cover four quarters, ending with `rate_last_quarter` (year * 4 + quarter
    - 1), the newest quarter with reported hours that ends by then, as mart_mine_scorecard takes it (marts.sql)."""
    return _with_times(warehouse_query(
        """
        with bounds as (
            select (max(issue_date) - interval 12 month)::date as since, max(issue_date) as until from violation
        )
        select b.since, b.until,
               (select max(p.cal_yr * 4 + p.cal_qtr - 1) from production_quarter p
                where last_day(make_date(p.cal_yr, p.cal_qtr * 3, 1)) <= b.until) as rate_last_quarter
        from bounds b
        """
    ), "since", "until")


def mine_sections(mine_id: str, limit: int = 10) -> Table:
    """The standards cited most at one mine in the 12 months, with their headings. MSHA cites 30 CFR, and a section
    number alone is not a section (29 CFR 7.7 and 30 CFR 7.7 both exist), so the join names the title, as the marts
    do; without it a citation of such a number would count once per title."""
    return warehouse_query(
        """
        with bounds as (select max(issue_date) as until from violation)
        select v.cfr_section, any_value(c.heading) as heading, count(*) as citations,
               count(*) filter (where v.sig_sub) as ss_citations, sum(v.proposed_penalty) as proposed_penalty,
               max(v.issue_date) as last_cited
        from violation as v
        cross join bounds
        left join cfr_section as c on c.title = 30 and c.section = v.cfr_section
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
    """The text of a cited section: the section of that number in 30 CFR, the title MSHA cites."""
    return warehouse_query(
        """
        select section, title, part, heading, text, amended_on
        from cfr_section
        where title = 30 and section = $section
        """,
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


def document_files() -> Table:
    """The distinct downloaded files, one per SHA-256 however many URLs led to it, and their size."""
    return warehouse_query(
        """
        select count(*) as files, coalesce(sum(bytes), 0) as bytes
        from (select sha256, any_value(bytes) as bytes from document where sha256 is not null group by sha256)
        """
    )


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
            options=f"-c default_transaction_read_only=on -c statement_timeout={STATEMENT_TIMEOUT_MS} -c TimeZone=UTC",
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
    """Stored events of the selected mode, with arrivals in the last 24 hours from the same scan."""
    where, params = _mode_filter(mode)
    return _with_times(stream_query(
        f"""
        select kind, count(*) as events, max(received_at) as last_received,
               count(*) filter (where received_at > now() - interval '24 hours') as last_24h
        from stream.events {where}
        group by kind
        """, params,
    ), "last_received")


def daily_by_kind(days: int = 14) -> Table:
    """Source-day totals across both modes, as counted by the analytics consumer. Today is a UTC date."""
    return stream_query(
        """
        select day, kind, sum(events)::bigint as events from stream.daily_counts
        where day > current_date - %(days)s::integer and day <= current_date
        group by day, kind order by day, kind
        """, {"days": days},
    )


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


def arrivals(mode: str | None = None, minutes: int = 15, grain: str = "minute") -> Table:
    """Events received per `grain` (minute, hour or day, UTC), mode and kind over the last `minutes` minutes (a range
    read of the received_at index)."""
    if grain not in ARRIVAL_GRAINS:
        raise ValueError(f"grain must be one of {sorted(ARRIVAL_GRAINS)}, not {grain!r}")
    where, params = _mode_filter(mode)
    recent = "received_at > now() - make_interval(mins => %(minutes)s)"
    return _with_times(stream_query(
        f"""
        select date_trunc(%(grain)s, received_at) as at, mode, kind, count(*) as events
        from stream.events {f"{where} and {recent}" if where else f"where {recent}"}
        group by 1, 2, 3
        order by 1, 2, 3
        """,
        {**params, "minutes": minutes, "grain": grain},
    ), "at")


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


def check_history(hours: int = 48) -> Table:
    """Every watcher check of the last `hours` hours, oldest first."""
    return _with_times(stream_query(
        """
        select source, checked_at, status, found, note
        from stream.source_checks
        where checked_at > now() - make_interval(hours => %(hours)s)
        order by checked_at
        """,
        {"hours": hours},
    ), "checked_at")


# The daily update of the official files (SQLite and JSON in the data root)

OFFICIAL_RUN = "ssp official run"
OFFICIAL_COUNTS = ("new", "changed", "gone", "restored", "moved", "rejected", "checked")
CRAWL_STORES = ("osha", "arlweb")  # the official run's sources that write a crawl store as they go
PROGRESS_ROWS = 10_000  # the newest requests read for a running source: more than a day's budget of either crawl


def _official_path() -> Path:
    return config.data_root() / "state" / "official.sqlite"


def crawl_progress(source: str, since: str, until: str) -> dict:
    """What a running source of the official run has done since `since` (its start), from its crawl store: the
    requests it sent, the newest one's time, how many answered 304 (unchanged), and its changes by kind. Only the
    newest PROGRESS_ROWS requests are read, so the look stays small however large the store is."""
    path = config.data_root() / "state" / f"crawl-{source}.sqlite"
    found = {"asked": 0, "last_request": None, "not_modified": 0, **dict.fromkeys(OFFICIAL_COUNTS[:-1], 0)}
    if not path.is_file():
        raise FileNotFoundError(f"{path.name} is missing")
    db = read_only(path)
    try:
        row = db.execute(
            "select count(*) as asked, max(fetched_at) as last_request,"
            " coalesce(sum(http_status = 304), 0) as not_modified"
            " from (select fetched_at, http_status from fetches order by rowid desc limit ?)"
            " where fetched_at between ? and ?",
            (PROGRESS_ROWS, since, until)).fetchone()
        found |= {key: row[key] for key in ("asked", "last_request", "not_modified")}
        try:
            for change, number in db.execute(
                    "select change, count(*) from changes where observed_at between ? and ? group by change",
                    (since, until)):
                found[change] = number
        except sqlite3.OperationalError as problem:
            if "no such table: changes" not in str(problem):  # first-version stores have no changes table
                raise
    finally:
        db.close()
    return found


def official_run() -> Table:
    """The latest daily run of the official files, one row per source it asked: its host, status, requests, note and
    what it found (state/official.sqlite), with the run's start, deadline, end and stop reason on every row."""
    path = _official_path()
    missing = Table(problem="No daily run of the official files on this host yet: the orchestrator's official flow "
                            f"records one every day, and `{OFFICIAL_RUN}` one now.")
    if not path.exists():
        return missing
    try:
        db = read_only(path)
        try:
            run = db.execute("select * from runs order by started_at desc limit 1").fetchone()
            rows = db.execute("select * from sources where run_id = ? order by started_at",
                              (run["run_id"],)).fetchall() if run else []
        finally:
            db.close()
        records = []
        if run and not rows:  # the run record is committed before its first source starts
            rows = [{"source": None, "host": None, "status": "waiting", "requests": 0, "note": None,
                     "started_at": None, "ended_at": None, "details": None}]
        for row in rows:
            details = json.loads(row["details"] or "{}")
            found = {**(details.get("changes") or {}), "checked": details.get("checked_unchanged", 0)}
            live = row["status"] == "running" and row["ended_at"] is None and row["source"] in CRAWL_STORES
            progress, note = {}, row["note"]
            if live:
                try:
                    progress = crawl_progress(row["source"], row["started_at"], run["ended_at"] or run["until"])
                except (OSError, sqlite3.Error) as problem:
                    note = f"Crawl progress unavailable: {_first_line(problem)}"
                found = {name: progress.get(name) for name in OFFICIAL_COUNTS}
            records.append({"source": row["source"], "host": row["host"], "status": row["status"],
                            "requests": progress.get("asked") if live else row["requests"], "note": note,
                            "started_at": row["started_at"], "ended_at": row["ended_at"],
                            **{name: found.get(name) if live else int(found.get(name) or 0)
                               for name in OFFICIAL_COUNTS},
                            "live": live, "last_request": progress.get("last_request"),
                            "not_modified": progress.get("not_modified") if live else 0,
                            "run_id": run["run_id"], "run_started_at": run["started_at"], "run_until": run["until"],
                            "run_ended_at": run["ended_at"], "run_stop": run["stop_reason"]})
    except sqlite3.Error as problem:
        return Table(problem=f"The official files' record could not be read: {_first_line(problem)}")
    if run is None:
        return missing
    columns = ["source", "host", "status", "requests", "note", "started_at", "ended_at", *OFFICIAL_COUNTS, "live",
               "last_request", "not_modified", "run_id", "run_started_at", "run_until", "run_ended_at", "run_stop"]
    return _with_times(Table(pd.DataFrame(records, columns=columns)), "started_at", "ended_at", "last_request",
                       "run_started_at", "run_until", "run_ended_at")


def official_runs(hours: int = 48) -> Table:
    """The daily runs of the official files that ended in the last `hours` hours or have not ended: their start,
    deadline, end and stop reason."""
    path = _official_path()
    if not path.exists():
        return Table(problem="No daily run of the official files on this host yet.")
    since = (datetime.now(UTC) - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        db = read_only(path)
        try:
            rows = db.execute("select run_id, started_at, until, ended_at, stop_reason from runs"
                              " where coalesce(ended_at, until) >= ? order by started_at", (since,)).fetchall()
        finally:
            db.close()
    except sqlite3.Error as problem:
        return Table(problem=f"The official files' record could not be read: {_first_line(problem)}")
    frame = pd.DataFrame([dict(row) for row in rows], columns=["run_id", "started_at", "until", "ended_at",
                                                               "stop_reason"])
    return _with_times(Table(frame), "started_at", "until", "ended_at")


def official_changes(limit: int = 10) -> Table:
    """The newest pages and files the latest official run found new, changed, gone, restored or moved, newest first,
    from the crawl stores it writes (refused files apart)."""
    path = _official_path()
    if not path.exists():
        return Table(problem="No daily run of the official files on this host yet.")
    try:
        db = read_only(path)
        try:
            sources = db.execute(
                "select s.source, s.started_at, coalesce(s.ended_at, r.ended_at, r.until) as until, s.crawl_run_id"
                " from sources s join runs r using (run_id)"
                " where r.run_id = (select run_id from runs order by started_at desc limit 1)"
                " and (s.crawl_run_id is not null or s.status = 'running')").fetchall()
        finally:
            db.close()
        found = []
        for row in sources:
            source = row["source"]
            store = config.data_root() / "state" / f"crawl-{source}.sqlite"
            if source not in CRAWL_STORES or not store.exists():
                continue
            db = read_only(store)
            try:
                found += [{"source": source, **dict(change)} for change in db.execute(
                    "select url, change, detected_type, observed_at from changes where observed_at between ? and ?"
                    " and (? is null or run_id = ?) and change != 'rejected' order by observed_at desc, id desc"
                    " limit ?", (row["started_at"], row["until"], row["crawl_run_id"], row["crawl_run_id"], limit))]
            except sqlite3.OperationalError as problem:
                if "no such table: changes" not in str(problem):
                    raise
            finally:
                db.close()
    except sqlite3.Error as problem:
        return Table(problem=f"The official files' record could not be read: {_first_line(problem)}")
    frame = pd.DataFrame(sorted(found, key=lambda row: row["observed_at"], reverse=True)[:limit],
                         columns=["source", "url", "change", "detected_type", "observed_at"])
    return _with_times(Table(frame), "observed_at")


def feed_history(limit: int | None = 14, hours: int | None = None) -> Table:
    """The latest attempts to publish the feed, oldest first: when, the outcome, the manifests written, their window,
    and how many pages and files they listed as changed (new, changed, gone, restored or moved)."""
    path = _official_path()
    if not path.exists():
        return Table(problem="No feed for SafeStart on this host yet.")
    since = (datetime.now(UTC) - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ") if hours else ""
    try:
        db = read_only(path)
        try:
            rows = db.execute("select attempted_at, outcome, first_sequence, last_sequence, window_start,"
                              " window_end, counts, note from publishes where attempted_at >= ? order by id desc"
                              " limit ?", (since, -1 if limit is None else limit)).fetchall()
        except sqlite3.OperationalError as problem:
            if "no such table: publishes" not in str(problem):
                raise
            rows = []  # a record from before the feed
        finally:
            db.close()
    except sqlite3.Error as problem:
        return Table(problem=f"The feed's record could not be read: {_first_line(problem)}")
    records = []
    for row in reversed(rows):
        counts = json.loads(row["counts"]) if row["counts"] else {}
        records.append({**{key: row[key] for key in ("attempted_at", "outcome", "first_sequence", "last_sequence",
                                                      "window_start", "window_end", "note")},
                        "changes": sum(int(counts.get(name) or 0) for name in OFFICIAL_COUNTS[:5]),
                        "checked": int(counts.get("checked") or 0)})
    frame = pd.DataFrame(records, columns=["attempted_at", "outcome", "first_sequence", "last_sequence",
                                           "window_start", "window_end", "note", "changes", "checked"])
    return _with_times(Table(frame), "attempted_at", "window_start", "window_end")


def official_feed() -> Table:
    """The feed for SafeStart, in one row: the newest manifest (exports/official/latest.json: its sequence, when it
    was generated and its age in hours, whether that is too old, its window, or why it cannot be read) and the latest
    attempt to publish one (its time, outcome and note)."""
    try:
        report = feed_status(config.data_root(), datetime.now(UTC))
    except (OSError, sqlite3.Error) as problem:
        return Table(problem=f"The feed's record could not be read: {_first_line(problem)}")
    latest, attempt = report["latest"] or {}, report["last_attempt"] or {}
    if not latest and not attempt:
        return Table(problem="No feed for SafeStart on this host yet: each daily run of the official files publishes "
                             "its manifest to exports/official/.")
    counts = latest.get("counts") or {}
    window = latest.get("window") or {}
    row = {"sequence": latest.get("sequence"), "generated_at": latest.get("generatedAt"),
           "changes": sum(counts.get(name, 0) for name in OFFICIAL_COUNTS[:5]), "checked": counts.get("checked", 0),
           "age_hours": latest.get("ageHours"), "stale": bool(latest.get("stale")), "problem": latest.get("problem"),
           "window_start": window.get("start"), "window_end": window.get("end"),
           "attempted_at": attempt.get("attempted_at"), "outcome": attempt.get("outcome"), "note": attempt.get("note")}
    return _with_times(Table(pd.DataFrame([row])), "generated_at", "attempted_at")


def official_purges() -> Table:
    """Each site's latest purge of retired originals on this host (state/official.sqlite)."""
    try:
        purges = latest_purges(config.data_root())
    except (OSError, sqlite3.Error) as problem:
        return Table(problem=f"The purge record could not be read: {_first_line(problem)}")
    frame = pd.DataFrame([{"site": site, **{key: found[key] for key in ("purged_at", "outcome", "deleted",
                                                                        "deleted_bytes", "note")}}
                          for site, found in sorted(purges.items())],
                         columns=["site", "purged_at", "outcome", "deleted", "deleted_bytes", "note"])
    return _with_times(Table(frame), "purged_at")
