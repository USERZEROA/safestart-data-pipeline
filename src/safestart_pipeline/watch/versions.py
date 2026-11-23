"""New enforcement records, found by comparing consecutive versions of MSHA's datasets held in the lake.

MSHA republishes each dataset whole every week, and the lake keeps every version as its own Parquet file
(`lake/msha_ogd.py`). A record is new when its key (violation number, inspection event number, accident document
number, 107(a) order number) is in one version and not in the version before; a record that only changed keeps its key
and is left to the replay and the warehouse. Each new record becomes one live event, and each compared version one
dataset_version event. DuckDB compares the versions straight from the Parquet files within a fixed memory limit, and
only the new records reach Python: 2,150 of the 3.1 million violations on 2026-11-23.

The watcher compares from the newest version it has already compared (on a first run, the second newest held version)
to the newest held version, one consecutive pair at a time, so versions that arrive while it is stopped are not
skipped, and a record that leaves a version and comes back is not announced twice (`publish.publish_new`). A version
in which a large share of the records looks new has changed its format or keys rather than gained news, so it is
reported as an error and left uncompared. Each mine's district, state and name come from the lake's current mines
dataset, so consumers need no lookup.
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from email.utils import parsedate_to_datetime
from itertools import chain
from pathlib import Path
from typing import Any

import duckdb

from ..stream.events import COMMODITIES, Event, utc_now
from ..verify import read_only
from .checks import Check
from .publish import Publisher, publish_new
from .state import WatchState

SOURCE = "versions"
MEMORY_LIMIT = "1GB"  # the cloud host has 4 GB, shared with Kafka and PostgreSQL
THREADS = 2
FETCH_ROWS = 1000
# A version is refused when more than this many of its records, and more than this share of them, look new.
SUSPECT_NEW_ROWS = 1000
SUSPECT_SHARE = 0.10
FATAL = "01"
# Degrees of injury that mean days away from work or restricted work. A new accident's day counts usually come later:
# of the 66 "days away from work only" accidents new on 2026-11-23, 58 had no count yet.
LOST_OR_RESTRICTED = frozenset({"03", "04", "05"})
YES_NO = {"Y": True, "N": False}
SECTION = re.compile(r"[^(/\s]+")

Row = dict[str, Any]


def iso_date(value: date | None) -> str | None:
    return value.isoformat() if value else None


def cfr_section(part_section: str | None) -> str | None:
    """The section a citation names, without its paragraph: '56.14107(a)' -> '56.14107', '56.5001(a)/.5005' ->
    '56.5001'."""
    match = SECTION.match(part_section.strip()) if part_section else None
    return match.group(0) if match else None


def is_serious(degree: str | None, days_lost: int | None, days_restrict: int | None) -> bool:
    """Fatal, or with lost or restricted work days, by the day counts or by a degree of injury that means them."""
    return degree == FATAL or degree in LOST_OR_RESTRICTED or (days_lost or 0) > 0 or (days_restrict or 0) > 0


def violation_values(row: Row, mine: str) -> tuple[str, Row]:
    section, sig_sub = cfr_section(row["part_section"]), YES_NO.get(row["sig_sub"])
    title = ("S&S violation" if sig_sub else "Violation") + (f" of 30 CFR {section}" if section else "") + f" at {mine}"
    return title, {"event_no": row["event_no"], "cfr_section": section, "part_section": row["part_section"],
                   "sig_sub": sig_sub, "likelihood": row["likelihood"], "injury_illness": row["injury_illness"],
                   "negligence": row["negligence"], "proposed_penalty": row["proposed_penalty"]}


def inspection_values(row: Row, mine: str) -> tuple[str, Row]:
    return f"{row['activity'] or 'Inspection'} at {mine}", {
        "activity_code": row["activity_code"], "activity": row["activity"], "begin_date": iso_date(row["begin_date"]),
        "end_date": iso_date(row["end_date"])}


def accident_values(row: Row, mine: str) -> tuple[str, Row]:
    degree = row["degree_injury_cd"]
    serious = is_serious(degree, row["days_lost"], row["days_restrict"])
    what = "Fatal accident" if degree == FATAL else "Serious accident" if serious else "Accident"
    title = f"{what} at {mine}" + (f" ({row['classification'].lower()})" if row["classification"] else "")
    return title, {"degree_injury_cd": degree, "degree_injury": row["degree_injury"], "serious": serious,
                   "classification": row["classification"], "accident_type": row["accident_type"],
                   "no_injuries": row["no_injuries"], "days_lost": row["days_lost"]}


def order_values(row: Row, mine: str) -> tuple[str, Row]:
    return f"107(a) imminent danger order at {mine}", {"termination_date": iso_date(row["termination_date"])}


@dataclass(frozen=True)
class Watched:
    """How one dataset's new records become events."""

    kind: str
    key: str  # the key column
    label: str  # the dataset in titles
    date: str  # SQL over the record `r`: when it happened at the source
    columns: str  # SQL over `r`: coal_metal, record_mine, record_state, then the kind's own values
    values: Callable[[Row, str], tuple[str, Row]]  # (row, mine) -> (title, details)


WATCHED: dict[str, Watched] = {
    "violations": Watched(
        "violation", "violation_no", "violations", "coalesce(r.violation_issue_dt, r.violation_occur_dt)",
        "r.coal_metal_ind as coal_metal, r.mine_name as record_mine, null as record_state, r.event_no, r.part_section,"
        " r.sig_sub, r.likelihood, r.inj_illness as injury_illness, r.negligence, r.proposed_penalty",
        violation_values),
    "inspections": Watched(
        "inspection", "event_no", "inspections", "coalesce(r.inspection_begin_dt, r.inspection_end_dt)",
        "r.coal_metal_ind as coal_metal, null as record_mine, null as record_state, r.activity_code, r.activity,"
        " r.inspection_begin_dt as begin_date, r.inspection_end_dt as end_date",
        inspection_values),
    "accidents": Watched(
        "accident", "document_no", "accidents", "r.accident_dt",
        "r.coal_metal_ind as coal_metal, null as record_mine, null as record_state, r.degree_injury_cd,"
        " r.degree_injury, r.classification, r.accident_type, r.no_injuries, r.days_lost, r.days_restrict",
        accident_values),
    "orders_107a": Watched(
        "order_107a", "violation_no", "107(a) orders", "r.violation_date_issued",
        "r.coal_or_metal as coal_metal, r.mine_name as record_mine, r.state_abbreviation as record_state,"
        " r.termination_date",
        order_values),
}


class SuspectVersion(ValueError):
    """A version in which too many records look new to be news."""


# Versions held in the lake ------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Version:
    name: str  # the Parquet file's stem: the first 16 hex digits of the source file's SHA-256
    path: Path
    rows: int
    source_file: str
    source_sha256: str
    built_at: str


def held_versions(folder: Path) -> list[Version]:
    """The versions the lake holds of one dataset, oldest first. The manifest lists them in the order the lake built
    them, which is the order they were downloaded, and names the newest as current."""
    manifest_path = folder / "manifest.json"
    if not manifest_path.exists():
        return []
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    held = [Version(Path(item["parquet"]).stem, folder / item["parquet"], item["rows"], item["source_file"],
                    item["source_sha256"], item["built_at"])
            for item in manifest["versions"] if (folder / item["parquet"]).exists()]
    return sorted(held, key=lambda version: version.path.name == manifest.get("current"))


def pending_pairs(held: list[Version], compared: set[str]) -> list[tuple[Version, Version]]:
    """Consecutive (older, newer) pairs from the newest version already compared, or else the second newest, to the
    newest."""
    done = [index for index, version in enumerate(held) if version.name in compared]
    start = done[-1] if done else max(len(held) - 2, 0)
    return list(zip(held[start:], held[start + 1:]))


def current_mines(lake: Path) -> Path:
    held = held_versions(lake / "mines")
    if not held:
        raise FileNotFoundError(f"{lake / 'mines'}: no mines dataset to give each record its mine's district and state")
    return held[-1].path


def published_at(bulk_db: Path, version: Version) -> datetime | str:
    """When MSHA published the version: its file's Last-Modified time in the bulk store, else when the lake built it."""
    if bulk_db.exists():
        db = read_only(bulk_db)
        try:
            row = db.execute("select last_modified from versions where sha256 = ? and last_modified is not null",
                             (version.source_sha256,)).fetchone()
        finally:
            db.close()
        if row:
            try:
                return parsedate_to_datetime(row[0])
            except (TypeError, ValueError):
                pass
    return version.built_at


# Comparison -----------------------------------------------------------------------------------------------------------


def fresh_sql(watched: Watched) -> str:
    """Records of the newer version whose key the older one lacks, one per key (MSHA repeats a few records whole)."""
    key = watched.key
    return (f"select * from read_parquet($new) r where r.{key} is not null"
            f" and not exists (select 1 from read_parquet($old) o where o.{key} = r.{key})"
            f" qualify row_number() over (partition by r.{key}) = 1")


def count_sql(watched: Watched) -> str:
    return f"select count(*), count(*) filter (where {watched.date} is null) from ({fresh_sql(watched)}) r"


def rows_sql(watched: Watched) -> str:
    return (f"select n.*, m.district, coalesce(m.state, n.record_state) as state,"
            f" coalesce(m.current_mine_name, n.record_mine) as mine"
            f" from (select r.{watched.key} as key, r.mine_id, {watched.date} as occurred, {watched.columns}"
            f" from ({fresh_sql(watched)}) r) n"
            f" left join read_parquet($mines) m on m.mine_id = n.mine_id"
            f" where n.occurred is not null order by n.occurred, n.key")


def fetch_rows(cursor: duckdb.DuckDBPyConnection) -> Iterator[Row]:
    names = [column[0] for column in cursor.description]
    while batch := cursor.fetchmany(FETCH_ROWS):
        yield from (dict(zip(names, values, strict=True)) for values in batch)


def record_events(rows: Iterator[Row], watched: Watched, source: dict[str, str]) -> Iterator[Event]:
    for row in rows:
        mine = row["mine"] or f"mine {row['mine_id']}"
        title, details = watched.values(row, mine)
        yield Event.new(kind=watched.kind, key=row["key"], mode="live", occurred_at=row["occurred"], title=title,
                        details=details, source=source, mine_id=row["mine_id"], district=row["district"],
                        state=row["state"], coal_metal=row["coal_metal"] if row["coal_metal"] in COMMODITIES else None)


def version_event(dataset: str, watched: Watched, older: Version, newer: Version, new_rows: int,
                  published: datetime | str) -> Event:
    return Event.new(
        kind="dataset_version", key=f"{dataset}:{newer.name}", mode="live", occurred_at=published,
        title=f"New MSHA {watched.label} data: {new_rows:,} new of {newer.rows:,} records",
        details={"dataset": dataset, "version": newer.name, "previous_version": older.name, "rows": newer.rows,
                 "new_rows": new_rows},
        source={"dataset": dataset, "file": newer.source_file, "sha256": newer.source_sha256})


def compare(db: duckdb.DuckDBPyConnection, data_root: Path, state: WatchState, publisher: Publisher, dataset: str,
            older: Version, newer: Version, mines: Path, watched: Watched) -> tuple[int, str]:
    """Publish one pair's new records and its dataset_version event, then remember the pair. Returns the count of events
    sent and a note."""
    files = {"new": str(newer.path), "old": str(older.path)}
    new_rows, undated = db.execute(count_sql(watched), files).fetchone()
    if new_rows > SUSPECT_NEW_ROWS and new_rows > SUSPECT_SHARE * newer.rows:
        raise SuspectVersion(f"{newer.name}: {new_rows:,} of {newer.rows:,} records look new against {older.name},"
                             " so its keys or format changed; left uncompared")
    summary = version_event(dataset, watched, older, newer, new_rows,
                            published_at(data_root / "state" / "bulk.sqlite", newer))
    rows = fetch_rows(db.execute(rows_sql(watched), {**files, "mines": str(mines)}))
    source = {"dataset": dataset, "version": newer.name}
    sent = publish_new(chain(record_events(rows, watched, source), [summary]), publisher, state)
    state.add_comparison(dataset, newer.name, older.name, newer.rows, new_rows, utc_now())
    state.commit()
    note = f"{dataset} {older.name} -> {newer.name}: {new_rows:,} new"
    return sent, note + (f" ({undated:,} without a date left out)" if undated else "")


def connect(spill: Path) -> duckdb.DuckDBPyConnection:
    db = duckdb.connect()
    db.execute(f"set memory_limit = '{MEMORY_LIMIT}'")
    db.execute(f"set threads = {THREADS}")
    db.execute("set temp_directory = ?", [str(spill)])
    return db


def check_versions(data_root: Path, state: WatchState, publisher: Publisher,
                   datasets: Mapping[str, Watched] = WATCHED) -> Check:
    """Compare every watched dataset's versions not compared yet and publish what is new."""
    lake = data_root / "lake" / "msha_ogd"
    found, notes, errors = 0, [], []
    with connect(state.path.parent / "watch-duckdb.tmp") as db:
        for dataset, watched in datasets.items():
            try:  # one dataset's damaged manifest, file or version does not hold back the others
                for older, newer in pending_pairs(held_versions(lake / dataset), state.compared_versions(dataset)):
                    sent, note = compare(db, data_root, state, publisher, dataset, older, newer, current_mines(lake),
                                         watched)
                    found += sent
                    notes.append(note)
            except (duckdb.Error, sqlite3.Error, OSError, KeyError, ValueError) as problem:
                errors.append(f"{dataset}: {problem}"[:500])
    status = "error" if errors else "ok" if found else "unchanged"
    return Check(SOURCE, status, found, "; ".join(errors + notes) or "no new versions")
