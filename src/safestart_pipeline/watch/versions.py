"""New enforcement records, found by comparing consecutive versions of MSHA's datasets held in the lake.

MSHA republishes each dataset whole every week, and the lake keeps every version as its own Parquet file
(`lake/msha_ogd.py`). A record is new when its key (violation number, inspection event number, accident document
number, 107(a) order number) is in one version and not in the version before; a record that only changed keeps its key
and is left to the replay and the warehouse. Each new record becomes one live event, and each compared version one
dataset_version event. DuckDB compares the versions straight from the Parquet files within a fixed memory limit, and
only the new records reach Python: 2,150 of the 3.1 million violations on 2026-11-23. Records become events through
`stream/records.py`, the mapping the replay uses too, so a record announced here is the same event the replay sends:
its source date, mine, title and details, and the one copy kept of a key MSHA repeats.

The watcher compares from the newest version it has already compared (on a first run, the second newest held version)
to the newest held version, one consecutive pair at a time, so versions that arrive while it is stopped are not
skipped, and a record that leaves a version and comes back is not announced twice (`publish.publish_new`). A version
in which a large share of the records looks new has changed its format or keys rather than gained news: it is refused,
reported as an error in the check that finds it, and none of its records is announced, nor the version itself. It is
remembered as refused all the same, and the next version is compared with it, since the versions after a change of
keys keep the new keys; otherwise every later version would be held back behind it.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from itertools import chain
from pathlib import Path
from typing import Any

import duckdb

from ..stream.events import Event, utc_now
from ..stream.records import BY_DATASET, Kind, event, mine_sql, one_copy_sql
from ..verify import read_only
from .checks import Check
from .publish import Publisher, publish_new
from .state import WatchState

SOURCE = "versions"
# Well below the watcher container's 512 MiB (docker-compose.yml): comparing two versions of the 3.1 million
# violations peaked at 343 MiB in the image, against 607 MiB with 1 GB; DuckDB spills to disk beyond it.
MEMORY_LIMIT = "256MB"
THREADS = 2
FETCH_ROWS = 1000
# A version is refused when more than this many of its records, and more than this share of them, look new.
SUSPECT_NEW_ROWS = 1000
SUSPECT_SHARE = 0.10
LABELS = {"orders_107a": "107(a) orders"}  # a dataset in titles, where its name will not do
WATCHED: Mapping[str, Kind] = BY_DATASET

Row = dict[str, Any]


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


def fresh_sql(kind: Kind) -> str:
    """Records of the newer version whose key the older one lacks, one copy per key (`records.one_copy_sql`)."""
    newer = (f"(select * from read_parquet($new) n"
             f" where not exists (select 1 from read_parquet($old) o where o.{kind.key} = n.{kind.key}))")
    return one_copy_sql(kind, newer)


def count_sql(kind: Kind) -> str:
    return f"select count(*), count(*) filter (where occurred is null) from ({fresh_sql(kind)})"


def rows_sql(kind: Kind) -> str:
    return (f"select * from ({mine_sql(fresh_sql(kind), 'read_parquet($mines)')})"
            " where occurred is not null order by occurred, key")


def fetch_rows(cursor: duckdb.DuckDBPyConnection) -> Iterator[Row]:
    names = [column[0] for column in cursor.description]
    while batch := cursor.fetchmany(FETCH_ROWS):
        yield from (dict(zip(names, values, strict=True)) for values in batch)


def version_event(dataset: str, older: Version, newer: Version, new_rows: int, published: datetime | str) -> Event:
    return Event.new(
        kind="dataset_version", key=f"{dataset}:{newer.name}", mode="live", occurred_at=published,
        title=f"New MSHA {LABELS.get(dataset, dataset)} data: {new_rows:,} new of {newer.rows:,} records",
        details={"dataset": dataset, "version": newer.name, "previous_version": older.name, "rows": newer.rows,
                 "new_rows": new_rows},
        source={"dataset": dataset, "file": newer.source_file, "sha256": newer.source_sha256})


def compare(db: duckdb.DuckDBPyConnection, data_root: Path, state: WatchState, publisher: Publisher, dataset: str,
            older: Version, newer: Version, mines: Path, kind: Kind) -> tuple[int, str]:
    """Publish one pair's new records and its dataset_version event, then remember the pair. Returns the count of events
    sent and a note. SuspectVersion when too many records look new: the newer version is then remembered as refused,
    with nothing published, so that it is the baseline of the next pair."""
    files = {"new": str(newer.path), "old": str(older.path)}
    new_rows, undated = db.execute(count_sql(kind), files).fetchone()
    if new_rows > SUSPECT_NEW_ROWS and new_rows > SUSPECT_SHARE * newer.rows:
        state.add_comparison(dataset, newer.name, older.name, newer.rows, new_rows, utc_now(), refused=True)
        state.commit()
        raise SuspectVersion(f"{newer.name}: {new_rows:,} of {newer.rows:,} records look new against {older.name},"
                             " so its keys or format changed; refused, nothing announced, and the next version is"
                             " compared with it")
    summary = version_event(dataset, older, newer, new_rows, published_at(data_root / "state" / "bulk.sqlite", newer))
    rows = fetch_rows(db.execute(rows_sql(kind), {**files, "mines": str(mines)}))
    source = {"dataset": dataset, "version": newer.name}
    records = (event(kind, row, mode="live", source=source) for row in rows)
    sent = publish_new(chain(records, [summary]), publisher, state)
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
                   datasets: Mapping[str, Kind] = WATCHED) -> Check:
    """Compare every watched dataset's versions not compared yet and publish what is new."""
    lake = data_root / "lake" / "msha_ogd"
    found, notes, errors = 0, [], []
    with connect(state.path.parent / "watch-duckdb.tmp") as db:
        for dataset, kind in datasets.items():
            try:  # one dataset's damaged manifest, file or version does not hold back the others
                for older, newer in pending_pairs(held_versions(lake / dataset), state.compared_versions(dataset)):
                    try:
                        sent, note = compare(db, data_root, state, publisher, dataset, older, newer,
                                             current_mines(lake), kind)
                    except SuspectVersion as refused:  # remembered as the baseline: the next pair goes on
                        errors.append(f"{dataset}: {refused}"[:500])
                        continue
                    found += sent
                    notes.append(note)
            except (duckdb.Error, sqlite3.Error, OSError, KeyError, ValueError) as problem:
                errors.append(f"{dataset}: {problem}"[:500])
    status = "error" if errors else "ok" if found else "unchanged"
    return Check(SOURCE, status, found, "; ".join(errors + notes) or "no new versions")
