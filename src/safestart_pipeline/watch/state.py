"""The watchers' own state: a small SQLite file, `<data root>/state/watch.sqlite`.

It remembers the id of every event a watcher has published, so nothing is published twice however checks overlap; the
dataset versions already compared; and named times, such as when each source was last checked. The watchers find a few
thousand records a week, so the file stays small. SAFESTART_WATCH_STATE (or `watch_state` in pipeline.local.toml) puts
it elsewhere, so a check can run against a data root it must only read.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

from ..config import data_root, setting

SCHEMA = """
create table if not exists emitted (
  event_id text primary key,
  emitted_at text not null
) without rowid;
create table if not exists compared (
  dataset text not null,
  version text not null,
  previous_version text not null,
  rows integer not null,
  new_rows integer not null,
  compared_at text not null,
  primary key (dataset, version)
);
create table if not exists times (name text primary key, value text not null);
"""
PARAMETERS = 500  # host parameters per query, well under SQLite's limit


def default_path() -> Path:
    return Path(setting("watch_state", str(data_root() / "state" / "watch.sqlite")))


class WatchState:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.db = sqlite3.connect(path, timeout=60)
        self.db.execute("pragma journal_mode=wal")
        self.db.executescript(SCHEMA)

    def close(self) -> None:
        self.db.commit()
        self.db.close()

    def commit(self) -> None:
        self.db.commit()

    # Published events ---------------------------------------------------------------------------------------------------

    def emitted(self, event_ids: Sequence[str]) -> set[str]:
        """The ids among these that were published before."""
        found: set[str] = set()
        for start in range(0, len(event_ids), PARAMETERS):
            chunk = event_ids[start:start + PARAMETERS]
            marks = ", ".join("?" for _ in chunk)
            rows = self.db.execute(f"select event_id from emitted where event_id in ({marks})", chunk)
            found.update(row[0] for row in rows)
        return found

    def add_emitted(self, event_ids: Sequence[str], at: str) -> None:
        self.db.executemany("insert or ignore into emitted (event_id, emitted_at) values (?, ?)",
                            [(event_id, at) for event_id in event_ids])

    def any_emitted(self, kind: str) -> bool:
        """Whether an event of this kind was ever published. Its ids start with '<kind>:', and ';' sorts right after ':',
        so the primary key answers with a range."""
        row = self.db.execute("select 1 from emitted where event_id >= ? and event_id < ? limit 1",
                              (f"{kind}:", f"{kind};")).fetchone()
        return row is not None

    # Dataset versions ---------------------------------------------------------------------------------------------------

    def compared_versions(self, dataset: str) -> set[str]:
        return {row[0] for row in self.db.execute("select version from compared where dataset = ?", (dataset,))}

    def add_comparison(self, dataset: str, version: str, previous_version: str, rows: int, new_rows: int,
                       at: str) -> None:
        self.db.execute(
            "insert or replace into compared (dataset, version, previous_version, rows, new_rows, compared_at)"
            " values (?, ?, ?, ?, ?, ?)", (dataset, version, previous_version, rows, new_rows, at))

    # Named times --------------------------------------------------------------------------------------------------------

    def time(self, name: str) -> datetime | None:
        row = self.db.execute("select value from times where name = ?", (name,)).fetchone()
        return datetime.fromisoformat(row[0]) if row else None

    def set_time(self, name: str, moment: datetime) -> None:
        self.db.execute("insert or replace into times (name, value) values (?, ?)", (name, moment.isoformat()))
