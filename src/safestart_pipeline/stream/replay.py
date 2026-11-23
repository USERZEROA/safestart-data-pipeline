"""`python -m safestart_pipeline.stream.replay`: MSHA's enforcement history pushed through Kafka in source-time order.

The replay reads the current Parquet file of four lake datasets (inspections, violations, 107(a) orders and accidents)
with DuckDB and sends one `replay` event per source record from a start date (2024-01-01 by default), oldest first.
Records become events through `stream/records.py`, the mapping the version watcher uses too, so a record the watcher
announced is the same event here: its source date, mine, title and details, and the one copy kept of a key MSHA
repeats. That copy is chosen among all of a key's copies before the first month is read, so a key whose copies fall in
different months is sent once. DuckDB sorts one calendar month at a time and hands it over in small chunks, so memory
stays flat however long the window (sorting all of 2024-2026 at once took the process to 258 MB).

The source clock: source day `start + n` begins `n * day_seconds` after the replay starts, and each day's events are
spread evenly across that day's interval, at most `max_rate` per second. At the default 5 seconds per day the stream
keeps the rhythm of the record (MSHA's inspectors work weekdays) and the 2024-2026 history takes about 90 minutes.
A replay that falls behind (a slow broker) sends without pausing until it has caught up. With `--loop` it starts again
from the start date after the newest record, reading the lake afresh; the event ids are unchanged, so the consumers
keep what they already have and store and count nothing twice.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import tempfile
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from datetime import date, timedelta
from pathlib import Path
from typing import Any, NamedTuple

import duckdb

from ..config import data_root
from .events import Event
from .kafka import make_publisher, stop_on_signals
from .records import BY_NAME, KINDS, Kind, event, mine_sql, one_copy_sql, select_sql

log = logging.getLogger(__name__)


class Row(NamedTuple):
    day: date
    day_index: int  # this record's place among its source day's records
    day_events: int  # how many records the source day has
    kind: str
    key: str
    record: dict[str, Any]  # the record as `records.mine_sql` reads it


def current_parquet(lake: Path, dataset: str) -> Path:
    manifest = json.loads((lake / dataset / "manifest.json").read_text(encoding="utf-8"))
    return lake / dataset / manifest["current"]


def next_month(day: date) -> date:
    return date(day.year + day.month // 12, day.month % 12 + 1, 1)


def parquet_sql(path: Path) -> str:
    return "read_parquet('" + path.as_posix().replace("'", "''") + "')"


def newest_sql(files: Mapping[str, str]) -> str:
    return "SELECT max(day) FROM (" + " UNION ALL ".join(
        f"SELECT max({kind.date}) AS day FROM {files[kind.name]} r" for kind in KINDS) + ")"


def repeated_sql(kind: Kind, file: str) -> str:
    """The copy kept of each key with a copy in [$start, $until) and another anywhere in the file."""
    window = f"SELECT r.{kind.key} FROM {file} r WHERE {kind.date} >= $start AND {kind.date} < $until"
    repeated = f"SELECT r.{kind.key} FROM {file} r WHERE r.{kind.key} IN ({window}) GROUP BY ALL HAVING count(*) > 1"
    return one_copy_sql(kind, f"(SELECT * FROM {file} r WHERE r.{kind.key} IN ({repeated}))")


def month_sql(kind: Kind, file: str) -> str:
    """The kind's records in [$start, $until): those with a key of their own, and the copy kept of a repeated key."""
    return (f"SELECT {select_sql(kind)} FROM {file} r WHERE {kind.date} >= $start AND {kind.date} < $until"
            f" AND r.{kind.key} IS NOT NULL AND r.{kind.key} NOT IN (SELECT key FROM repeated_{kind.name})"
            f" UNION ALL SELECT * FROM repeated_{kind.name} WHERE occurred >= $start AND occurred < $until")


def rows_sql(files: Mapping[str, str], mines: str) -> str:
    """Every kind's records in [$start, $until), source day by source day, each day's in the kind order of `KINDS`.
    The kinds read different columns, so a column holds one kind's values and is null in the others' records."""
    records = " UNION ALL BY NAME ".join(
        f"SELECT {order} AS kind_order, '{kind.name}' AS kind, * FROM ({month_sql(kind, files[kind.name])})"
        for order, kind in enumerate(KINDS))
    return (f"SELECT occurred AS day, row_number() OVER (PARTITION BY occurred ORDER BY kind_order, mine_id, key) - 1"
            f" AS day_index, count(*) OVER (PARTITION BY occurred) AS day_events, *"
            f" FROM ({mine_sql(records, mines)}) ORDER BY day, day_index")


def read_rows(lake: Path, start: date, until: date | None = None, *, chunk: int = 2000) -> Iterator[Row]:
    """The records from `start` up to (not including) `until`, in source-time order, one calendar month at a time."""
    files = {kind.name: parquet_sql(current_parquet(lake, kind.dataset)) for kind in KINDS}
    with tempfile.TemporaryDirectory(prefix="replay-duckdb-") as spill:
        con = duckdb.connect(config={"memory_limit": "256MB", "threads": 2, "temp_directory": spill})
        try:
            newest = con.execute(newest_sql(files)).fetchone()[0]
            end = min(until or date.max, newest + timedelta(days=1)) if newest else start
            for kind in KINDS:
                con.execute(f"CREATE TEMP TABLE repeated_{kind.name} AS {repeated_sql(kind, files[kind.name])}",
                            {"start": start, "until": end})
            query = rows_sql(files, parquet_sql(current_parquet(lake, "mines")))
            month = start
            while month < end:
                after = min(next_month(month), end)
                cursor = con.execute(query, {"start": month, "until": after})
                names = [column[0] for column in cursor.description]
                while rows := cursor.fetchmany(chunk):
                    for values in rows:
                        record = dict(zip(names, values, strict=True))
                        yield Row(record["day"], record["day_index"], record["day_events"], record["kind"],
                                  record["key"], record)
                month = after
        finally:
            con.close()


def build_event(row: Row, versions: Mapping[str, str]) -> Event:
    """The replay event of one record; `versions` names the lake version each kind was read from."""
    kind = BY_NAME[row.kind]
    return event(kind, row.record, mode="replay", source={"dataset": kind.dataset, "version": versions[kind.name]})


class Pacer:
    """The source clock: source day `start + n` begins `n * day_seconds` after the pacer was made, a day's events are
    spread evenly over that interval, and consecutive events are at least `1 / max_rate` seconds apart."""

    def __init__(self, start: date, day_seconds: float, max_rate: float | None = None, *,
                 now: Callable[[], float] = time.monotonic, sleep: Callable[[float], object] = time.sleep) -> None:
        self.start, self.day_seconds = start, day_seconds
        self.gap = 1.0 / max_rate if max_rate else 0.0
        self.now, self.sleep = now, sleep
        self.origin, self.last, self.behind = now(), -math.inf, 0.0

    def wait(self, day: date, index: int, count: int) -> None:
        """Pause until the `index`-th of a source day's `count` events is due."""
        due = max(self.origin + ((day - self.start).days + index / count) * self.day_seconds, self.last + self.gap)
        if (pause := due - self.now()) > 0:
            self.sleep(pause)
        self.last = self.now()
        self.behind = max(0.0, self.last - due)


def replay(send: Callable[[Event], None], lake: Path, *, start: date, until: date | None = None,
           day_seconds: float = 5.0, max_rate: float | None = None, loop: bool = False,
           stop: threading.Event | None = None, now: Callable[[], float] = time.monotonic) -> dict[str, Any]:
    """Send the records from `start` (up to `until`) at the source clock's pace; returns what was sent."""
    stop = stop or threading.Event()
    sent, passes, clock = 0, 0, None
    began = reported = now()
    while not stop.is_set():
        versions = {kind.name: current_parquet(lake, kind.dataset).stem for kind in KINDS}
        pacer = Pacer(start, day_seconds, max_rate, now=now, sleep=stop.wait)
        for row in read_rows(lake, start, until):
            pacer.wait(row.day, row.day_index, row.day_events)
            if stop.is_set():
                break
            send(build_event(row, versions))
            sent, clock = sent + 1, row.day
            if now() - reported >= 10:
                reported = now()
                lag = f", {pacer.behind:.1f} s behind" if day_seconds else ""  # unpaced, there is no schedule
                log.info("source clock %s: %d events sent (%.0f/s)%s", clock, sent, sent / (reported - began), lag)
        passes += 1
        if not loop:
            break
    seconds = now() - began
    return {"events": sent, "passes": passes, "source_clock": clock and clock.isoformat(), "seconds": round(seconds, 1),
            "events_per_second": round(sent / seconds, 1) if seconds else None}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Replay MSHA's enforcement history through Kafka")
    parser.add_argument("--start", type=date.fromisoformat, default=date(2024, 1, 1), help="first source day")
    parser.add_argument("--until", type=date.fromisoformat, help="stop before this source day")
    parser.add_argument("--day-seconds", type=float, default=5.0,
                        help="wall seconds per source day (0: as fast as the broker takes them)")
    parser.add_argument("--max-rate", type=float, help="at most this many events per second")
    parser.add_argument("--loop", action="store_true", help="start again from --start after the newest record")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    publisher = make_publisher()
    summary = replay(publisher.send, data_root() / "lake" / "msha_ogd", start=args.start, until=args.until,
                     day_seconds=args.day_seconds, max_rate=args.max_rate, loop=args.loop, stop=stop_on_signals())
    publisher.flush()
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
