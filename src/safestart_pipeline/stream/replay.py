"""`python -m safestart_pipeline.stream.replay`: MSHA's enforcement history pushed through Kafka in source-time order.

The replay reads the current Parquet file of four lake datasets (inspections, violations, 107(a) orders and accidents)
with DuckDB, joins each record to its mine for the district, state and commodity, and sends one `replay` event per
source record from a start date (2024-01-01 by default), oldest first. DuckDB sorts one calendar month at a time and
hands it over in small chunks, so memory stays flat however long the window (sorting all of 2024-2026 at once took
the process to 258 MB). A source key that appears twice in a dataset (51 violation numbers and one inspection number
do) is sent once.

The source clock: source day `start + n` begins `n * day_seconds` after the replay starts, and each day's events are
spread evenly across that day's interval, at most `max_rate` per second. At the default 5 seconds per day the stream
keeps the rhythm of the record (MSHA's inspectors work weekdays) and the 2024-2026 history takes about 90 minutes.
A replay that falls behind (a slow broker) sends without pausing until it has caught up. With `--loop` it starts again
from the start date after the newest record, reading the lake afresh; the event ids are unchanged, so the consumers
keep what they already have and store and count nothing twice.

Commodity is the mine's: 15% of the 2024-2026 violations carry the other program's C/M flag (a coal inspector citing
part 56 at a stone quarry), so the record's own flag is used only for a mine missing from the mines dataset. An
accident is serious when it was fatal, its degree of injury is days away from work or restricted duty (codes 03, 04,
05), or it records lost or restricted days: the degree is set when the accident is reported, while the day counts
arrive when the miner is back at work (1,003 code-03 accidents since 2024 have none yet).
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

DATASETS = {"inspection": "inspections", "violation": "violations", "order_107a": "orders_107a",
            "accident": "accidents"}
DEGREES = {
    "00": "Accident without injury", "01": "Fatal accident", "02": "Permanent disability", "03": "Days-away injury",
    "04": "Days-away and restricted injury", "05": "Restricted-duty injury", "06": "Injury without lost time",
    "07": "Occupational illness", "08": "Natural-causes injury", "09": "Non-employee injury", "10": "Other injury",
}
# Within one source day: inspections first (violations are cited during them), then violations, orders and accidents.
QUERY = """
WITH record AS (
    SELECT inspection_begin_dt AS day, 0 AS kind_order, 'inspection' AS kind, event_no AS key, mine_id,
           coal_metal_ind AS own_coal_metal, NULL AS own_state, NULL AS own_name,
           to_json({{'activity_code': activity_code, 'activity': activity, 'begin_date': inspection_begin_dt,
                     'end_date': inspection_end_dt}}) AS details
    FROM read_parquet({inspections}) WHERE inspection_begin_dt >= $start AND inspection_begin_dt < $until
    UNION ALL
    SELECT violation_issue_dt, 1, 'violation', violation_no, mine_id, coal_metal_ind, NULL, mine_name,
           to_json({{'event_no': event_no,
                     'cfr_section': nullif(regexp_extract(part_section, '^[0-9]+[.][0-9]+(-[0-9]+)?'), ''),
                     'part_section': part_section, 'sig_sub': CASE sig_sub WHEN 'Y' THEN true WHEN 'N' THEN false END,
                     'likelihood': likelihood, 'injury_illness': inj_illness, 'negligence': negligence,
                     'proposed_penalty': proposed_penalty}})
    FROM read_parquet({violations}) WHERE violation_issue_dt >= $start AND violation_issue_dt < $until
    UNION ALL
    SELECT violation_date_issued, 2, 'order_107a', violation_no, mine_id, coal_or_metal, state_abbreviation, mine_name,
           to_json({{'termination_date': termination_date}})
    FROM read_parquet({orders_107a}) WHERE violation_date_issued >= $start AND violation_date_issued < $until
    UNION ALL
    SELECT accident_dt, 3, 'accident', document_no, mine_id, coal_metal_ind, NULL, NULL,
           to_json({{'degree_injury_cd': degree_injury_cd, 'degree_injury': degree_injury,
                     'serious': coalesce(degree_injury_cd IN ('01', '03', '04', '05') OR days_lost > 0
                                         OR days_restrict > 0, false),
                     'classification': classification, 'accident_type': accident_type, 'no_injuries': no_injuries,
                     'days_lost': days_lost}})
    FROM read_parquet({accidents}) WHERE accident_dt >= $start AND accident_dt < $until
), chosen AS (
    SELECT * FROM record WHERE key IS NOT NULL
    QUALIFY row_number() OVER (PARTITION BY kind, key ORDER BY details) = 1
)
SELECT c.day, row_number() OVER (PARTITION BY c.day ORDER BY c.kind_order, c.mine_id, c.key) - 1 AS day_index,
       count(*) OVER (PARTITION BY c.day) AS day_events, c.kind, c.key, c.mine_id, m.district,
       coalesce(m.state, c.own_state), coalesce(m.coal_metal_ind, c.own_coal_metal),
       coalesce(m.current_mine_name, c.own_name), c.details
FROM chosen c LEFT JOIN read_parquet({mines}) m ON m.mine_id = c.mine_id
ORDER BY c.day, day_index
"""
NEWEST = """
SELECT max(day) FROM (
    SELECT max(inspection_begin_dt) AS day FROM read_parquet({inspections})
    UNION ALL SELECT max(violation_issue_dt) FROM read_parquet({violations})
    UNION ALL SELECT max(violation_date_issued) FROM read_parquet({orders_107a})
    UNION ALL SELECT max(accident_dt) FROM read_parquet({accidents}))
"""
log = logging.getLogger(__name__)


class Row(NamedTuple):
    day: date
    day_index: int  # this record's place among its source day's records
    day_events: int  # how many records the source day has
    kind: str
    key: str
    mine_id: str | None
    district: str | None
    state: str | None
    coal_metal: str | None
    mine_name: str | None
    details: str  # JSON


def current_parquet(lake: Path, dataset: str) -> Path:
    manifest = json.loads((lake / dataset / "manifest.json").read_text(encoding="utf-8"))
    return lake / dataset / manifest["current"]


def next_month(day: date) -> date:
    return date(day.year + day.month // 12, day.month % 12 + 1, 1)


def read_rows(lake: Path, start: date, until: date | None = None, *, chunk: int = 2000) -> Iterator[Row]:
    """The records from `start` up to (not including) `until`, in source-time order, one calendar month at a time."""
    files = {name: current_parquet(lake, name) for name in (*DATASETS.values(), "mines")}
    paths = {name: "'" + path.as_posix().replace("'", "''") + "'" for name, path in files.items()}
    with tempfile.TemporaryDirectory(prefix="replay-duckdb-") as spill:
        con = duckdb.connect(config={"memory_limit": "256MB", "threads": 2, "temp_directory": spill})
        try:
            newest = con.execute(NEWEST.format(**paths)).fetchone()[0]
            end = min(until or date.max, newest + timedelta(days=1)) if newest else start
            month = start
            while month < end:
                after = min(next_month(month), end)
                cursor = con.execute(QUERY.format(**paths), {"start": month, "until": after})
                while rows := cursor.fetchmany(chunk):
                    yield from (Row(*row) for row in rows)
                month = after
        finally:
            con.close()


def title(kind: str, details: Mapping[str, Any], place: str) -> str:
    if kind == "inspection":
        return f"{details['activity'] or 'Inspection'} began at {place}"
    if kind == "violation":
        cited = f" of 30 CFR {details['part_section']}" if details["part_section"] else ""
        return f"{'S&S violation' if details['sig_sub'] else 'Violation'}{cited} at {place}"
    if kind == "order_107a":
        return f"Imminent-danger order (107(a)) at {place}"
    what = f"{DEGREES.get(details['degree_injury_cd'], 'Accident')} at {place}"
    return f"{what}: {details['classification'].capitalize()}" if details["classification"] else what


def build_event(row: Row, versions: Mapping[str, str]) -> Event:
    """The replay event of one record; `versions` names the lake version each kind was read from."""
    details = json.loads(row.details)
    place = row.mine_name or (f"mine {row.mine_id}" if row.mine_id else "an unknown mine")
    return Event.new(
        kind=row.kind, key=row.key, mode="replay", occurred_at=row.day, title=title(row.kind, details, place),
        details=details, source={"dataset": DATASETS[row.kind], "version": versions[row.kind]}, mine_id=row.mine_id,
        district=row.district, state=row.state, coal_metal=row.coal_metal,
    )


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
        versions = {kind: current_parquet(lake, dataset).stem for kind, dataset in DATASETS.items()}
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
