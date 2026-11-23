"""`python -m safestart_pipeline.stream.analytics`: the analytics consumer (group `safestart-analytics`) keeps the count
of events per source day, kind and district in `stream.daily_counts`, and forecasts every completed source day into
`stream.forecasts` with its residual and an anomaly flag.

Exactly once. Kafka delivers at least once: a replay loop sends the same events again, and a crash before the offset
commit redelivers a batch. An event is added to daily_counts only by the statement that first writes its event_id to
the ledger `stream.counted_events`, and the batch's offsets are committed after that transaction, so each count moves
once per event however often the event arrives. One transaction runs at a time (an advisory lock), so a second
instance of the consumer cannot interleave its counts with a forecast being computed.

Completed days. A source day is complete on a topic once every partition the consumer reads has carried a later day or
has been read to its end. The replay sends its events in source-time order and stamps each with the last source day of
its pass (`source["through"]`). While a pass is under way (the topic has not yet carried a replayed event of that
day), a partition stands at the newest day of the replayed events it has carried; live events, which a watcher finds
in no particular order and often far ahead of the replay, are counted but never move the clock, so no day is forecast
before the replay has passed it (a consumer restarted during a pass learns of it from the pass's next event). Once
the pass has reached its last day, a partition stands at the newer of its newest replayed and live days, so the days
the watchers find after the replay are forecast in the same process as they complete; a partition still being read
keeps its replayed day until that reaches the pass's last day, since the rest of the pass may still be on it.
safety.sources, which no replay feeds, is placed by its live days throughout. A store without forecasts waits for the
replay's first event on the enforcement topic: placed by live events alone, every day before the newest of them would
look complete, and the whole history would be forecast at once from nearly empty counts. An event that arrives for a
day already forecast (a record found late) changes that day's count, and the forecasts that depend on the day are
computed again: a forecast row is a function of daily_counts alone, so whenever it is computed it comes out the same.

Forecast ("ewma-weekday"). Enforcement follows the work week: since 2024 MSHA has cited 512 violations on an average
Tuesday and 16 on an average Saturday. Each day of a series (kind, district) is therefore forecast from the same weekday
in the previous WEEKS weeks, by an exponentially weighted moving average with smoothing ALPHA whose weights are
normalized over the weeks the stream covers (as pandas' `ewm(adjust=True)`). It is a one-step-ahead forecast: day d
uses only days before d. The residual is observed minus predicted, and a day is an anomaly when |residual| exceeds
THRESHOLD times a scale: the square root of the weighted variance of those weeks around the prediction, but at least
that of a Poisson count (the prediction itself) and at least 1, so that one or two events more in a quiet series are
never flagged. A day is judged only with MIN_HISTORY weeks of history. A series has a row for a day when it has an
event that day or in that day's history.

The parameters were chosen on the lake's 52 enforcement series (4 kinds by 13 districts, January 2024 to November
2026), scored on the series-days that have history: this method's mean absolute error is 3.65 events, against 6.3 for
the best plain day-to-day EWMA, which misses the weekly cycle (smoothing 1, that is yesterday's count; 7.1 with this
method's smoothing of 0.1), and 3.9 to 4.0 for shorter memories (smoothing 0.2 or 0.3 over 12 weeks). It flags 1.5% of
the series-days it judges (accidents 0.95%, inspections 1.85%, violations 2.15%, 107(a) orders none): public holidays,
and days such as 2024-05-22, when MSHA opened 1,664 "Special Emphasis Program" inspections and every district's
inspection count was flagged (266 in M2 against 22.5 forecast).
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import time
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, timedelta
from typing import NamedTuple

import psycopg

from ..config import postgres_url
from .events import (
    ENFORCEMENT_KINDS,
    SOURCE_KINDS,
    TOPIC_ENFORCEMENT,
    TOPIC_SOURCES,
    Event,
)
from .kafka import Batch, decode, make_consumer, run, stop_on_signals

GROUP = "safestart-analytics"
ALPHA = 0.1  # smoothing: each earlier week weighs 0.9 times the week after it (a half-life of 6.6 weeks)
WEEKS = 26  # history of each forecast: the same weekday in the 26 weeks before (0.9 ** 26 = 6.5% weight left out)
THRESHOLD = 3.0  # an anomaly is a residual beyond 3 scales, the usual control-chart limit
MIN_HISTORY = 4  # weeks of history before a day can be called an anomaly
METHOD = f"ewma-weekday(alpha={ALPHA},weeks={WEEKS},k={THRESHOLD})"
TOPIC_KINDS = {TOPIC_ENFORCEMENT: sorted(ENFORCEMENT_KINDS), TOPIC_SOURCES: sorted(SOURCE_KINDS)}
LOCK = 0x5AFE57A7  # pg_advisory_xact_lock key held by every analytics transaction

COUNT = """
WITH counted AS (
    INSERT INTO stream.counted_events (event_id, day, kind, district, serious)
    SELECT * FROM unnest(%s::text[], %s::date[], %s::text[], %s::text[], %s::boolean[])
    ON CONFLICT (event_id) DO NOTHING
    RETURNING day, kind, district, serious
), added AS (
    SELECT day, kind, district, count(*)::int AS events, (count(*) FILTER (WHERE serious))::int AS serious
    FROM counted GROUP BY day, kind, district
), kept AS (
    INSERT INTO stream.daily_counts AS c (day, kind, district, events, serious)
    SELECT day, kind, district, events, serious FROM added
    ON CONFLICT (day, kind, district) DO UPDATE
    SET events = c.events + excluded.events, serious = c.serious + excluded.serious, updated_at = now()
)
SELECT kind, district, day, events FROM added
"""
FORECAST = """
INSERT INTO stream.forecasts AS f (day, kind, district, observed, predicted, residual, anomaly, method)
SELECT * FROM unnest(%s::date[], %s::text[], %s::text[], %s::float8[], %s::float8[], %s::float8[], %s::boolean[],
                     %s::text[])
ON CONFLICT (day, kind, district) DO UPDATE
SET observed = excluded.observed, predicted = excluded.predicted, residual = excluded.residual,
    anomaly = excluded.anomaly, method = excluded.method, updated_at = now()
WHERE (f.observed, f.predicted, f.residual, f.anomaly, f.method)
      IS DISTINCT FROM (excluded.observed, excluded.predicted, excluded.residual, excluded.anomaly, excluded.method)
"""
log = logging.getLogger(__name__)
SeriesDay = tuple[str, str, date]  # (kind, district, day)
Partition = tuple[str, int]  # (topic, partition)


class Item(NamedTuple):
    """An event as the counts see it."""

    event_id: str
    day: date
    kind: str
    district: str
    serious: bool


def item(event: Event) -> Item:
    """Serious: a significant-and-substantial violation, or an accident whose event says it is serious (the replay and
    the watcher both mark it by definitions.is_serious)."""
    flag = {"violation": "sig_sub", "accident": "serious"}.get(event.kind)
    return Item(event.event_id, date.fromisoformat(event.occurred_at[:10]), event.kind, event.district or "",
                flag is not None and event.details.get(flag) is True)


def pass_end(event: Event) -> date | None:
    """The last source day of the replay pass that sent the event (`source["through"]`, see stream/replay.py); None
    for a live event or a replayed one without a readable stamp."""
    try:
        return date.fromisoformat(event.source["through"]) if event.mode == "replay" else None
    except (KeyError, ValueError):
        return None


class Forecast(NamedTuple):
    predicted: float
    residual: float
    anomaly: bool


def forecast(observed: int, history: Sequence[int]) -> Forecast:
    """One day's forecast from the same weekday's counts in earlier weeks, the most recent first."""
    weights = [(1 - ALPHA) ** age for age in range(len(history))]
    total = sum(weights)
    predicted = sum(w * count for w, count in zip(weights, history)) / total if history else 0.0
    variance = sum(w * (count - predicted) ** 2 for w, count in zip(weights, history)) / total if history else 0.0
    residual = observed - predicted
    scale = math.sqrt(max(variance, predicted, 1.0))
    return Forecast(predicted, residual, len(history) >= MIN_HISTORY and abs(residual) > THRESHOLD * scale)


def forecast_rows(counts: Mapping[tuple[str, str], Mapping[date, int]], targets: Iterable[SeriesDay],
                  origin: date) -> list[tuple]:
    """The forecasts rows of the target series-days that have an event on the day or in its history; `counts` maps
    (kind, district) to events by day, and `origin` is the first day the stream covers."""
    rows = []
    for kind, district, day in sorted(set(targets)):
        series = counts.get((kind, district), {})
        weeks = [day - timedelta(weeks=age) for age in range(1, WEEKS + 1)]
        history = [series.get(week, 0) for week in weeks if week >= origin]
        observed = series.get(day, 0)
        if observed or any(history):
            result = forecast(observed, history)
            rows.append((day, kind, district, observed, result.predicted, result.residual, result.anomaly, METHOD))
    return rows


def plan(done: date | None, through: date | None, origin: date, former_origin: date | None,
         changed: Iterable[SeriesDay]) -> tuple[list[date], set[SeriesDay]]:
    """What to forecast after a batch, given the last day forecast so far (`done`) and the last complete day
    (`through`): the newly complete days, for every series; and the series-days whose history holds a count that
    changed on a day already forecast (that day and the same weekday in the next WEEKS weeks). When the stream now
    reaches further back than `former_origin`, the days whose history crossed it are redone for every series."""
    days: list[date] = []
    if through is not None and (done is None or through > done):
        first = origin if done is None else done + timedelta(days=1)
        days = [first + timedelta(days=n) for n in range((through - first).days + 1)]
    if done is None:
        return days, set()
    if former_origin is not None and origin < former_origin:
        crossed = (former_origin + timedelta(days=n) for n in range(7 * WEEKS))
        days = [day for day in crossed if day <= done] + days
    redo = {(kind, district, day + timedelta(weeks=age)) for kind, district, day in changed if day <= done
            for age in range(WEEKS + 1) if day + timedelta(weeks=age) <= done}
    return days, redo


class Watermark:
    """Where each partition the consumer reads stands in source time, and how far each topic's replay pass has come
    (see the module docstring). A partition's days are forgotten when it is revoked, since another consumer may read it
    meanwhile; a topic's pass is not, so a rebalance neither ends a pass nor starts one again."""

    def __init__(self) -> None:
        self.assigned: set[Partition] = set()
        self.ended: set[Partition] = set()
        self.replayed: dict[Partition, date] = {}  # the newest day of the replayed events a partition has carried
        self.live: dict[Partition, date] = {}  # the newest day of its live events
        self.through: dict[str, date] = {}  # per topic, the last source day of the newest replay pass seen
        self.reached: dict[str, date] = {}  # per topic, the newest day of the replayed events it has carried

    def assign(self, partitions: Iterable[Partition]) -> None:
        self.assigned |= set(partitions)

    def revoke(self, partitions: Iterable[Partition]) -> None:
        for partition in partitions:
            self.assigned.discard(partition)
            self.ended.discard(partition)
            self.replayed.pop(partition, None)
            self.live.pop(partition, None)

    def seen(self, partition: Partition, day: date, *, replayed: bool, through: date | None = None) -> None:
        """An event of `day` on the partition; `through` is a replayed event's pass stamp."""
        newest = self.replayed if replayed else self.live
        newest[partition] = max(newest.get(partition, day), day)
        topic = partition[0]
        if replayed:
            self.reached[topic] = max(self.reached.get(topic, day), day)
            if through is not None:
                self.through[topic] = max(self.through.get(topic, through), through)
        self.assigned.add(partition)
        self.ended.discard(partition)

    def end(self, partition: Partition) -> None:
        self.assigned.add(partition)
        self.ended.add(partition)

    def replaying(self, topic: str) -> bool:
        """Whether a replay pass is under way on the topic: it has stamped a last day the topic has not carried yet."""
        through = self.through.get(topic)
        return through is not None and self.reached[topic] < through

    def place(self, partition: Partition) -> date | None:
        """A partition's place in source time: during a pass, or while it is still being read behind the pass's last
        day, its newest replayed day; otherwise the newer of its newest replayed and live days."""
        replayed, live, through = self.replayed.get(partition), self.live.get(partition), self.through.get(partition[0])
        behind = through is not None and (replayed is None or replayed < through)
        if self.replaying(partition[0]) or (behind and partition not in self.ended):
            return replayed
        return max((day for day in (replayed, live) if day is not None), default=None)

    def complete_through(self, topic: str, *, wait_for_replay: bool = False) -> date | None:
        """The last complete source day of a topic; None until every partition with events to read has placed itself,
        and with `wait_for_replay` until the topic has carried a replayed event."""
        if wait_for_replay and topic not in self.reached:
            return None
        places = {p: self.place(p) for p in self.assigned if p[0] == topic}
        known = [place for place in places.values() if place is not None]
        waiting = [place for p, place in places.items() if p not in self.ended]
        if not known or None in waiting:
            return None
        return min(waiting, default=max(known)) - timedelta(days=1)


class Analytics:
    """The consumer's handler, with what it keeps between batches: the watermark, and per topic the last day it has
    forecast and the first day the stream covers."""

    def __init__(self, conn: psycopg.Connection) -> None:
        self.conn = conn
        self.watermark = Watermark()
        self.done: dict[str, date | None] = {}
        self.origin: dict[str, date | None] = {}
        self.totals: Counter[str] = Counter()

    def handle(self, batch: Batch) -> None:
        items: list[Item] = []
        for record in batch.records:
            try:
                event = decode(record)
            except ValueError as problem:
                self.totals["invalid"] += 1
                log.warning("invalid message at %s[%d]@%d skipped: %s", record.topic, record.partition, record.offset,
                            problem)
                continue
            items.append(item(event))
            self.watermark.seen((record.topic, record.partition), items[-1].day, replayed=event.mode == "replay",
                                through=pass_end(event))
        for partition in batch.ended:
            self.watermark.end(partition)
        with self.conn.transaction():
            self.conn.execute("SELECT pg_advisory_xact_lock(%s)", (LOCK,))
            changed = self.count(items)
            for topic, kinds in TOPIC_KINDS.items():
                self.refresh(topic, kinds, [key for key in changed if key[0] in kinds])

    def count(self, items: list[Item]) -> list[SeriesDay]:
        """Count the events not counted before; returns the series-days whose counts changed."""
        unique = list({event.event_id: event for event in reversed(items)}.values())
        if not unique:
            return []
        rows = self.conn.execute(COUNT, [list(column) for column in zip(*unique)]).fetchall()
        added = sum(events for *_, events in rows)
        self.totals["counted"] += added
        self.totals["repeated"] += len(items) - added
        return [(kind, district, day) for kind, district, day, _ in rows]

    def refresh(self, topic: str, kinds: list[str], changed: list[SeriesDay]) -> None:
        """Forecast the days that became complete and redo those that depend on a late count."""
        origin = self.conn.execute(
            "SELECT min(day) FROM stream.daily_counts WHERE kind = ANY(%s)", (kinds,)).fetchone()[0]
        if origin is None:
            return
        if topic not in self.done:
            self.done[topic] = self.conn.execute(
                "SELECT max(day) FROM stream.forecasts WHERE kind = ANY(%s)", (kinds,)).fetchone()[0]
        done = self.done[topic]
        # The stack always starts the replay, and an empty store waits for it (see "Completed days").
        through = self.watermark.complete_through(topic, wait_for_replay=done is None and topic == TOPIC_ENFORCEMENT)
        days, redo = plan(done, through, origin, self.origin.get(topic), changed)
        self.origin[topic] = origin
        if not days and not redo:
            return
        wanted = set(days) | {day for *_, day in redo}
        counts: dict[tuple[str, str], dict[date, int]] = {}
        for kind, district, day, events in self.conn.execute(
            "SELECT kind, district, day, events FROM stream.daily_counts"
            " WHERE kind = ANY(%s) AND day BETWEEN %s AND %s",
            (kinds, min(wanted) - timedelta(weeks=WEEKS), max(wanted)),
        ):
            counts.setdefault((kind, district), {})[day] = events
        targets = redo | {(kind, district, day) for kind, district in counts for day in days}
        if rows := forecast_rows(counts, targets, origin):
            self.conn.execute(FORECAST, [list(column) for column in zip(*rows)])
        self.done[topic] = max(day for day in (done, through) if day is not None)
        self.totals["forecasts"] += len(rows)
        self.totals["anomalies"] += sum(row[6] for row in rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Count events per day and forecast each completed day")
    parser.add_argument("--batch", type=int, default=500, help="most messages per transaction")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    with psycopg.connect(postgres_url(), autocommit=True) as conn:
        analytics, reported = Analytics(conn), time.monotonic()

        def handle(batch: Batch) -> None:
            nonlocal reported
            analytics.handle(batch)
            if time.monotonic() - reported >= 30:
                reported = time.monotonic()
                log.info("%s; complete through %s", dict(analytics.totals),
                         {topic: str(day) for topic, day in analytics.done.items()})

        run(make_consumer(GROUP), handle, stop=stop_on_signals(), batch_size=args.batch,
            on_assign=lambda parts: analytics.watermark.assign((p.topic, p.partition) for p in parts),
            on_revoke=lambda parts: analytics.watermark.revoke((p.topic, p.partition) for p in parts))
    print(json.dumps(dict(analytics.totals), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
