"""PostgreSQL tests of the consumers. They run when TEST_DATABASE_URL names a database (any; nothing persists): each
test works inside one transaction, on a fresh `stream` schema, and rolls everything back at the end."""

from __future__ import annotations

import os
from collections import Counter
from datetime import UTC, date, datetime, timedelta

import pytest

from safestart_pipeline.stream.analytics import Analytics, forecast_rows
from safestart_pipeline.stream.events import TOPIC_ENFORCEMENT, TOPIC_SOURCES, Event
from safestart_pipeline.stream.init import apply_schema
from safestart_pipeline.stream.kafka import Batch, Record
from safestart_pipeline.stream.storage import Totals, store

psycopg = pytest.importorskip("psycopg")
START = date(2024, 1, 1)


@pytest.fixture
def conn():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("set TEST_DATABASE_URL to run the PostgreSQL tests")
    with psycopg.connect(url) as connection:
        connection.execute("DROP SCHEMA IF EXISTS stream CASCADE")
        apply_schema(connection)
        apply_schema(connection)  # idempotent
        yield connection
        connection.rollback()


def violation(key, day, district="C04", sig_sub=True, mode="replay"):
    return Event.new(kind="violation", key=key, mode=mode, occurred_at=day, title=f"Violation {key}",
                     details={"cfr_section": "75.400", "sig_sub": sig_sub}, mine_id=f"mine-{district}",
                     district=district, state="WV", coal_metal="C")


def test_storage_keeps_each_event_once_with_where_it_first_arrived(conn):
    first, second = violation("1", START), violation("2", START, district="M3")
    records = [Record(TOPIC_ENFORCEMENT, 0, 10, first.to_json()), Record(TOPIC_ENFORCEMENT, 1, 11, second.to_json()),
               Record(TOPIC_ENFORCEMENT, 0, 12, first.to_json()),  # sent twice
               Record(TOPIC_ENFORCEMENT, 2, 13, b"{not json"), Record(TOPIC_SOURCES, 0, 14, first.to_json())]
    totals = Totals()
    store(conn, records, totals)
    assert totals == Totals(received=5, stored=2, already_stored=1, invalid=2)
    store(conn, records[:2], totals)  # the batch redelivered after a crash before the offset commit
    assert (totals.stored, totals.already_stored) == (2, 3)
    rows = conn.execute("SELECT event_id, kafka_partition, kafka_offset, details->>'cfr_section', occurred_at,"
                        " source FROM stream.events ORDER BY event_id").fetchall()
    assert [row[:4] for row in rows] == [("violation:1", 0, 10, "75.400"), ("violation:2", 1, 11, "75.400")]
    assert rows[0][4] == datetime(2024, 1, 1, tzinfo=UTC) and rows[0][5] == {}


def history(days=120):
    """A replay of `days` days: violations in two districts on weekdays (more on Tuesdays), a few on weekends."""
    events = []
    for n in range(days):
        day = START + timedelta(days=n)
        for district, base in (("C04", 6), ("M3", 3)):
            count = base + (4 if day.weekday() == 1 else 0) if day.weekday() < 5 else n % 2
            count += 25 if (district, n) == ("C04", 71) else 0  # one unusual day
            events += [violation(f"{district}-{n}-{i}", day, district, sig_sub=i % 3 == 0) for i in range(count)]
    return events


def batches(events, offset=0, size=50):
    """The events as the analytics consumer reads them: two partitions by district, in batches, then both ended."""
    records = [Record(TOPIC_ENFORCEMENT, 0 if event.district == "C04" else 1, offset + n, event.to_json())
               for n, event in enumerate(events)]
    for start in range(0, len(records), size):
        yield Batch(records[start:start + size], set())
    yield Batch([], {(TOPIC_ENFORCEMENT, 0), (TOPIC_ENFORCEMENT, 1), (TOPIC_SOURCES, 0)})


def consumer(conn):
    analytics = Analytics(conn)
    analytics.watermark.assign([(TOPIC_ENFORCEMENT, 0), (TOPIC_ENFORCEMENT, 1), (TOPIC_SOURCES, 0)])
    return analytics


def stored_forecasts(conn):
    return conn.execute("SELECT day, kind, district, observed, predicted, residual, anomaly, method"
                        " FROM stream.forecasts ORDER BY kind, district, day").fetchall()


def recomputed_forecasts(conn, through):
    """Every forecast computed from scratch from daily_counts: what the table must always hold."""
    counts = {}
    for kind, district, day, events in conn.execute("SELECT kind, district, day, events FROM stream.daily_counts"):
        counts.setdefault((kind, district), {})[day] = events
    origin = min(day for series in counts.values() for day in series)
    days = [origin + timedelta(days=n) for n in range((through - origin).days + 1)]
    rows = forecast_rows(counts, [(kind, district, day) for kind, district in counts for day in days], origin)
    return sorted(rows, key=lambda row: (row[1], row[2], row[0]))


def assert_forecasts_match(conn, through):
    stored, expected = stored_forecasts(conn), recomputed_forecasts(conn, through)
    assert [row[:4] + row[6:] for row in stored] == [row[:4] + row[6:] for row in expected]
    assert [value for row in stored for value in row[4:6]] == pytest.approx(
        [value for row in expected for value in row[4:6]])


def test_counts_move_once_per_event_across_replays_and_restarts(conn):
    events = history()
    analytics = consumer(conn)
    for batch in batches(events):
        analytics.handle(batch)
    expected = Counter((event.occurred_at[:10], event.district) for event in events)
    serious = Counter((event.occurred_at[:10], event.district) for event in events if event.details["sig_sub"])
    rows = conn.execute("SELECT day, district, events, serious FROM stream.daily_counts").fetchall()
    assert {(str(day), district): (n, s) for day, district, n, s in rows} == {
        key: (n, serious[key]) for key, n in expected.items()}
    assert analytics.totals["counted"] == len(events)
    last = START + timedelta(days=119)
    assert analytics.done == {TOPIC_ENFORCEMENT: last - timedelta(days=1)}  # no source events: nothing to forecast
    assert_forecasts_match(conn, last - timedelta(days=1))
    anomalies = conn.execute("SELECT day, district FROM stream.forecasts WHERE anomaly").fetchall()
    assert (START + timedelta(days=71), "C04") in anomalies

    restarted = consumer(conn)  # a restart, then the whole replay again (a loop) at new offsets
    for batch in batches(events, offset=len(events)):
        restarted.handle(batch)
    again = conn.execute("SELECT day, district, events, serious FROM stream.daily_counts").fetchall()
    assert sorted(again) == sorted(rows) and restarted.totals["counted"] == 0
    assert restarted.totals["repeated"] == len(events)
    assert conn.execute("SELECT count(*) FROM stream.counted_events").fetchone()[0] == len(events)
    assert_forecasts_match(conn, last - timedelta(days=1))


def test_late_and_earlier_events_recompute_the_forecasts_that_depend_on_them(conn):
    analytics = consumer(conn)
    for batch in batches(history()):
        analytics.handle(batch)
    through = analytics.done[TOPIC_ENFORCEMENT]
    late = [violation(f"late-{i}", START + timedelta(days=40), mode="live") for i in range(30)]
    earlier = [violation(f"early-{i}", START - timedelta(days=n), "M3") for n in range(1, 30) for i in range(2)]
    for batch in batches(late + earlier, offset=10_000):
        analytics.handle(batch)
    assert analytics.done[TOPIC_ENFORCEMENT] == through
    assert_forecasts_match(conn, through)  # the stream now starts 29 days earlier, and day 40 holds 30 more


def test_a_source_day_is_not_forecast_before_every_partition_has_moved_past_it(conn):
    analytics = consumer(conn)
    events = history(days=10)
    on_zero = [Record(TOPIC_ENFORCEMENT, 0, n, e.to_json()) for n, e in enumerate(events) if e.district == "C04"]
    analytics.handle(Batch(on_zero, set()))
    assert analytics.done.get(TOPIC_ENFORCEMENT) is None
    assert conn.execute("SELECT count(*) FROM stream.forecasts").fetchone()[0] == 0
    analytics.handle(Batch([], {(TOPIC_ENFORCEMENT, 1)}))  # partition 1 is empty
    assert analytics.done[TOPIC_ENFORCEMENT] == START + timedelta(days=8)
