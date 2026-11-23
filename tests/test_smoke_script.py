from __future__ import annotations

import importlib.util
import json
import os
import sys
from datetime import date
from pathlib import Path

import psycopg
import pytest

from safestart_pipeline.stream.events import TOPIC_ENFORCEMENT, Event

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("scripts_smoke_test", ROOT / "scripts" / "smoke_test.py")
smoke = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = smoke
spec.loader.exec_module(smoke)

DAY = date(2026, 11, 23)


def test_the_smoke_events_follow_the_contract_and_stay_apart_from_real_records():
    events = smoke.smoke_events("ab12cd34", DAY)
    assert [event.kind for event in events] == ["inspection", "violation", "violation", "accident"]
    assert len({event.event_id for event in events}) == 4
    assert {event.district for event in events} == {"SMOKE-ab12cd34"}
    assert {event.occurred_at for event in events} == {"2026-11-23T12:00:00Z"}
    assert all(event.topic == TOPIC_ENFORCEMENT and event.key == b"smoke-ab12cd34" for event in events)
    assert all(Event.from_json(event.to_json()) == event for event in events)
    assert events[1].details["cfr_section"] == "75.400" and events[2].details["part_section"] == "75.1722(a)"


def test_expected_counts_apply_the_contracts_serious_rule():
    assert smoke.expected_counts(smoke.smoke_events("ab12cd34", DAY)) == {
        "inspection": (1, 0), "violation": (2, 1), "accident": (1, 1)}


def test_endpoints_come_from_the_container_environment_or_the_host_ports_in_env(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("# comment\nPOSTGRES_PASSWORD='pw'\nKAFKA_HOST_PORT=29092\nDASHBOARD_HOST_PORT=8600\n",
                        encoding="utf-8")
    host = smoke.endpoints({}, smoke.read_env(env_file))
    assert host == smoke.Endpoints("127.0.0.1:29092", "postgresql://safestart:pw@127.0.0.1:15432/safestart",
                                   "http://127.0.0.1:8600")
    inside = smoke.endpoints({"SAFESTART_KAFKA_BOOTSTRAP": "kafka:9092", "SAFESTART_POSTGRES_URL": "postgresql://p",
                              "SAFESTART_DASHBOARD_URL": "http://dashboard:8501/"}, {})
    assert inside == smoke.Endpoints("kafka:9092", "postgresql://p", "http://dashboard:8501")
    with pytest.raises(SystemExit, match="start.py"):
        smoke.endpoints({}, smoke.read_env(tmp_path / "missing.env"))


def test_wait_for_polls_until_the_check_holds_or_time_runs_out():
    now, answers = [0.0], iter([False, False, True])
    clock, sleep = (lambda: now[0]), (lambda seconds: now.__setitem__(0, now[0] + seconds))
    assert smoke.wait_for(lambda: next(answers), timeout=10, clock=clock, sleep=sleep) and now[0] == 2
    calls = []
    assert not smoke.wait_for(lambda: calls.append(1) or False, timeout=3, clock=clock, sleep=sleep)
    assert len(calls) == 4
    assert smoke.wait_for(lambda: True, timeout=0)  # asked at least once


@pytest.fixture
def stream_db():
    """The stream schema in TEST_DATABASE_URL, inside a transaction that is rolled back afterwards."""
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is not set")
    with psycopg.connect(url) as connection:
        connection.execute((ROOT / "src" / "safestart_pipeline" / "stream" / "schema.sql").read_text(encoding="utf-8"))
        yield connection
        connection.rollback()


def write_as_consumers(connection, events, *, store=None, count_twice=()):
    """Rows as the storage and analytics consumers write them."""
    for offset, event in enumerate(events):
        if store is None or event.event_id in store:
            connection.execute(
                "INSERT INTO stream.events (event_id, schema_version, kind, mode, occurred_at, emitted_at, mine_id, "
                "district, state, coal_metal, title, details, source, kafka_topic, kafka_partition, kafka_offset) "
                "VALUES (%s, 1, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, %s)",
                (event.event_id, event.kind, event.mode, event.occurred_at, event.emitted_at, event.mine_id,
                 event.district, event.state, event.coal_metal, event.title, json.dumps(event.details),
                 json.dumps(event.source), event.topic, offset))
        for _ in range(2 if event.kind in count_twice else 1):
            connection.execute(
                "INSERT INTO stream.daily_counts (day, kind, district, events, serious) VALUES (%s, %s, %s, 1, %s) "
                "ON CONFLICT (day, kind, district) DO UPDATE SET events = daily_counts.events + 1, "
                "serious = daily_counts.serious + EXCLUDED.serious",
                (DAY, event.kind, event.district, int(smoke.is_serious(event))))


def test_verify_passes_once_both_consumers_wrote_every_event_and_cleanup_removes_them(stream_db):
    events = smoke.smoke_events("ab12cd34", DAY)
    assert smoke.verify(stream_db, events, timeout=0)  # nothing written yet
    write_as_consumers(stream_db, events)
    assert smoke.verify(stream_db, events, timeout=0) == []
    smoke.remove(stream_db, events)
    assert smoke.stored(stream_db, events) == set() and smoke.counted(stream_db, "SMOKE-ab12cd34") == {}


def test_verify_names_the_consumer_that_fell_short(stream_db):
    events = smoke.smoke_events("ab12cd34", DAY)
    write_as_consumers(stream_db, events, store={events[0].event_id}, count_twice={"accident"})
    storage, analytics = smoke.verify(stream_db, events, timeout=0)
    assert storage.startswith("storage consumer:") and events[3].event_id in storage
    assert analytics.startswith("analytics consumer:") and "'accident': (2, 2)" in analytics
