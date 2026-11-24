from __future__ import annotations

import re
import sys
import types
from datetime import UTC, datetime, time, timedelta
from pathlib import Path

import psycopg
import pytest

from safestart_pipeline import stream
from safestart_pipeline.stream.events import Event
from safestart_pipeline.watch import __main__ as watch
from safestart_pipeline.watch import sitemaps
from safestart_pipeline.watch.checks import INSERT, STATUSES, Check, record_check
from safestart_pipeline.watch.publish import (
    KAFKA_MODULE,
    ListPublisher,
    kafka_publisher,
    publish_new,
)
from safestart_pipeline.watch.state import WatchState
from test_watch_versions import COAL_MINE, MINES, NEW, OLD, add_version, violation

NOW = datetime(2026, 11, 23, 18, 0, tzinfo=UTC)


def regulation(number: str) -> Event:
    return Event.new(kind="regulation", key=number, mode="live", occurred_at="2026-11-18", title=f"MSHA: {number}")


@pytest.fixture
def state(tmp_path):
    state = WatchState(tmp_path / "state" / "watch.sqlite")
    yield state
    state.close()


# Publishing -----------------------------------------------------------------------------------------------------------


def test_an_event_is_published_once_however_often_it_is_found(tmp_path):
    state, publisher = WatchState(tmp_path / "watch.sqlite"), ListPublisher()
    assert publish_new([regulation("2026-1"), regulation("2026-2"), regulation("2026-1")], publisher, state) == 2
    assert [event.event_id for event in publisher.events] == ["regulation:2026-1", "regulation:2026-2"]
    assert publisher.flushes == 1
    assert publish_new([regulation("2026-2")], publisher, state) == 0 and publisher.flushes == 1  # nothing to flush
    state.close()
    reopened = WatchState(tmp_path / "watch.sqlite")  # the ids outlive the process
    assert publish_new([regulation("2026-1"), regulation("2026-3")], publisher, reopened) == 1
    assert reopened.any_emitted("regulation") and not reopened.any_emitted("violation")
    reopened.close()


def test_the_kafka_publisher_comes_from_the_stream_layer_when_it_exists(monkeypatch):
    made = ListPublisher()
    monkeypatch.setitem(sys.modules, KAFKA_MODULE, types.SimpleNamespace(make_publisher=lambda: made))
    assert kafka_publisher() is made
    monkeypatch.setitem(sys.modules, KAFKA_MODULE, types.SimpleNamespace())
    with pytest.raises(RuntimeError, match="make_publisher"):
        kafka_publisher()
    monkeypatch.setitem(sys.modules, KAFKA_MODULE, None)  # the module is absent
    with pytest.raises(RuntimeError, match="is not available"):
        kafka_publisher()


# Recording checks -----------------------------------------------------------------------------------------------------


class FakeConnection:
    def __init__(self, calls: list):
        self.calls = calls

    def __enter__(self):
        return self

    def __exit__(self, *problem):
        return False

    def execute(self, sql, params):
        self.calls.append((sql, params))


def test_a_check_is_stored_when_postgresql_answers_and_logged_when_it_does_not(caplog):
    calls, options = [], []

    def connect(url, **kwargs):
        options.append((url, kwargs))
        return FakeConnection(calls)

    check = Check("federal-register", "ok", 2, "2 new", NOW)
    assert record_check(check, url="postgresql://example/db", connect=connect)
    assert calls == [(INSERT, ("federal-register", NOW, "ok", 2, "2 new"))]
    assert options == [("postgresql://example/db", {"connect_timeout": 5})]

    def refuse(url, **kwargs):
        raise psycopg.OperationalError("connection refused")

    caplog.set_level("INFO", logger="watch")
    assert not record_check(check, url="postgresql://example/db", connect=refuse)
    assert "federal-register: ok, 2 new; 2 new" in caplog.text and "connection refused" in caplog.text


def test_a_check_has_one_of_the_stored_statuses():
    assert Check("sitemaps-msha", "refused").status == "refused"
    with pytest.raises(ValueError, match="status"):
        Check("versions", "fine")
    with pytest.raises(ValueError, match="negative"):
        Check("versions", "ok", -1)


def test_the_statuses_are_the_ones_stream_source_checks_allows():
    """The schema sets its list again each time it is applied, so a store made with an older list takes this one."""
    schema = Path(stream.__file__).with_name("schema.sql").read_text(encoding="utf-8")
    allowed = re.search(r"CONSTRAINT source_checks_status_check CHECK \(status IN \(([^)]*)\)\)", schema)
    assert allowed and {value.strip(" '") for value in allowed[1].split(",")} == STATUSES


# Scheduling -----------------------------------------------------------------------------------------------------------


def test_checks_run_on_their_schedules_and_one_failure_does_not_stop_the_others(state):
    recorded, ran = [], []

    def run(name: str) -> Check:
        ran.append(name)
        if name == "sitemaps-msha":
            raise RuntimeError("sitemap host down")
        return Check(name, "unchanged", checked_at=NOW + timedelta(minutes=5))

    names = list(watch.SCHEDULE)
    assert watch.due(state, names, NOW) == names  # never checked: all due
    checks = watch.run_checks(names, state, run, now=lambda: NOW, record=recorded.append)
    assert ran == names and [check.status for check in checks] == ["unchanged", "error", "unchanged", "unchanged"]
    assert "RuntimeError: sitemap host down" in checks[1].note
    assert all(check.checked_at == NOW for check in checks) and recorded == checks  # stamped with the start
    assert watch.due(state, names, NOW + timedelta(hours=1)) == ["federal-register"]
    assert watch.due(state, names, NOW + timedelta(hours=6)) == ["versions", "sitemaps-msha", "federal-register"]
    assert watch.seconds_to_next(state, names, NOW + timedelta(minutes=30)) == 1800.0


def test_a_check_skipped_while_its_crawl_runs_is_due_again_soon_and_one_that_ran_goes_back_to_its_interval(state):
    def busy(name: str) -> Check:
        return Check(name, "skipped", note="the osha crawl is running", retry_in=timedelta(hours=1))

    watch.run_checks(["sitemaps-osha"], state, busy, now=lambda: NOW, record=lambda check: True)
    assert watch.due(state, ["sitemaps-osha"], NOW + timedelta(minutes=59)) == []
    assert watch.due(state, ["sitemaps-osha"], NOW + timedelta(hours=1)) == ["sitemaps-osha"]
    assert watch.seconds_to_next(state, ["sitemaps-osha"], NOW) == 3600.0
    later = NOW + timedelta(hours=1)
    watch.run_checks(["sitemaps-osha"], state, lambda name: Check(name, "unchanged"), now=lambda: later,
                     record=lambda check: True)
    assert watch.due(state, ["sitemaps-osha"], later + timedelta(hours=23)) == []
    assert watch.due(state, ["sitemaps-osha"], later + timedelta(days=1)) == ["sitemaps-osha"]


def test_the_osha_check_is_not_starved_by_the_daily_run_of_the_official_files(state):
    """The daily run writes OSHA's inventory from 16:30 to 04:30 UTC, and the watcher waits until it has been quiet
    for 15 minutes. A check that falls inside that window is asked again hourly, reads once the run has ended, and its
    next time falls outside the window, day after day."""
    clock, reads = [datetime(2026, 11, 24, 17, 0, tzinfo=UTC)], []  # the first check comes inside the window

    def check(name: str) -> Check:
        moment = clock[0]
        if moment.time() >= time(16, 30) or moment.time() < time(4, 45):
            return Check(name, "skipped", note="the osha crawl is running", retry_in=sitemaps.BUSY_RETRY)
        reads.append(moment)
        return Check(name, "unchanged")

    while clock[0] < datetime(2026, 11, 28, 17, 0, tzinfo=UTC):
        watch.run_checks(watch.due(state, ["sitemaps-osha"], clock[0]), state, check, now=lambda: clock[0],
                         record=lambda check: True)
        clock[0] += timedelta(seconds=max(watch.seconds_to_next(state, ["sitemaps-osha"], clock[0]), 1.0))
    assert [f"{read:%d %H:%M}" for read in reads] == ["25 05:00", "26 05:00", "27 05:00", "28 05:00"]


def test_the_log_goes_to_the_data_root_to_a_named_file_or_to_standard_error_alone(tmp_path, monkeypatch):
    monkeypatch.delenv("SAFESTART_WATCH_LOG", raising=False)
    assert watch.log_file(tmp_path) == tmp_path / "logs" / "watch.log"
    monkeypatch.setenv("SAFESTART_WATCH_LOG", str(tmp_path / "elsewhere" / "watch.log"))
    assert watch.log_file(tmp_path) == tmp_path / "elsewhere" / "watch.log"
    monkeypatch.setenv("SAFESTART_WATCH_LOG", "-")
    assert watch.log_file(tmp_path) is None


def test_once_runs_the_due_checks_and_reports_a_missing_kafka_publisher(tmp_path, monkeypatch):
    """With the containers' settings: the state in a volume of its own, the log on standard error alone."""
    add_version(tmp_path, "mines", MINES, OLD)
    add_version(tmp_path, "violations", [violation("1")], OLD)
    add_version(tmp_path, "violations", [violation("1"), violation("2", COAL_MINE)], NEW)
    monkeypatch.setenv("SAFESTART_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("SAFESTART_WATCH_STATE", str(tmp_path / "watch-state" / "watch.sqlite"))
    monkeypatch.setenv("SAFESTART_WATCH_LOG", "-")
    monkeypatch.setattr(watch, "setup_logging", lambda path: pytest.fail(f"no log file was asked for, got {path}"))
    publisher, attempts = ListPublisher(), []

    def unreachable(url, **kwargs):
        attempts.append(url)
        raise psycopg.OperationalError("connection refused")

    monkeypatch.setattr(psycopg, "connect", unreachable)
    monkeypatch.setattr(watch, "kafka_publisher", lambda: publisher)

    assert watch.main(["once", "--only", "versions"]) == 0
    assert [event.event_id for event in publisher.events] == ["violation:2", f"dataset_version:violations:{NEW}"]
    assert len(attempts) == 1  # the check was offered to PostgreSQL, and only logged when it could not be stored
    assert watch.main(["once", "--only", "versions"]) == 0 and len(publisher.events) == 2  # not due again yet
    assert watch.main(["once", "--only", "versions", "--force"]) == 0 and len(publisher.events) == 2  # nothing new
    assert (tmp_path / "watch-state" / "watch.sqlite").exists()
    assert not (tmp_path / "logs").exists() and not (tmp_path / "state").exists()  # nothing written to the data root

    def missing():
        raise RuntimeError("no Kafka publisher")

    monkeypatch.setattr(watch, "kafka_publisher", missing)
    assert watch.main(["once"]) == 2
