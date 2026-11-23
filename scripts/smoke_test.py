#!/usr/bin/env python3
"""End-to-end check of a running stack: contract events sent to Kafka must reach PostgreSQL through both consumers.

It sends four synthetic events of one mine (an inspection, an S&S and an ordinary violation, a lost-time accident) in a
district named after the run, so they cannot be mistaken for real records. It then waits until the storage consumer
has stored each one in stream.events and the analytics consumer has counted each one exactly once in
stream.daily_counts, and checks the dashboard's health endpoint. Afterwards it deletes its rows from PostgreSQL
(`--keep` leaves them); the events themselves stay in the Kafka log until retention removes them.

On the host, after `python scripts/start.py`, it reads the PostgreSQL password and the host ports from .env. Inside a
service container the SAFESTART_KAFKA_BOOTSTRAP, SAFESTART_POSTGRES_URL and SAFESTART_DASHBOARD_URL variables win:
`docker compose exec analytics-consumer python /app/scripts/smoke_test.py`.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import urllib.request
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from datetime import time as clock_time
from pathlib import Path

import psycopg
from confluent_kafka import Producer

from safestart_pipeline.stream.events import Event

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Endpoints:
    kafka: str
    postgres: str
    dashboard: str


def read_env(path: Path) -> dict[str, str]:
    """KEY=VALUE settings of a Compose .env file (empty when there is none)."""
    values = {}
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            key, separator, value = line.partition("=")
            if separator and key.strip() and not key.lstrip().startswith("#"):
                value = value.strip()
                quoted = len(value) >= 2 and value[0] == value[-1] and value[0] in "'\""
                values[key.strip()] = value[1:-1] if quoted else value
    return values


def endpoints(environ: Mapping[str, str], env: Mapping[str, str]) -> Endpoints:
    """Where the stack listens: SAFESTART_* variables (set inside the containers), else the host ports in .env. The
    host ports listen on IPv4 only, so they are addressed as 127.0.0.1 ("localhost" may try ::1 first)."""
    password = env.get("POSTGRES_PASSWORD")
    postgres = environ.get("SAFESTART_POSTGRES_URL") or (
        password and f"postgresql://safestart:{password}@127.0.0.1:{env.get('POSTGRES_HOST_PORT') or 15432}/safestart")
    if not postgres:
        raise SystemExit("No PostgreSQL address: run `python scripts/start.py` first, or set SAFESTART_POSTGRES_URL.")
    return Endpoints(
        kafka=environ.get("SAFESTART_KAFKA_BOOTSTRAP") or f"127.0.0.1:{env.get('KAFKA_HOST_PORT') or 19092}",
        postgres=postgres,
        dashboard=(environ.get("SAFESTART_DASHBOARD_URL")
                   or f"http://127.0.0.1:{env.get('DASHBOARD_HOST_PORT') or 8501}").rstrip("/"),
    )


def smoke_events(run: str, day: date) -> list[Event]:
    """The run's events, at noon UTC so the source day is the same calendar day in every U.S. time zone."""
    envelope = {"mode": "live", "occurred_at": datetime.combine(day, clock_time(12), tzinfo=UTC),
                "mine_id": f"smoke-{run}", "district": f"SMOKE-{run}", "state": "WV", "coal_metal": "C",
                "source": {"producer": "scripts/smoke_test.py", "run": run}}
    inspection = f"smoke-{run}-1"
    violation = {"event_no": inspection, "likelihood": "Reasonably Likely", "negligence": "Moderate",
                 "injury_illness": "Lost Workdays or Restricted Duty"}
    return [
        Event.new(kind="inspection", key=inspection, title=f"Smoke test {run}: regular inspection",
                  details={"activity_code": "E01", "activity": "Regular Safety and Health Inspection",
                           "begin_date": day.isoformat(), "end_date": day.isoformat()}, **envelope),
        Event.new(kind="violation", key=f"smoke-{run}-2", title=f"Smoke test {run}: S&S violation of 30 CFR 75.400",
                  details={**violation, "cfr_section": "75.400", "part_section": "75.400", "sig_sub": True,
                           "proposed_penalty": 1200.0}, **envelope),
        Event.new(kind="violation", key=f"smoke-{run}-3", title=f"Smoke test {run}: violation of 30 CFR 75.1722",
                  details={**violation, "cfr_section": "75.1722", "part_section": "75.1722(a)", "sig_sub": False,
                           "likelihood": "Unlikely", "proposed_penalty": 250.0}, **envelope),
        Event.new(kind="accident", key=f"smoke-{run}-4", title=f"Smoke test {run}: lost-time accident",
                  details={"degree_injury_cd": "03", "degree_injury": "DAYS AWAY FROM WORK ONLY", "serious": True,
                           "classification": "MACHINERY", "accident_type": "CAUGHT IN, UNDER OR BETWEEN",
                           "no_injuries": 1, "days_lost": 4}, **envelope),
    ]


def is_serious(event: Event) -> bool:
    """The contract's rule: an S&S violation, or an accident that was fatal or cost lost or restricted work days."""
    if event.kind == "violation":
        return event.details.get("sig_sub") is True
    return event.kind == "accident" and event.details.get("serious") is True


def expected_counts(events: Iterable[Event]) -> dict[str, tuple[int, int]]:
    """(events, serious) per kind, as stream.daily_counts should hold them for the run's district."""
    counts: dict[str, tuple[int, int]] = {}
    for event in events:
        total, serious = counts.get(event.kind, (0, 0))
        counts[event.kind] = (total + 1, serious + is_serious(event))
    return counts


def send(events: list[Event], bootstrap: str, timeout: float = 30.0) -> None:
    """Produce the events to their topics, keyed as the contract says, and wait for every acknowledgement."""
    producer = Producer({"bootstrap.servers": bootstrap, "client.id": "safestart-smoke-test", "acks": "all",
                         "enable.idempotence": True, "message.timeout.ms": int(timeout * 1000)})
    failures: list[str] = []

    def delivered(error, message) -> None:
        if error is not None:
            failures.append(f"{message.topic()}: {error}")

    for event in events:
        producer.produce(event.topic, key=event.key, value=event.to_json(), on_delivery=delivered)
    if (waiting := producer.flush(timeout + 5)) or failures:
        raise RuntimeError(f"Kafka at {bootstrap} did not take every event ({waiting} unsent): {'; '.join(failures)}")


def stored(connection: psycopg.Connection, events: list[Event]) -> set[str]:
    rows = connection.execute("SELECT event_id FROM stream.events WHERE event_id = ANY(%s)",
                              ([event.event_id for event in events],)).fetchall()
    return {row[0] for row in rows}


def counted(connection: psycopg.Connection, district: str) -> dict[str, tuple[int, int]]:
    rows = connection.execute("SELECT kind, sum(events)::int, sum(serious)::int FROM stream.daily_counts "
                              "WHERE district = %s GROUP BY kind", (district,)).fetchall()
    return {kind: (total, serious) for kind, total, serious in rows}


def remove(connection: psycopg.Connection, events: list[Event]) -> None:
    """Delete the run's rows: its events, and the counts and forecasts of its district."""
    district, ids = events[0].district, [event.event_id for event in events]
    with connection.transaction():
        connection.execute("DELETE FROM stream.events WHERE event_id = ANY(%s)", (ids,))
        connection.execute("DELETE FROM stream.daily_counts WHERE district = %s", (district,))
        connection.execute("DELETE FROM stream.forecasts WHERE district = %s", (district,))


def wait_for(check: Callable[[], bool], timeout: float, interval: float = 1.0, *,
             clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep) -> bool:
    """Poll `check` until it holds (True) or the time is up (False); it is always asked at least once."""
    deadline = clock() + timeout
    while not check():
        if clock() >= deadline:
            return False
        sleep(interval)
    return True


def verify(connection: psycopg.Connection, events: list[Event], timeout: float) -> list[str]:
    """Wait for both consumers; the problems left when the time is up (none once both wrote every event)."""
    district = events[0].district or ""
    wanted = ({event.event_id for event in events}, expected_counts(events))
    found: tuple[set[str], dict[str, tuple[int, int]]] = (set(), {})

    def arrived() -> bool:
        nonlocal found
        found = (stored(connection, events), counted(connection, district))
        return found == wanted

    if wait_for(arrived, timeout):
        return []
    problems = []
    if missing := sorted(wanted[0] - found[0]):
        problems.append(f"storage consumer: {', '.join(missing)} not in stream.events after {timeout:.0f} s")
    if found[1] != wanted[1]:
        problems.append(f"analytics consumer: stream.daily_counts holds (events, serious) {found[1]} for district "
                        f"{district}, expected {wanted[1]}")
    return problems


def dashboard_healthy(url: str) -> bool:
    try:
        with urllib.request.urlopen(f"{url}/_stcore/health", timeout=5) as response:
            return response.status == 200
    except OSError:
        return False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Send contract events through the running stack and check that both "
                                                 "consumers write them to PostgreSQL.")
    parser.add_argument("--timeout", type=float, default=120, help="seconds to wait for the consumers")
    parser.add_argument("--keep", action="store_true", help="leave the run's rows in PostgreSQL")
    parser.add_argument("--skip-dashboard", action="store_true", help="do not check the dashboard")
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env", help="the Compose settings (default: .env)")
    args = parser.parse_args(argv)
    where = endpoints(os.environ, read_env(args.env_file))
    run = uuid.uuid4().hex[:8]
    events = smoke_events(run, datetime.now(UTC).date())
    send(events, where.kafka)
    print(f"Sent {len(events)} events of run {run} to Kafka at {where.kafka}.")
    with psycopg.connect(where.postgres, autocommit=True, connect_timeout=10) as connection:
        problems = verify(connection, events, args.timeout)
        if not args.keep:
            remove(connection, events)
    if not args.skip_dashboard and not wait_for(lambda: dashboard_healthy(where.dashboard), min(args.timeout, 60), 3):
        problems.append(f"dashboard: {where.dashboard}/_stcore/health does not answer 200")
    for problem in problems:
        print(f"FAILED {problem}", file=sys.stderr)
    if problems:
        return 1
    dashboard = "" if args.skip_dashboard else "; the dashboard is up"
    cleanup = "" if args.keep else " (the run's rows are removed again)"
    print(f"Passed: both consumers wrote every event to PostgreSQL{dashboard}{cleanup}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
