"""`python -m safestart_pipeline.stream.init`: wait until Kafka and PostgreSQL answer, create both topics, and apply
`schema.sql`.

Every step is idempotent (the topics are created only when missing, every table and index is `IF NOT EXISTS`, and the
statuses a source check may have are set again), so the stack can start in any order and init can run before each
start; a store made before a status was added takes it at the next start.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from collections.abc import Callable
from pathlib import Path

import psycopg
from confluent_kafka.admin import AdminClient

from ..config import postgres_url
from .kafka import client_config, ensure_topics

SCHEMA = Path(__file__).with_name("schema.sql")
log = logging.getLogger(__name__)


def wait_for(name: str, check: Callable[[], object], timeout: float, *, sleep: Callable[[float], None] = time.sleep,
             now: Callable[[], float] = time.monotonic) -> None:
    """Call `check` until it succeeds, waiting 1, 2, 4 ... (at most 10) seconds between tries, for up to `timeout`."""
    deadline, delay = now() + timeout, 1.0
    while True:
        try:
            check()
            log.info("%s answers", name)
            return
        except Exception as problem:  # any failure means "not up yet"
            if now() + delay > deadline:
                raise RuntimeError(f"{name} did not answer within {timeout:.0f} s: {problem}") from problem
            log.info("waiting for %s: %s", name, problem)
            sleep(delay)
            delay = min(delay * 2, 10.0)


def apply_schema(conn: psycopg.Connection) -> None:
    conn.execute(SCHEMA.read_text(encoding="utf-8"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Create the Kafka topics and the PostgreSQL stream schema")
    parser.add_argument("--timeout", type=float, default=120.0, help="seconds to wait for each service")
    parser.add_argument("--partitions", type=int, help="partitions per new topic (default: kafka_partitions, 3)")
    parser.add_argument("--replication", type=int, default=1, help="replicas per new topic (one broker: 1)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    admin = AdminClient(client_config())
    wait_for("kafka", lambda: admin.list_topics(timeout=5), args.timeout)
    wait_for("postgresql", lambda: psycopg.connect(postgres_url(), connect_timeout=5).close(), args.timeout)
    topics = ensure_topics(admin, partitions=args.partitions, replication=args.replication)
    with psycopg.connect(postgres_url(), autocommit=True) as conn:
        apply_schema(conn)
    print(json.dumps({"topics": topics, "schema": "applied"}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
