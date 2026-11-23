"""`python -m safestart_pipeline.stream.storage`: the storage consumer (group `safestart-storage`) keeps every valid
event in `stream.events`, one row per event.

Each Kafka batch is validated against the event contract and inserted in one transaction, keyed by `event_id`: an
event sent again (a replay loop, a repeated live check) or a batch redelivered after a crash adds nothing, and the row
keeps the topic, partition and offset where the event was first stored. A message that is not a valid event, or sits
on the wrong topic for its kind, is counted and logged with its position and skipped; its offset is committed with the
batch, so it is not read again forever. Offsets are committed only after the database commit (see `kafka.run`).

A lost database connection ends the process; whatever restarts it (Compose, Prefect) resumes from the last committed
offsets.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import asdict, dataclass

import psycopg
from psycopg.types.json import Jsonb

from ..config import postgres_url
from .kafka import Batch, Record, decode, make_consumer, run, stop_on_signals

GROUP = "safestart-storage"
COLUMNS = ("event_id", "schema_version", "kind", "mode", "occurred_at", "emitted_at", "mine_id", "district", "state",
           "coal_metal", "title", "details", "source", "kafka_topic", "kafka_partition", "kafka_offset")
INSERT = (f"INSERT INTO stream.events ({', '.join(COLUMNS)}) VALUES ({', '.join(['%s'] * len(COLUMNS))})"
          " ON CONFLICT (event_id) DO NOTHING")
log = logging.getLogger(__name__)


@dataclass
class Totals:
    received: int = 0
    stored: int = 0
    already_stored: int = 0
    invalid: int = 0


def rows(records: list[Record], totals: Totals) -> list[tuple]:
    """The insert rows of a batch's valid events; invalid messages are counted and logged."""
    result = []
    for record in records:
        try:
            event = decode(record)
        except ValueError as problem:
            totals.invalid += 1
            log.warning("invalid message at %s[%d]@%d skipped: %s", record.topic, record.partition, record.offset,
                        problem)
            continue
        result.append((event.event_id, event.schema_version, event.kind, event.mode, event.occurred_at,
                       event.emitted_at, event.mine_id, event.district, event.state, event.coal_metal, event.title,
                       Jsonb(event.details), Jsonb(event.source), record.topic, record.partition, record.offset))
    return result


def store(conn: psycopg.Connection, records: list[Record], totals: Totals) -> None:
    """Insert a batch's valid events in one transaction; events already stored are left as they are."""
    totals.received += len(records)
    batch = rows(records, totals)
    if not batch:
        return
    with conn.transaction(), conn.cursor() as cursor:
        cursor.executemany(INSERT, batch)
        stored = cursor.rowcount
    totals.stored += stored
    totals.already_stored += len(batch) - stored


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Store every event from Kafka in PostgreSQL (stream.events)")
    parser.add_argument("--batch", type=int, default=500, help="most messages per transaction")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    totals = Totals()
    with psycopg.connect(postgres_url(), autocommit=True) as conn:
        reported, received = time.monotonic(), 0

        def handle(batch: Batch) -> None:
            nonlocal reported, received
            store(conn, batch.records, totals)
            if (now := time.monotonic()) - reported >= 30:
                log.info("%s, %.0f messages/s", asdict(totals), (totals.received - received) / (now - reported))
                reported, received = now, totals.received

        run(make_consumer(GROUP), handle, stop=stop_on_signals(), batch_size=args.batch)
    print(json.dumps(asdict(totals), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
