"""Kafka clients for the streaming layer, on librdkafka (confluent-kafka).

Delivery is at least once from end to end, and the consumers make its effect exactly once. The producer is idempotent:
the broker drops a batch it already holds, so a network retry neither duplicates nor reorders a partition. Each
consumer commits its Kafka offsets only after the database transaction that holds the batch has committed; a crash
in between redelivers the batch, and the database absorbs it because every write is keyed by the event's `event_id`.

Connection: `config.kafka_bootstrap()`, plus SASL/SSL for a managed broker, read from the environment only so that no
secret lives in a file: SAFESTART_KAFKA_SECURITY_PROTOCOL (SASL_SSL by default once a user name is set),
SAFESTART_KAFKA_SASL_MECHANISM (PLAIN by default, or SCRAM-SHA-256 / SCRAM-SHA-512), SAFESTART_KAFKA_SASL_USERNAME,
SAFESTART_KAFKA_SASL_PASSWORD and SAFESTART_KAFKA_SSL_CA_LOCATION.
"""

from __future__ import annotations

import logging
import os
import signal
import threading
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewTopic,
    ResourceType,
)

from ..config import kafka_bootstrap, setting
from .events import TOPIC_ENFORCEMENT, TOPIC_SOURCES, Event

TOPICS = (TOPIC_ENFORCEMENT, TOPIC_SOURCES)
RETENTION_MS = 7 * 24 * 3600 * 1000  # a week; older history is replayed from the lake (idempotent by event id)
log = logging.getLogger(__name__)


def client_config(**overrides: Any) -> dict[str, Any]:
    """librdkafka settings shared by every client: the brokers and, when the environment names them, SASL/SSL."""
    conf: dict[str, Any] = {"bootstrap.servers": kafka_bootstrap()}
    env = os.environ.get
    username = env("SAFESTART_KAFKA_SASL_USERNAME")
    if protocol := env("SAFESTART_KAFKA_SECURITY_PROTOCOL") or ("SASL_SSL" if username else None):
        conf["security.protocol"] = protocol
    if username:
        if not (password := env("SAFESTART_KAFKA_SASL_PASSWORD")):
            raise RuntimeError("SAFESTART_KAFKA_SASL_USERNAME is set but SAFESTART_KAFKA_SASL_PASSWORD is not")
        conf |= {"sasl.mechanism": env("SAFESTART_KAFKA_SASL_MECHANISM") or "PLAIN", "sasl.username": username,
                 "sasl.password": password}
    if ca := env("SAFESTART_KAFKA_SSL_CA_LOCATION"):
        conf["ssl.ca.location"] = ca
    return conf | overrides


def ensure_topics(admin: AdminClient | None = None, *, partitions: int | None = None, replication: int = 1,
                  timeout: float = 30.0) -> dict[str, str]:
    """Create both topics if they are missing and give both a 7-day retention. The enforcement topic is keyed by mine,
    so its partition count bounds how many instances of one consumer group can share the work."""
    admin = admin or AdminClient(client_config())
    count = partitions or int(setting("kafka_partitions", "3"))
    existing = set(admin.list_topics(timeout=timeout).topics)
    retention = {"retention.ms": str(RETENTION_MS)}
    missing = [NewTopic(topic, num_partitions=count, replication_factor=replication, config=retention)
               for topic in TOPICS if topic not in existing]
    result: dict[str, str] = {}
    for topic, future in (admin.create_topics(missing, request_timeout=timeout).items() if missing else ()):
        try:
            future.result()
            result[topic] = "created"
        except KafkaException as problem:
            if problem.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
            existing.add(topic)  # another init created it meanwhile
    present = [topic for topic in TOPICS if topic in existing]
    if present:
        entry = ConfigEntry("retention.ms", str(RETENTION_MS), incremental_operation=AlterConfigOpType.SET)
        resources = [ConfigResource(ResourceType.TOPIC, topic, incremental_configs=[entry]) for topic in present]
        for resource, future in admin.incremental_alter_configs(resources, request_timeout=timeout).items():
            future.result()
            result[resource.name] = "retention set"
    return result


class Publisher:
    """Sends events to their topic, keyed so that one mine's events keep their order on one partition. `flush` waits
    until the broker has acknowledged everything sent and raises if any event was not delivered."""

    def __init__(self, producer: Producer) -> None:
        self._producer = producer
        self.sent = 0
        self.failures: list[str] = []

    def send(self, event: Event) -> None:
        while True:
            try:
                self._producer.produce(event.topic, event.to_json(), key=event.key, on_delivery=self._delivered)
                break
            except BufferError:  # the local queue is full: serve delivery reports until there is room
                self._producer.poll(0.5)
        self.sent += 1
        self._producer.poll(0)
        if self.failures:
            raise RuntimeError(f"{len(self.failures)} event(s) were not delivered, the first: {self.failures[0]}")

    def flush(self, timeout: float = 60.0) -> None:
        if (waiting := self._producer.flush(timeout)) > 0:
            raise RuntimeError(f"{waiting} event(s) still unacknowledged after {timeout:.0f} s")
        if self.failures:
            raise RuntimeError(f"{len(self.failures)} event(s) were not delivered, the first: {self.failures[0]}")

    def _delivered(self, error: KafkaError | None, message: Message) -> None:
        if error is not None:
            self.failures.append(f"{message.topic()} key {message.key()!r}: {error}")


def _log_client_error(error: KafkaError) -> None:
    # librdkafka retries broker and network errors itself; a fatal error (idempotence lost) stops the client.
    (log.error if error.fatal() else log.warning)("kafka: %s", error)


def make_publisher(**overrides: Any) -> Publisher:
    return Publisher(Producer(client_config(**{
        "enable.idempotence": True,  # implies acks=all and ordered retries
        "compression.type": "lz4",
        "linger.ms": 20,  # small batches: a paced replay still reaches the consumers within a blink
        "queue.buffering.max.messages": 10000,  # an unpaced replay waits for the broker instead of piling up memory
        "client.id": "safestart-publisher",
        "error_cb": _log_client_error,
    } | overrides)))


@dataclass(frozen=True)
class Record:
    """One Kafka message as the consumers see it."""

    topic: str
    partition: int
    offset: int
    value: bytes | None


@dataclass(frozen=True)
class Batch:
    records: list[Record]
    ended: set[tuple[str, int]]  # partitions read to their end, as of the last record of this batch


def decode(record: Record) -> Event:
    """The event a message carries; ValueError when it is not a valid event or sits on the wrong topic for its kind."""
    if record.value is None:
        raise ValueError("a message without a value")
    event = Event.from_json(record.value)
    if event.topic != record.topic:
        raise ValueError(f"a {event.kind} event belongs on {event.topic}, not on {record.topic}")
    return event


def make_consumer(group: str, **overrides: Any) -> Consumer:
    return Consumer(client_config(**{
        "group.id": group,
        "client.id": group,
        "enable.auto.commit": False,  # offsets are committed after the database commit
        "auto.offset.reset": "earliest",  # a new group starts from the oldest retained event
        "enable.partition.eof": True,  # report when a partition has been read to its end
        "queued.max.messages.kbytes": 16384,  # prefetch at most 16 MB (64 MB by default): the host has 4 GB
        "error_cb": _log_client_error,
    } | overrides))


def next_offsets(records: Iterable[Record]) -> list[TopicPartition]:
    """The positions to commit after a batch: one past the last record read from each partition."""
    last: dict[tuple[str, int], int] = {}
    for record in records:
        key = (record.topic, record.partition)
        last[key] = max(last.get(key, -1), record.offset)
    return [TopicPartition(topic, partition, offset + 1) for (topic, partition), offset in sorted(last.items())]


def run(consumer: Consumer, handle: Callable[[Batch], None], *, stop: threading.Event, topics: Iterable[str] = TOPICS,
        batch_size: int = 500, wait: float = 1.0,
        on_assign: Callable[[list[TopicPartition]], None] | None = None,
        on_revoke: Callable[[list[TopicPartition]], None] | None = None) -> None:
    """Consume until `stop` is set: each batch goes to `handle`, which commits its own database transaction, and only
    then are the batch's offsets committed. An exception from `handle` leaves them uncommitted and ends the loop, so
    a restarted consumer reads the batch again."""
    consumer.subscribe(list(topics), on_assign=lambda _, partitions: on_assign and on_assign(partitions),
                       on_revoke=lambda _, partitions: on_revoke and on_revoke(partitions))
    try:
        while not stop.is_set():
            records: list[Record] = []
            ended: set[tuple[str, int]] = set()
            for message in consumer.consume(batch_size, wait):
                place = (message.topic(), message.partition())
                if (error := message.error()) is None:
                    records.append(Record(*place, message.offset(), message.value()))
                    ended.discard(place)
                elif error.code() == KafkaError._PARTITION_EOF:
                    ended.add(place)
                elif error.fatal():
                    raise KafkaException(error)
                else:
                    log.warning("kafka: %s", error)
            if records or ended:
                handle(Batch(records, ended))
            if records:
                try:
                    consumer.commit(offsets=next_offsets(records), asynchronous=False)
                except KafkaException as problem:  # e.g. a rebalance took the partitions; the new owner re-reads
                    log.warning("offsets not committed, the batch will be read again: %s", problem)
    finally:
        consumer.close()


def stop_on_signals() -> threading.Event:
    """An event set by Ctrl+C or a termination signal, so a consumer finishes its batch and leaves its group."""
    stop = threading.Event()
    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        if (number := getattr(signal, name, None)) is not None:
            signal.signal(number, lambda *_: stop.set())
    return stop
