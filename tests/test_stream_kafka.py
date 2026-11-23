from __future__ import annotations

import threading
from concurrent.futures import Future
from datetime import date
from types import SimpleNamespace

import pytest
from confluent_kafka import KafkaError

from safestart_pipeline.stream.events import TOPIC_ENFORCEMENT, TOPIC_SOURCES, Event
from safestart_pipeline.stream.init import wait_for
from safestart_pipeline.stream.kafka import (
    RETENTION_MS,
    Publisher,
    Record,
    client_config,
    decode,
    ensure_topics,
    next_offsets,
    run,
)


def event(key="9837820", **overrides):
    values = {"kind": "violation", "key": key, "mode": "replay", "occurred_at": date(2025, 1, 8),
              "title": "Violation at a mine", "mine_id": "4609612", "district": "C04", "coal_metal": "C"}
    return Event.new(**{**values, **overrides})


class Message:
    def __init__(self, topic, partition, offset=-1, value=b"", error=None):
        self._topic, self._partition, self._offset, self._value, self._error = topic, partition, offset, value, error

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def value(self):
        return self._value

    def key(self):
        return None

    def error(self):
        return self._error


def eof(topic, partition):
    return Message(topic, partition, error=KafkaError(KafkaError._PARTITION_EOF))


class FakeConsumer:
    """Hands out the given polls, then sets `stop`; records commits."""

    def __init__(self, polls, stop):
        self.polls, self.stop, self.commits, self.closed = list(polls), stop, [], False

    def subscribe(self, topics, on_assign=None, on_revoke=None):
        self.topics = topics

    def consume(self, num_messages, timeout):
        if not self.polls:
            self.stop.set()
            return []
        return self.polls.pop(0)

    def commit(self, offsets, asynchronous):
        assert asynchronous is False  # a commit returns only once the broker has it
        self.commits.append([(o.topic, o.partition, o.offset) for o in offsets])

    def close(self):
        self.closed = True


def test_offsets_are_committed_only_after_the_handler_and_a_failed_batch_is_read_again():
    batch = [Message(TOPIC_ENFORCEMENT, 0, 7, b"a"), Message(TOPIC_ENFORCEMENT, 1, 3, b"b"),
             Message(TOPIC_ENFORCEMENT, 0, 8, b"c")]
    stop = threading.Event()
    crashed = FakeConsumer([batch], stop)

    def fail(_):
        raise RuntimeError("database down")

    with pytest.raises(RuntimeError, match="database down"):
        run(crashed, fail, stop=stop)
    assert crashed.commits == [] and crashed.closed  # nothing committed: the batch will be delivered again

    handled, stop = [], threading.Event()
    restarted = FakeConsumer([batch], stop)
    run(restarted, lambda b: handled.append(b), stop=stop)
    assert [record.offset for record in handled[0].records] == [7, 3, 8]
    assert restarted.commits == [[(TOPIC_ENFORCEMENT, 0, 9), (TOPIC_ENFORCEMENT, 1, 4)]]
    assert restarted.topics == [TOPIC_ENFORCEMENT, TOPIC_SOURCES]


def test_partitions_read_to_their_end_are_reported_with_the_batch():
    stop, handled = threading.Event(), []
    polls = [
        [Message(TOPIC_ENFORCEMENT, 0, 5, b"x"), eof(TOPIC_ENFORCEMENT, 0), eof(TOPIC_ENFORCEMENT, 1),
         Message(TOPIC_ENFORCEMENT, 1, 9, b"y")],  # partition 1 received more after its end was reported
        [eof(TOPIC_SOURCES, 0), Message(TOPIC_SOURCES, 0, error=KafkaError(KafkaError._TRANSPORT))],
    ]
    consumer = FakeConsumer(polls, stop)
    run(consumer, handled.append, stop=stop)
    assert handled[0].ended == {(TOPIC_ENFORCEMENT, 0)}
    assert handled[1].records == [] and handled[1].ended == {(TOPIC_SOURCES, 0)}
    assert len(consumer.commits) == 1  # an end-only batch has nothing to commit


def test_next_offsets_point_past_the_last_record_of_each_partition():
    records = [Record("t", 2, 40, b""), Record("t", 0, 11, b""), Record("t", 2, 39, b"")]
    assert [(o.topic, o.partition, o.offset) for o in next_offsets(records)] == [("t", 0, 12), ("t", 2, 41)]


def test_decode_refuses_empty_invalid_and_misplaced_messages():
    good = event()
    assert decode(Record(TOPIC_ENFORCEMENT, 0, 1, good.to_json())) == good
    for value, topic, message in ((None, TOPIC_ENFORCEMENT, "without a value"),
                                  (b"{", TOPIC_ENFORCEMENT, "valid JSON"),
                                  (good.to_json(), TOPIC_SOURCES, "belongs on msha.enforcement")):
        with pytest.raises(ValueError, match=message):
            decode(Record(topic, 0, 1, value))


class FakeProducer:
    def __init__(self, full=0, failing=False):
        self.full, self.failing, self.sent, self.pending = full, failing, [], []

    def produce(self, topic, value, key, on_delivery):
        if self.full:
            self.full -= 1
            raise BufferError("queue full")
        self.sent.append((topic, key, value))
        self.pending.append((on_delivery, SimpleNamespace(topic=lambda: topic, key=lambda: key)))

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        for callback, message in self.pending:
            callback(KafkaError(KafkaError._MSG_TIMED_OUT) if self.failing else None, message)
        self.pending.clear()
        return 0


def test_the_publisher_keys_events_by_mine_and_waits_for_room_in_a_full_queue():
    producer, sent = FakeProducer(full=2), event()
    publisher = Publisher(producer)
    publisher.send(sent)
    publisher.flush()
    assert producer.sent == [(TOPIC_ENFORCEMENT, b"4609612", sent.to_json())] and publisher.sent == 1


def test_flush_raises_when_an_event_was_not_delivered():
    publisher = Publisher(FakeProducer(failing=True))
    publisher.send(event())
    with pytest.raises(RuntimeError, match="not delivered"):
        publisher.flush()


def done(value=None):
    future = Future()
    future.set_result(value)
    return future


class FakeAdmin:
    def __init__(self, existing):
        self.existing, self.created, self.altered = existing, [], []

    def list_topics(self, timeout):
        return SimpleNamespace(topics={topic: None for topic in self.existing})

    def create_topics(self, topics, request_timeout):
        self.created += topics
        return {topic.topic: done() for topic in topics}

    def incremental_alter_configs(self, resources, request_timeout):
        self.altered += resources
        return {resource: done() for resource in resources}


def test_ensure_topics_creates_what_is_missing_and_sets_a_week_of_retention():
    admin = FakeAdmin(existing={TOPIC_ENFORCEMENT, "__consumer_offsets"})
    result = ensure_topics(admin, partitions=3)
    assert result == {TOPIC_SOURCES: "created", TOPIC_ENFORCEMENT: "retention set"}
    (created,) = admin.created
    assert (created.topic, created.num_partitions, created.config) == (TOPIC_SOURCES, 3, {"retention.ms": "604800000"})
    assert RETENTION_MS == 7 * 24 * 3600 * 1000
    assert [(r.name, [(e.name, e.value) for e in r.incremental_configs]) for r in admin.altered] == [
        (TOPIC_ENFORCEMENT, [("retention.ms", "604800000")])]


def test_sasl_and_ssl_come_from_the_environment_only(monkeypatch):
    for name in ("USERNAME", "PASSWORD", "MECHANISM"):
        monkeypatch.delenv(f"SAFESTART_KAFKA_SASL_{name}", raising=False)
    monkeypatch.delenv("SAFESTART_KAFKA_SECURITY_PROTOCOL", raising=False)
    monkeypatch.setenv("SAFESTART_KAFKA_BOOTSTRAP", "broker.example:9093")
    assert client_config() == {"bootstrap.servers": "broker.example:9093"}
    monkeypatch.setenv("SAFESTART_KAFKA_SASL_USERNAME", "pipeline")
    with pytest.raises(RuntimeError, match="PASSWORD"):
        client_config()
    monkeypatch.setenv("SAFESTART_KAFKA_SASL_PASSWORD", "secret")
    monkeypatch.setenv("SAFESTART_KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
    monkeypatch.setenv("SAFESTART_KAFKA_SSL_CA_LOCATION", "/etc/ssl/ca.pem")
    assert client_config(**{"group.id": "g"}) == {
        "bootstrap.servers": "broker.example:9093", "security.protocol": "SASL_SSL", "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": "pipeline", "sasl.password": "secret", "ssl.ca.location": "/etc/ssl/ca.pem", "group.id": "g"}


def test_init_waits_for_a_service_with_growing_pauses_then_gives_up():
    clock, pauses = [0.0], []

    def sleep(seconds):
        clock[0] += seconds
        pauses.append(seconds)

    def refused():
        raise OSError("refused")

    answers = iter([refused, refused, lambda: None])
    wait_for("kafka", lambda: next(answers)(), timeout=60, sleep=sleep, now=lambda: clock[0])
    assert pauses == [1.0, 2.0]
    pauses.clear()
    with pytest.raises(RuntimeError, match="postgresql did not answer within 5 s: refused"):
        wait_for("postgresql", refused, timeout=5, sleep=sleep, now=lambda: clock[0])
    assert pauses == [1.0, 2.0]  # a third pause of 4 s would end past the deadline
