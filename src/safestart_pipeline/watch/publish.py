"""Where watcher events go.

A Publisher sends events and flushes them. The real one is the stream layer's Kafka publisher
(`stream.kafka.make_publisher()`), imported only when the watchers run for real, so checks and tests need no broker;
ListPublisher keeps events in memory. `publish_new` adds the watchers' promise never to publish an event twice: ids
published before are skipped, and new ids are remembered once the publisher has flushed them. A crash between that flush
and the record repeats at most one check's events, which consumers absorb because an event's id is fixed for its source
record.
"""

from __future__ import annotations

import importlib
from collections.abc import Iterable
from dataclasses import dataclass, field
from itertools import batched
from typing import Protocol

from ..stream.events import Event, utc_now
from .state import WatchState

KAFKA_MODULE = "safestart_pipeline.stream.kafka"
BATCH = 1000


class Publisher(Protocol):
    def send(self, event: Event) -> None: ...

    def flush(self) -> None: ...


@dataclass
class ListPublisher:
    """Keeps what it is sent, for tests and dry runs."""

    events: list[Event] = field(default_factory=list)
    flushes: int = 0

    def send(self, event: Event) -> None:
        self.events.append(event)

    def flush(self) -> None:
        self.flushes += 1


def kafka_publisher() -> Publisher:
    """The stream layer's Kafka publisher; a RuntimeError that says so when that module or its factory is missing."""
    try:
        module = importlib.import_module(KAFKA_MODULE)
    except ModuleNotFoundError as problem:
        if problem.name != KAFKA_MODULE:  # the module exists but needs something not installed: say exactly that
            raise
        module = None
    make = getattr(module, "make_publisher", None)
    if make is None:
        raise RuntimeError(f"the watchers publish through {KAFKA_MODULE}.make_publisher(), which is not available;"
                           " add the stream layer's Kafka client before running them")
    return make()


def publish_new(events: Iterable[Event], publisher: Publisher, state: WatchState) -> int:
    """Send each event whose id was never published, flush, then remember the ids. Returns how many were sent."""
    seen: set[str] = set()
    sent: list[str] = []
    for chunk in batched(events, BATCH):
        fresh: dict[str, Event] = {}
        for event in chunk:
            if event.event_id not in seen:
                seen.add(event.event_id)
                fresh[event.event_id] = event
        published = state.emitted(list(fresh))
        for event_id, event in fresh.items():
            if event_id not in published:
                publisher.send(event)
                sent.append(event_id)
    if sent:
        publisher.flush()
        state.add_emitted(sent, utc_now())
        state.commit()
    return len(sent)
