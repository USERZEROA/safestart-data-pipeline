"""Versioned, validated events: the contract between producers, watchers and consumers.

Every event has one envelope: what happened (`kind`), when it happened at the source (`occurred_at`, never rewritten),
which mine it concerns (with the mine's district, state and commodity, so consumers need no lookup), a one-line title
for the live feed, the kind's own values in `details`, and where the record came from in `source`. `event_id` is fixed
for a given source record, so a replay or a repeated live check is idempotent downstream.
"""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, fields
from datetime import UTC, date, datetime
from typing import Any, Mapping

SCHEMA_VERSION = 1
TOPIC_ENFORCEMENT = "msha.enforcement"  # inspections, violations, accidents and 107(a) orders, replayed or new
TOPIC_SOURCES = "safety.sources"  # new dataset versions, changed website pages, new Federal Register documents
ENFORCEMENT_KINDS = frozenset({"inspection", "violation", "accident", "order_107a"})
SOURCE_KINDS = frozenset({"dataset_version", "page_change", "regulation"})
KINDS = ENFORCEMENT_KINDS | SOURCE_KINDS
MODES = frozenset({"replay", "live"})
COMMODITIES = frozenset({"C", "M"})  # MSHA's coal / metal and non-metal indicator
MAX_TITLE = 300


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def as_utc(value: str | date | datetime) -> str:
    """An ISO 8601 UTC timestamp ending in Z. A bare date (most MSHA records carry only a date) is midnight UTC."""
    if isinstance(value, datetime):
        moment = value
    elif isinstance(value, date):
        moment = datetime(value.year, value.month, value.day, tzinfo=UTC)
    elif isinstance(value, str):
        try:
            moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as problem:
            raise ValueError(f"not an ISO 8601 timestamp: {value!r}") from problem
        if len(value) == 10:  # YYYY-MM-DD
            moment = moment.replace(tzinfo=UTC)
    else:
        raise ValueError(f"not a timestamp: {value!r}")
    if moment.tzinfo is None:
        raise ValueError(f"timestamp without a time zone: {value!r}")
    return moment.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _scalar(value: Any) -> bool:
    if isinstance(value, float):
        return math.isfinite(value)
    return value is None or isinstance(value, (str, int, bool))


@dataclass(frozen=True)
class Event:
    schema_version: int
    event_id: str
    kind: str
    mode: str
    occurred_at: str
    emitted_at: str
    mine_id: str | None
    district: str | None
    state: str | None
    coal_metal: str | None
    title: str
    details: dict[str, Any]
    source: dict[str, str]

    @classmethod
    def new(
        cls, *, kind: str, key: str, mode: str, occurred_at: str | date | datetime, title: str,
        details: Mapping[str, Any] | None = None, source: Mapping[str, str] | None = None, mine_id: str | None = None,
        district: str | None = None, state: str | None = None, coal_metal: str | None = None,
        emitted_at: str | None = None,
    ) -> Event:
        """A validated event; `key` is the source record's own identifier (violation number, document number, URL plus
        its last-modified time ...), so the same record always gets the same event_id."""
        return cls.from_dict({
            "schema_version": SCHEMA_VERSION, "event_id": f"{kind}:{key}", "kind": kind, "mode": mode,
            "occurred_at": as_utc(occurred_at), "emitted_at": emitted_at or utc_now(), "mine_id": mine_id,
            "district": district, "state": state, "coal_metal": coal_metal, "title": title[:MAX_TITLE],
            "details": dict(details or {}), "source": dict(source or {}),
        })

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Event:
        if not isinstance(data, Mapping):
            raise ValueError("an event is a JSON object")
        expected = {field.name for field in fields(cls)}
        if missing := expected - set(data):
            raise ValueError("missing event fields: " + ", ".join(sorted(missing)))
        if extra := set(data) - expected:
            raise ValueError("unsupported event fields: " + ", ".join(sorted(extra)))
        if data["schema_version"] != SCHEMA_VERSION or type(data["schema_version"]) is not int:
            raise ValueError("unsupported schema_version")
        if data["kind"] not in KINDS:
            raise ValueError(f"unknown kind: {data['kind']!r}")
        if data["mode"] not in MODES:
            raise ValueError(f"mode must be replay or live, not {data['mode']!r}")
        if not isinstance(data["event_id"], str) or not data["event_id"].startswith(f"{data['kind']}:"):
            raise ValueError("event_id must be '<kind>:<source key>'")
        if data["coal_metal"] is not None and data["coal_metal"] not in COMMODITIES:
            raise ValueError("coal_metal must be C, M or null")
        for name in ("mine_id", "district", "state"):
            if data[name] is not None and not isinstance(data[name], str):
                raise ValueError(f"{name} must be a string or null")
        if not isinstance(data["title"], str) or not data["title"].strip() or len(data["title"]) > MAX_TITLE:
            raise ValueError(f"title must be 1 to {MAX_TITLE} characters")
        details, source = data["details"], data["source"]
        if not isinstance(details, Mapping) or not all(isinstance(k, str) and _scalar(v) for k, v in details.items()):
            raise ValueError("details must map names to JSON scalars")
        if not isinstance(source, Mapping) or not all(isinstance(k, str) and isinstance(v, str) for k, v in source.items()):
            raise ValueError("source must map names to strings")
        return cls(**{**data, "occurred_at": as_utc(data["occurred_at"]), "emitted_at": as_utc(data["emitted_at"]),
                      "details": dict(details), "source": dict(source)})

    @classmethod
    def from_json(cls, raw: bytes | str) -> Event:
        try:
            data = json.loads(raw)
        except (TypeError, ValueError) as problem:
            raise ValueError("an event must be valid JSON") from problem
        return cls.from_dict(data)

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), separators=(",", ":"), ensure_ascii=False).encode("utf-8")

    @property
    def topic(self) -> str:
        return TOPIC_ENFORCEMENT if self.kind in ENFORCEMENT_KINDS else TOPIC_SOURCES

    @property
    def key(self) -> bytes:
        """Kafka key: a mine's events stay in order on one partition; source events are keyed by kind."""
        return (self.mine_id or self.kind).encode("utf-8")
