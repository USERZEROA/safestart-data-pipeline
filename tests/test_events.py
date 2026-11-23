from __future__ import annotations

from datetime import date

import pytest

from safestart_pipeline.stream.events import TOPIC_ENFORCEMENT, TOPIC_SOURCES, Event


def violation(**overrides):
    values = dict(kind="violation", key="9837820", mode="replay", occurred_at=date(2025, 1, 8),
                  title="S&S violation of 30 CFR 75.400 at Highwall Park Surface Mine",
                  details={"cfr_section": "75.400", "sig_sub": True, "proposed_penalty": 1205.0},
                  source={"dataset": "violations", "version": "1b0f05ec81db6631"},
                  mine_id="4609612", district="C04", state="WV", coal_metal="C", emitted_at="2026-11-23T12:00:00Z")
    return Event.new(**{**values, **overrides})


def test_an_event_round_trips_through_json():
    event = violation()
    assert event.event_id == "violation:9837820" and event.occurred_at == "2025-01-08T00:00:00Z"
    assert Event.from_json(event.to_json()) == event
    assert event.topic == TOPIC_ENFORCEMENT and event.key == b"4609612"


def test_source_events_go_to_their_own_topic_keyed_by_kind():
    event = Event.new(kind="regulation", key="2026-25001", mode="live", occurred_at="2026-11-23T14:05:00-05:00",
                      title="MSHA: Respirable crystalline silica; correction", source={"api": "federalregister"})
    assert event.topic == TOPIC_SOURCES and event.key == b"regulation"
    assert event.occurred_at == "2026-11-23T19:05:00Z"  # source times are kept, converted to UTC


@pytest.mark.parametrize("change, message", [
    ({"kind": "rumor"}, "unknown kind"),
    ({"mode": "backfill"}, "mode must be"),
    ({"coal_metal": "X"}, "coal_metal"),
    ({"title": " "}, "title"),
    ({"details": {"nested": {"a": 1}}}, "details"),
    ({"details": {"rate": float("nan")}}, "details"),
    ({"source": {"version": 3}}, "source"),
    ({"occurred_at": "2025-01-08T10:00:00"}, "time zone"),
])
def test_invalid_events_are_refused(change, message):
    with pytest.raises(ValueError, match=message):
        violation(**change)


def test_unknown_or_missing_fields_are_refused():
    data = violation().__dict__.copy()
    with pytest.raises(ValueError, match="unsupported"):
        Event.from_dict({**data, "extra": 1})
    data.pop("title")
    with pytest.raises(ValueError, match="missing"):
        Event.from_dict(data)
