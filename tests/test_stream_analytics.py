from __future__ import annotations

import math
from datetime import date, timedelta

import pytest

from safestart_pipeline.stream.analytics import (
    ALPHA,
    METHOD,
    MIN_HISTORY,
    WEEKS,
    Watermark,
    forecast,
    forecast_rows,
    item,
    plan,
)
from safestart_pipeline.stream.events import TOPIC_ENFORCEMENT, TOPIC_SOURCES, Event

E0, E1, S0 = (TOPIC_ENFORCEMENT, 0), (TOPIC_ENFORCEMENT, 1), (TOPIC_SOURCES, 0)
MONDAY = date(2025, 1, 6)
DECAY = 1 - ALPHA  # the weight of each earlier week relative to the week after it


def test_the_forecast_is_an_ewma_of_the_same_weekday_in_earlier_weeks():
    expected = (20 + DECAY * 10) / (1 + DECAY)  # the weights 1 and DECAY, normalized
    result = forecast(18, [20, 10])
    assert result.predicted == pytest.approx(expected) and result.residual == pytest.approx(18 - expected)
    assert forecast(0, []) == (0.0, 0.0, False)


def test_an_anomaly_needs_history_and_a_residual_beyond_three_scales():
    steady = [10] * MIN_HISTORY
    assert not forecast(19, steady).anomaly  # 9 < 3 * sqrt(10): a Poisson count of 10 varies that much
    assert forecast(20, steady).anomaly  # 10 > 9.49
    assert not forecast(20, steady[:-1]).anomaly  # too little history to judge
    assert not forecast(3, [0] * 8).anomaly and forecast(4, [0] * 8).anomaly  # the scale never drops below 1
    noisy = [30, 2, 30, 2, 30, 2]
    expected = forecast(60, noisy)
    weights = [DECAY ** age for age in range(6)]
    mean = sum(w * c for w, c in zip(weights, noisy)) / sum(weights)
    spread = math.sqrt(sum(w * (c - mean) ** 2 for w, c in zip(weights, noisy)) / sum(weights))
    assert expected.predicted == pytest.approx(mean) and expected.anomaly == (60 - mean > 3 * spread)


def test_rows_cover_series_with_events_on_the_day_or_in_its_history():
    counts = {("violation", "C04"): {MONDAY: 12, MONDAY + timedelta(weeks=1): 9},
              ("violation", "M3"): {MONDAY + timedelta(days=1): 4}}
    targets = [("violation", "C04", MONDAY + timedelta(weeks=1)), ("violation", "C04", MONDAY + timedelta(weeks=2)),
               ("violation", "M3", MONDAY + timedelta(weeks=1)),  # a Monday: M3 has no Monday events at all
               ("violation", "C04", MONDAY + timedelta(days=2))]
    rows = forecast_rows(counts, targets, origin=MONDAY)
    assert [(row[0], row[2], row[3], round(row[4], 4)) for row in rows] == [
        (MONDAY + timedelta(weeks=1), "C04", 9, 12.0),
        (MONDAY + timedelta(weeks=2), "C04", 0, round((9 + DECAY * 12) / (1 + DECAY), 4)),
    ]
    assert rows[0][7] == METHOD
    # Days before the origin are not history: the stream did not cover them.
    assert forecast_rows(counts, [("violation", "C04", MONDAY + timedelta(weeks=1))],
                         origin=MONDAY + timedelta(days=1))[0][4] == 0.0


def test_the_plan_forecasts_new_days_and_redoes_what_depends_on_a_late_count():
    origin, done = MONDAY, MONDAY + timedelta(days=100)
    assert plan(None, None, origin, None, []) == ([], set())
    days, redo = plan(None, origin + timedelta(days=2), origin, None, [("violation", "C04", origin)])
    assert days == [origin, origin + timedelta(days=1), origin + timedelta(days=2)] and redo == set()
    days, redo = plan(done, done + timedelta(days=1), origin, origin, [("accident", "M3", done - timedelta(days=15)),
                                                                     ("accident", "M3", done + timedelta(days=1))])
    assert days == [done + timedelta(days=1)]
    assert redo == {("accident", "M3", done - timedelta(days=15)), ("accident", "M3", done - timedelta(days=8)),
                    ("accident", "M3", done - timedelta(days=1))}  # later weeks are not forecast yet
    late = done - timedelta(weeks=WEEKS + 2)
    assert len(plan(done, None, origin, origin, [("accident", "M3", late)])[1]) == WEEKS + 1


def test_reaching_further_back_redoes_the_days_whose_history_crossed_the_former_origin():
    former, done = MONDAY, MONDAY + timedelta(days=200)
    days, _ = plan(done, done, former - timedelta(days=30), former, [])
    assert days == [former + timedelta(days=n) for n in range(7 * WEEKS)]


def test_a_day_is_complete_once_every_partition_is_past_it_or_read_to_its_end():
    mark = Watermark()
    mark.assign([E0, E1, S0])
    mark.seen(E0, date(2024, 3, 5), replayed=True)
    assert mark.complete_through(TOPIC_ENFORCEMENT) is None  # partition 1 has not placed itself yet
    mark.seen(E1, date(2024, 3, 3), replayed=True)
    assert mark.complete_through(TOPIC_ENFORCEMENT) == date(2024, 3, 2)
    mark.end(E1)  # partition 1 has nothing more: whatever comes next is at least as new as the newest day seen
    assert mark.complete_through(TOPIC_ENFORCEMENT) == date(2024, 3, 4)
    mark.seen(E1, date(2024, 3, 6), replayed=True)
    assert mark.complete_through(TOPIC_ENFORCEMENT) == date(2024, 3, 4)
    assert mark.complete_through(TOPIC_SOURCES) is None
    mark.end(S0)
    assert mark.complete_through(TOPIC_SOURCES) is None  # read to its end without a single event: no day known


def test_live_events_place_a_partition_only_when_no_replayed_event_has():
    mark = Watermark()
    mark.seen(E0, date(2024, 3, 5), replayed=True)
    mark.seen(E0, date(2026, 11, 20), replayed=False)  # a record a watcher found, ahead of the replay
    mark.seen(E1, date(2026, 11, 18), replayed=False)
    assert mark.complete_through(TOPIC_ENFORCEMENT) == date(2024, 3, 4)
    mark.revoke([E0])
    assert mark.complete_through(TOPIC_ENFORCEMENT) == date(2026, 11, 17)


def test_counts_treat_s_and_s_violations_and_serious_accidents_as_serious():
    def make(kind, details, district="C04"):
        return item(Event.new(kind=kind, key="1", mode="replay", occurred_at=date(2025, 1, 8), title="t",
                              details=details, district=district))

    assert make("violation", {"sig_sub": True}).serious and not make("violation", {"sig_sub": None}).serious
    assert make("accident", {"serious": True}).serious and not make("inspection", {"serious": True}).serious
    assert make("regulation", {}, district=None) == ("regulation:1", date(2025, 1, 8), "regulation", "", False)
