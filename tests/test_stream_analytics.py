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
    pass_end,
    plan,
)
from safestart_pipeline.stream.events import TOPIC_ENFORCEMENT, TOPIC_SOURCES, Event
from safestart_pipeline.stream.replay import Row, build_event

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


E2 = (TOPIC_ENFORCEMENT, 2)
LAST = date(2026, 11, 21)  # the replay pass's last source day, stamped on each of its events


def replayed(mark: Watermark, partition, day: date) -> None:
    mark.seen(partition, day, replayed=True, through=LAST)


def test_live_events_never_move_the_clock_while_a_replay_pass_is_under_way():
    """A fresh start: the watcher's newest records reach partition 1 before the replay's first record does."""
    mark = Watermark()
    mark.assign([E0, E1, E2])
    mark.seen(E1, date(2026, 11, 20), replayed=False)
    for partition in (E0, E1, E2):
        mark.end(partition)
    assert mark.complete_through(TOPIC_ENFORCEMENT, wait_for_replay=True) is None  # an empty store waits
    replayed(mark, E0, date(2024, 1, 1))
    replayed(mark, E2, date(2024, 1, 1))
    mark.end(E0)
    mark.end(E2)
    assert mark.replaying(TOPIC_ENFORCEMENT)
    assert mark.complete_through(TOPIC_ENFORCEMENT, wait_for_replay=True) == date(2023, 12, 31)
    mark.seen(E0, date(2026, 11, 22), replayed=False)  # more live records, on a partition the replay places too
    assert mark.complete_through(TOPIC_ENFORCEMENT) == date(2023, 12, 31)
    mark.end(E0)
    assert mark.complete_through(TOPIC_ENFORCEMENT) == date(2023, 12, 31)
    replayed(mark, E1, date(2025, 6, 3))
    mark.end(E1)
    assert mark.complete_through(TOPIC_ENFORCEMENT) == date(2025, 6, 2)


def test_once_the_pass_has_reached_its_last_day_the_live_days_move_the_clock():
    """The weekly versions the watcher compares after the replay are forecast in the same process."""
    mark = Watermark()
    for partition, day in ((E0, LAST), (E1, LAST - timedelta(days=1)), (E2, LAST)):  # nothing on day LAST on E1
        replayed(mark, partition, day)
        mark.end(partition)
    assert not mark.replaying(TOPIC_ENFORCEMENT) and mark.complete_through(TOPIC_ENFORCEMENT) == LAST - timedelta(1)
    for partition in (E0, E1, E2):
        mark.seen(partition, LAST + timedelta(days=7), replayed=False)
    # Partition 1, still being read, may hold the rest of the pass: its replayed day holds the clock until its end.
    assert mark.complete_through(TOPIC_ENFORCEMENT) == LAST - timedelta(days=2)
    for partition in (E0, E1, E2):
        mark.end(partition)
    assert mark.complete_through(TOPIC_ENFORCEMENT) == LAST + timedelta(days=6)


def test_a_partition_still_being_read_behind_the_last_day_keeps_its_replayed_place():
    mark = Watermark()
    replayed(mark, E0, LAST)
    replayed(mark, E1, LAST - timedelta(days=5))  # the consumer is behind on partition 1
    mark.seen(E1, LAST + timedelta(days=3), replayed=False)
    mark.end(E0)
    assert mark.complete_through(TOPIC_ENFORCEMENT) == LAST - timedelta(days=6)
    mark.end(E1)  # read to its end: nothing of the pass is left on it
    assert mark.complete_through(TOPIC_ENFORCEMENT) == LAST + timedelta(days=2)


def test_a_rebalance_neither_ends_a_pass_nor_starts_one_again():
    during = Watermark()
    replayed(during, E0, date(2025, 3, 3))
    during.revoke([E0])
    during.assign([E0])
    during.seen(E0, date(2026, 11, 20), replayed=False)
    during.end(E0)
    assert during.replaying(TOPIC_ENFORCEMENT) and during.complete_through(TOPIC_ENFORCEMENT) is None

    after = Watermark()
    replayed(after, E0, LAST)
    after.revoke([E0])
    after.assign([E0])
    after.seen(E0, LAST + timedelta(days=4), replayed=False)
    after.end(E0)
    assert after.complete_through(TOPIC_ENFORCEMENT) == LAST + timedelta(days=3)


def test_topics_without_a_replay_pass_are_placed_by_their_newest_days():
    """safety.sources has no replay; a replay without stamps (older versions) is placed as the newest day it gave."""
    mark = Watermark()
    mark.seen(S0, date(2026, 11, 20), replayed=False)
    mark.end(S0)
    assert mark.complete_through(TOPIC_SOURCES) == date(2026, 11, 19)
    mark.seen(E0, date(2024, 3, 5), replayed=True)
    mark.seen(E0, date(2026, 11, 20), replayed=False)
    mark.end(E0)
    assert mark.complete_through(TOPIC_ENFORCEMENT) == date(2026, 11, 19)


def test_the_pass_stamp_is_read_from_replayed_events_only():
    def make(mode, source):
        return Event.new(kind="violation", key="1", mode=mode, occurred_at=date(2025, 1, 8), title="t", source=source)

    assert pass_end(make("replay", {"through": "2026-11-21"})) == LAST
    assert pass_end(make("replay", {"dataset": "violations"})) is None
    assert pass_end(make("replay", {"through": "the end"})) is None  # never a reason to stop the consumer
    assert pass_end(make("live", {"through": "2026-11-21"})) is None
    row = Row(date(2025, 1, 8), 0, 1, LAST, "violation", "1", {
        "key": "1", "mine_id": "4609612", "occurred": date(2025, 1, 8), "own_coal_metal": "C", "own_state": None,
        "own_mine_name": None, "event_no": "1", "part_section": "75.400", "sig_sub": "Y", "likelihood": None,
        "inj_illness": None, "negligence": None, "proposed_penalty": None, "mine_district": "C04", "mine_state": "WV",
        "mine_name": "Highwall", "mine_coal_metal": "C"})
    assert pass_end(build_event(row, {"violation": "a1" * 8})) == LAST  # the replay stamps what the consumer reads


def test_counts_treat_s_and_s_violations_and_serious_accidents_as_serious():
    def make(kind, details, district="C04"):
        return item(Event.new(kind=kind, key="1", mode="replay", occurred_at=date(2025, 1, 8), title="t",
                              details=details, district=district))

    assert make("violation", {"sig_sub": True}).serious and not make("violation", {"sig_sub": None}).serious
    assert make("accident", {"serious": True}).serious and not make("inspection", {"serious": True}).serious
    assert make("regulation", {}, district=None) == ("regulation:1", date(2025, 1, 8), "regulation", "", False)
