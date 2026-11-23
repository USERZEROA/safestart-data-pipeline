from __future__ import annotations

import hashlib
import json
import threading
from datetime import date

import polars as pl
import pytest

from safestart_pipeline.stream.replay import (
    Pacer,
    build_event,
    next_month,
    read_rows,
    replay,
)

D = pl.Date
MINES = ({"mine_id": pl.String, "current_mine_name": pl.String, "district": pl.String, "state": pl.String,
          "coal_metal_ind": pl.String}, [
    ("4609612", "Highwall Park Surface Mine", "C04", "WV", "C"),
    ("0100003", "O'Neal Quarry & Mill", "M3", "AL", "M"),
])
INSPECTIONS = ({"event_no": pl.String, "mine_id": pl.String, "inspection_begin_dt": D, "inspection_end_dt": D,
                "activity_code": pl.String, "activity": pl.String, "coal_metal_ind": pl.String}, [
    ("I0", "4609612", date(2023, 12, 30), date(2024, 1, 2), "E01", "Regular Safety and Health Inspection", "C"),
    ("I1", "4609612", date(2024, 1, 2), None, "E01", "Regular Safety and Health Inspection", "C"),
    ("I2", "9999999", date(2024, 1, 3), date(2024, 1, 3), "E16", "Spot Inspection", "M"),  # mine not in the mines file
])
VIOLATIONS = ({"violation_no": pl.String, "event_no": pl.String, "mine_id": pl.String, "mine_name": pl.String,
               "violation_issue_dt": D, "part_section": pl.String, "sig_sub": pl.String, "likelihood": pl.String,
               "inj_illness": pl.String, "negligence": pl.String, "proposed_penalty": pl.Float64,
               "coal_metal_ind": pl.String}, [
    # The record's own C/M flag disagrees with its mine's: the mine's wins.
    ("V1", "I1", "4609612", "Highwall Park", date(2024, 1, 2), "75.400", "Y", "Reasonably", "LostDays",
     "ModNegligence", 1205.0, "M"),
    ("V2", "I2", "0100003", "Quarry", date(2024, 1, 3), "77.216-3(c)", "N", "Unlikely", "NoLostDays",
     "LowNegligence", None, "M"),
    ("V2", "I2", "0100003", "Quarry", date(2024, 1, 3), "77.216-3(c)", "N", "Unlikely", "NoLostDays",
     "LowNegligence", None, "M"),  # MSHA's export repeats a few records
    ("V3", "I2", "0100003", "Quarry", date(2024, 1, 4), None, None, None, None, None, 100.0, "M"),
])
ORDERS = ({"violation_no": pl.String, "mine_id": pl.String, "mine_name": pl.String, "violation_date_issued": D,
           "termination_date": D, "coal_or_metal": pl.String, "state_abbreviation": pl.String}, [
    ("V1", "4609612", "Highwall Park", date(2024, 1, 2), date(2024, 1, 2), "C", "WV"),
])
ACCIDENTS = ({"document_no": pl.String, "mine_id": pl.String, "accident_dt": D, "degree_injury_cd": pl.String,
              "degree_injury": pl.String, "classification": pl.String, "accident_type": pl.String,
              "no_injuries": pl.Int64, "days_lost": pl.Int64, "days_restrict": pl.Int64,
              "coal_metal_ind": pl.String}, [
    ("A1", "0100003", date(2024, 1, 3), "03", "DAYS AWAY FROM WORK ONLY", "HANDLING OF MATERIALS",
     "Over-exertion NEC", 1, None, None, "M"),  # serious by its degree; the day count is not in yet
    ("A2", "0100003", date(2024, 1, 4), "06", "NO DYS AWY FRM WRK,NO RSTR ACT", "MACHINERY", "Struck by", 1, 0, 0, "M"),
    ("A3", "0100003", date(2024, 1, 4), "07", "OCCUPATNAL ILLNESS NOT DEG 1-6", None, None, 1, None, 3, "M"),
    ("A4", "4609612", date(2024, 1, 5), "01", "FATALITY", "POWERED HAULAGE", "Struck by", 1, None, None, "C"),
])


def write(lake, name, table) -> str:
    schema, rows = table
    stem = hashlib.sha256(name.encode()).hexdigest()[:16]
    (lake / name).mkdir(parents=True)
    pl.DataFrame(rows, schema=schema, orient="row").write_parquet(lake / name / f"{stem}.parquet")
    (lake / name / "manifest.json").write_text(json.dumps({"dataset": name, "current": f"{stem}.parquet"}))
    return stem


@pytest.fixture
def lake(tmp_path):
    for name, table in (("mines", MINES), ("inspections", INSPECTIONS), ("violations", VIOLATIONS),
                        ("orders_107a", ORDERS), ("accidents", ACCIDENTS)):
        write(tmp_path, name, table)
    return tmp_path


def versions(lake):
    return {kind: json.loads((lake / name / "manifest.json").read_text())["current"][:-8]
            for kind, name in (("inspection", "inspections"), ("violation", "violations"),
                               ("order_107a", "orders_107a"), ("accident", "accidents"))}


def events(lake, start=date(2024, 1, 1), until=None):
    return [build_event(row, versions(lake)) for row in read_rows(lake, start, until)]


def test_months_are_read_one_after_another_without_a_gap(lake):
    assert [next_month(date(2024, 11, 30)), next_month(date(2024, 12, 1))] == [date(2024, 12, 1), date(2025, 1, 1)]
    assert [row.key for row in read_rows(lake, date(2023, 12, 30), date(2024, 1, 3))] == ["I0", "I1", "V1", "V1"]
    assert list(read_rows(lake, date(2024, 2, 1))) == []  # nothing after the newest record


def test_records_come_in_source_time_order_from_the_start_date_once_each(lake):
    rows = list(read_rows(lake, date(2024, 1, 1), date(2024, 1, 5)))
    assert [(row.day.day, row.kind, row.key) for row in rows] == [
        (2, "inspection", "I1"), (2, "violation", "V1"), (2, "order_107a", "V1"),
        (3, "inspection", "I2"), (3, "violation", "V2"), (3, "accident", "A1"),
        (4, "violation", "V3"), (4, "accident", "A2"), (4, "accident", "A3"),
    ]
    assert [(row.day_index, row.day_events) for row in rows[:3]] == [(0, 3), (1, 3), (2, 3)]


def test_events_carry_their_mine_and_the_contracts_details(lake):
    by_id = {event.event_id: event for event in events(lake)}
    v1 = by_id["violation:V1"]
    assert (v1.mine_id, v1.district, v1.state, v1.coal_metal, v1.mode) == ("4609612", "C04", "WV", "C", "replay")
    assert v1.occurred_at == "2024-01-02T00:00:00Z" and v1.topic == "msha.enforcement"
    assert v1.details == {"event_no": "I1", "cfr_section": "75.400", "part_section": "75.400", "sig_sub": True,
                          "likelihood": "Reasonably", "injury_illness": "LostDays", "negligence": "ModNegligence",
                          "proposed_penalty": 1205.0}
    assert v1.title == "S&S violation of 30 CFR 75.400 at Highwall Park Surface Mine"
    assert v1.source == {"dataset": "violations", "version": versions(lake)["violation"]}
    # A hyphenated section number is a section of its own (30 CFR 77.216-3); only the paragraph is dropped.
    assert by_id["violation:V2"].details["cfr_section"] == "77.216-3"
    v3 = by_id["violation:V3"]  # no section cited
    assert v3.details["cfr_section"] is None and v3.title == "Violation at O'Neal Quarry & Mill"
    i2 = by_id["inspection:I2"]  # its mine is unknown: the record's own commodity, no district
    assert (i2.district, i2.state, i2.coal_metal) == (None, None, "M")
    assert i2.title == "Spot Inspection began at mine 9999999"
    assert by_id["inspection:I1"].details == {
        "activity_code": "E01", "activity": "Regular Safety and Health Inspection", "begin_date": "2024-01-02",
        "end_date": None}
    assert by_id["order_107a:V1"].details == {"termination_date": "2024-01-02"}
    assert by_id["order_107a:V1"].title == "Imminent-danger order (107(a)) at Highwall Park Surface Mine"


def test_an_accident_is_serious_when_fatal_or_with_days_away_or_restricted(lake):
    accidents = {event.event_id: event for event in events(lake) if event.kind == "accident"}
    assert {key: event.details["serious"] for key, event in accidents.items()} == {
        "accident:A1": True, "accident:A2": False, "accident:A3": True, "accident:A4": True}
    assert accidents["accident:A4"].title == "Fatal accident at Highwall Park Surface Mine: Powered haulage"
    assert accidents["accident:A3"].title == "Occupational illness at O'Neal Quarry & Mill"


def test_the_pacer_spreads_each_source_day_over_its_interval():
    clock = FakeClock()
    pacer = Pacer(date(2024, 1, 1), day_seconds=10, now=clock.now, sleep=clock.sleep)
    for day, index, count in ((1, 0, 2), (1, 1, 2), (3, 0, 1)):
        pacer.wait(date(2024, 1, day), index, count)
    assert clock.sleeps == [5.0, 15.0]  # half-way through day 1, then the start of day 3 (day 2 is empty)


def test_the_pacer_caps_the_rate_and_catches_up_without_pausing():
    clock = FakeClock()
    capped = Pacer(date(2024, 1, 1), day_seconds=0, max_rate=4, now=clock.now, sleep=clock.sleep)
    for index in range(3):
        capped.wait(date(2024, 1, 9), index, 3)
    assert clock.sleeps == [0.25, 0.25]
    late = Pacer(date(2024, 1, 1), day_seconds=1, now=clock.now, sleep=clock.sleep)
    clock.t += 30  # the broker stalled
    late.wait(date(2024, 1, 2), 0, 1)
    assert clock.sleeps == [0.25, 0.25] and late.behind == pytest.approx(29.0)


def test_a_looping_replay_sends_the_same_events_again(lake):
    sent, stop = [], threading.Event()

    def send(event):
        sent.append(event.event_id)
        if len(sent) == 20:
            stop.set()

    summary = replay(send, lake, start=date(2024, 1, 1), day_seconds=0, loop=True, stop=stop)
    assert summary["passes"] == 2 and summary["events"] == 20 and summary["source_clock"] == "2024-01-05"
    assert sent[10:] == sent[:10] and len(set(sent)) == 10


class FakeClock:
    def __init__(self):
        self.t, self.sleeps = 100.0, []

    def now(self):
        return self.t

    def sleep(self, seconds):
        self.sleeps.append(round(seconds, 6))
        self.t += seconds
