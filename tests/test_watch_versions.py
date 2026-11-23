from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from safestart_pipeline.watch.publish import ListPublisher
from safestart_pipeline.watch.state import WatchState
from safestart_pipeline.watch.versions import (
    WATCHED,
    Version,
    check_versions,
    pending_pairs,
)

OLD, NEW, NEWER, NEWEST = "a1" * 8, "b2" * 8, "c3" * 8, "d4" * 8
COAL_MINE, METAL_MINE, UNLISTED_MINE = "4609612", "0100003", "3700198"
SCHEMAS = {
    "mines": {"mine_id": pl.String, "current_mine_name": pl.String, "state": pl.String, "district": pl.String,
              "coal_metal_ind": pl.String},
    "violations": {
        "violation_no": pl.String, "event_no": pl.String, "mine_id": pl.String, "mine_name": pl.String,
        "coal_metal_ind": pl.String, "violation_issue_dt": pl.Date, "violation_occur_dt": pl.Date,
        "part_section": pl.String, "sig_sub": pl.String, "likelihood": pl.String, "inj_illness": pl.String,
        "negligence": pl.String, "proposed_penalty": pl.Float64,
    },
    "inspections": {
        "event_no": pl.String, "mine_id": pl.String, "inspection_begin_dt": pl.Date, "inspection_end_dt": pl.Date,
        "activity_code": pl.String, "activity": pl.String, "coal_metal_ind": pl.String,
    },
    "accidents": {
        "document_no": pl.String, "mine_id": pl.String, "accident_dt": pl.Date, "degree_injury_cd": pl.String,
        "degree_injury": pl.String, "classification": pl.String, "accident_type": pl.String, "no_injuries": pl.Int64,
        "days_lost": pl.Int64, "days_restrict": pl.Int64, "coal_metal_ind": pl.String,
    },
    "orders_107a": {
        "violation_no": pl.String, "mine_id": pl.String, "mine_name": pl.String, "coal_or_metal": pl.String,
        "state_abbreviation": pl.String, "violation_date_issued": pl.Date, "termination_date": pl.Date,
    },
}
MINES = [
    {"mine_id": COAL_MINE, "current_mine_name": "Highwall Park Surface Mine", "state": "WV", "district": "C04",
     "coal_metal_ind": "C"},
    {"mine_id": METAL_MINE, "current_mine_name": "O'Neal Quarry & Mill", "state": "AL", "district": "M3",
     "coal_metal_ind": "M"},
]


def violation(number: str, mine: str = COAL_MINE, part: str | None = "75.400", sig_sub: str | None = "Y",
              issued: date | None = date(2026, 11, 20), **changes) -> dict:
    return {"violation_no": number, "event_no": "4500001", "mine_id": mine, "mine_name": "Name as cited",
            "coal_metal_ind": "C" if mine == COAL_MINE else "M", "violation_issue_dt": issued,
            "violation_occur_dt": issued, "part_section": part, "sig_sub": sig_sub, "likelihood": "Reasonably",
            "inj_illness": "LostDays", "negligence": "ModNegligence", "proposed_penalty": None} | changes


def inspection(number: str, end: date | None = date(2026, 11, 2), **changes) -> dict:
    return {"event_no": number, "mine_id": METAL_MINE, "inspection_begin_dt": date(2026, 11, 1),
            "inspection_end_dt": end, "activity_code": "E01", "activity": "Regular Safety and Health Inspection",
            "coal_metal_ind": "M"} | changes


def accident(number: str, degree: str | None = "06", days_lost: int | None = 0, **changes) -> dict:
    return {"document_no": number, "mine_id": COAL_MINE, "accident_dt": date(2026, 11, 16), "degree_injury_cd": degree,
            "degree_injury": "DAYS AWAY FROM WORK ONLY", "classification": "HANDLING OF MATERIALS",
            "accident_type": "Caught in, under or between", "no_injuries": 1, "days_lost": days_lost,
            "days_restrict": None, "coal_metal_ind": "C"} | changes


def order(number: str, mine: str = COAL_MINE, **changes) -> dict:
    return {"violation_no": number, "mine_id": mine, "mine_name": "Portable 4", "coal_or_metal": "M",
            "state_abbreviation": "RI", "violation_date_issued": date(2026, 11, 17), "termination_date": None} | changes


def sha256(dataset: str, stem: str) -> str:
    """A stand-in for the source zip's SHA-256: it starts with the Parquet stem, as the lake's names do."""
    return stem + hashlib.sha256(dataset.encode()).hexdigest()[16:]


def add_version(root: Path, dataset: str, rows: list[dict], stem: str, built_at: str = "2026-11-23T15:40:00Z") -> None:
    """One lake version as lake/msha_ogd.py writes it: a Parquet file and its manifest entry, the newest as current."""
    folder = root / "lake" / "msha_ogd" / dataset
    folder.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, schema=SCHEMAS[dataset]).write_parquet(folder / f"{stem}.parquet")
    manifest_path = folder / "manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {"dataset": dataset, "versions": []}
    manifest["versions"].append({"format": 4, "source_sha256": sha256(dataset, stem), "source_file": f"{dataset}.zip",
                                 "parquet": f"{stem}.parquet", "rows": len(rows), "built_at": built_at, "columns": []})
    manifest["current"] = f"{stem}.parquet"
    manifest_path.write_text(json.dumps(manifest))


def by_id(publisher: ListPublisher) -> dict:
    return {event.event_id: event for event in publisher.events}


@pytest.fixture
def lake(tmp_path):
    add_version(tmp_path, "mines", MINES, OLD)
    return tmp_path


def test_each_new_record_becomes_one_live_event_with_its_mine(lake):
    add_version(lake, "violations", [violation("1"), violation("2")], OLD)
    add_version(lake, "violations", [
        violation("1"), violation("2"),
        # A repeated key: the copy kept is the first by source date, wherever it stands in the file.
        violation("3", issued=date(2026, 11, 21), likelihood="Unlikely"), violation("3"),
        violation("4", METAL_MINE, "56.14107(a)", "N", coal_metal_ind="C"),  # the mine's commodity wins
        violation("5", issued=None),
        violation("6", issued=None, violation_occur_dt=date(2026, 11, 18)),  # no issue date: when it occurred
    ], NEW)
    add_version(lake, "inspections", [inspection("1")], OLD)
    add_version(lake, "inspections", [inspection("1"), inspection("2", end=None)], NEW)
    add_version(lake, "accidents", [accident("1")], OLD)
    add_version(lake, "accidents", [accident("1"), accident("2", "03", None), accident("3"), accident("4", "02"),
                                    accident("5", "07", None, days_restrict=3)], NEW, "2026-11-23T15:42:26Z")
    add_version(lake, "orders_107a", [order("1")], OLD)
    add_version(lake, "orders_107a", [order("1"), order("9", UNLISTED_MINE)], NEW)
    (lake / "state").mkdir()
    bulk = sqlite3.connect(lake / "state" / "bulk.sqlite")
    bulk.execute("create table versions (url text, sha256 text, content_length integer, last_modified text,"
                 " first_seen_at text)")
    bulk.execute("insert into versions values ('u', ?, 1, 'Mon, 23 Nov 2026 12:50:20 GMT', '2026-11-23T13:20:16Z')",
                 (sha256("violations", NEW),))
    bulk.commit()
    bulk.close()
    state, publisher = WatchState(lake / "state" / "watch.sqlite"), ListPublisher()

    check = check_versions(lake, state, publisher)

    events = by_id(publisher)
    assert sorted(events) == sorted([
        "violation:3", "violation:4", "violation:6", "inspection:2", "accident:2", "accident:3", "accident:4",
        "accident:5", "order_107a:9", *(f"dataset_version:{dataset}:{NEW}" for dataset in WATCHED)])
    assert (check.status, check.found) == ("ok", 13) and publisher.flushes == 4
    assert "violations a1a1a1a1a1a1a1a1 -> b2b2b2b2b2b2b2b2: 4 new (1 without a date left out)" in check.note
    serious = events["violation:3"]
    assert serious.title == "S&S violation of 30 CFR 75.400 at Highwall Park Surface Mine"
    assert (serious.mode, serious.occurred_at, serious.mine_id, serious.district, serious.state,
            serious.coal_metal) == ("live", "2026-11-20T00:00:00Z", COAL_MINE, "C04", "WV", "C")
    assert serious.details == {"event_no": "4500001", "cfr_section": "75.400", "part_section": "75.400",
                               "sig_sub": True, "likelihood": "Reasonably", "injury_illness": "LostDays",
                               "negligence": "ModNegligence", "proposed_penalty": None}
    assert serious.source == {"dataset": "violations", "version": NEW}
    cited = events["violation:4"]
    assert cited.title == "Violation of 30 CFR 56.14107 at O'Neal Quarry & Mill"
    assert (cited.details["cfr_section"], cited.details["part_section"], cited.details["sig_sub"]) == (
        "56.14107", "56.14107(a)", False)
    assert (cited.district, cited.coal_metal) == ("M3", "M")
    assert events["violation:6"].occurred_at == "2026-11-18T00:00:00Z"
    assert events["inspection:2"].details == {
        "activity_code": "E01", "activity": "Regular Safety and Health Inspection", "begin_date": "2026-11-01",
        "end_date": None}
    assert events["inspection:2"].title == "Regular Safety and Health Inspection at O'Neal Quarry & Mill"
    away = events["accident:2"]  # days away from work, the day count not yet reported
    assert away.details["serious"] is True
    assert away.title == "Days-away injury at Highwall Park Surface Mine: Handling of materials"
    assert {key: events[f"accident:{key}"].details["serious"] for key in "345"} == {"3": False, "4": True, "5": False}
    unlisted = events["order_107a:9"]  # a mine the mines dataset lacks: the order's own name, state and commodity
    assert (unlisted.title, unlisted.state, unlisted.district, unlisted.coal_metal) == (
        "Imminent-danger order (107(a)) at Portable 4", "RI", None, "M")
    assert unlisted.details == {"termination_date": None}
    summary = events[f"dataset_version:violations:{NEW}"]
    assert summary.details == {"dataset": "violations", "version": NEW, "previous_version": OLD, "rows": 7,
                               "new_rows": 4}
    assert summary.occurred_at == "2026-11-23T12:50:20Z"  # the file's Last-Modified time in the bulk store
    assert events[f"dataset_version:accidents:{NEW}"].occurred_at == "2026-11-23T15:42:26Z"  # else the lake's build
    assert events[f"dataset_version:orders_107a:{NEW}"].title == "New MSHA 107(a) orders data: 1 new of 2 records"
    assert summary.topic == "safety.sources" and serious.topic == "msha.enforcement"

    again = check_versions(lake, state, publisher)
    assert (again.status, again.found, again.note) == ("unchanged", 0, "no new versions")
    assert len(publisher.events) == 13
    state.close()


def test_versions_that_arrive_later_are_compared_in_turn_and_no_record_is_announced_twice(lake):
    only_violations = {"violations": WATCHED["violations"]}
    add_version(lake, "violations", [violation("1")], OLD)
    add_version(lake, "violations", [violation("1"), violation("2")], NEW)
    state, publisher = WatchState(lake / "state" / "watch.sqlite"), ListPublisher()
    check_versions(lake, state, publisher, only_violations)
    assert sorted(by_id(publisher)) == ["dataset_version:violations:" + NEW, "violation:2"]
    state.close()

    # Two versions arrive while the watcher is stopped; record 2 leaves and comes back.
    add_version(lake, "violations", [violation("1"), violation("3")], NEWER)
    add_version(lake, "violations", [violation("1"), violation("2"), violation("3"), violation("4")], NEWEST)
    state, publisher = WatchState(lake / "state" / "watch.sqlite"), ListPublisher()
    check = check_versions(lake, state, publisher, only_violations)
    assert [event.event_id for event in publisher.events] == [
        "violation:3", f"dataset_version:violations:{NEWER}", "violation:4", f"dataset_version:violations:{NEWEST}"]
    assert by_id(publisher)[f"dataset_version:violations:{NEWEST}"].details["new_rows"] == 2  # 2 and 4 by the diff
    assert check.found == 4 and state.compared_versions("violations") == {NEW, NEWER, NEWEST}
    state.close()


def test_a_first_check_compares_only_the_two_newest_versions(lake):
    add_version(lake, "violations", [violation("1")], OLD)
    add_version(lake, "violations", [violation("1"), violation("2")], NEW)
    add_version(lake, "violations", [violation("1"), violation("2"), violation("3")], NEWER)
    state, publisher = WatchState(lake / "state" / "watch.sqlite"), ListPublisher()
    check_versions(lake, state, publisher, {"violations": WATCHED["violations"]})
    assert sorted(by_id(publisher)) == [f"dataset_version:violations:{NEWER}", "violation:3"]
    assert by_id(publisher)[f"dataset_version:violations:{NEWER}"].details["previous_version"] == NEW
    state.close()


def test_a_version_whose_keys_changed_is_refused_and_left_uncompared(lake):
    add_version(lake, "violations", [violation(str(number)) for number in range(1500)], OLD)
    add_version(lake, "violations", [violation(f"0{number}") for number in range(1500)], NEW)
    state, publisher = WatchState(lake / "state" / "watch.sqlite"), ListPublisher()
    check = check_versions(lake, state, publisher, {"violations": WATCHED["violations"]})
    assert check.status == "error" and "1,500 of 1,500 records look new" in check.note
    assert publisher.events == [] and state.compared_versions("violations") == set()
    state.close()


def test_a_lake_without_mines_is_an_error_not_a_crash(tmp_path):
    add_version(tmp_path, "violations", [violation("1")], OLD)
    add_version(tmp_path, "violations", [violation("1"), violation("2")], NEW)
    state, publisher = WatchState(tmp_path / "state" / "watch.sqlite"), ListPublisher()
    check = check_versions(tmp_path, state, publisher, {"violations": WATCHED["violations"]})
    assert check.status == "error" and "no mines dataset" in check.note and publisher.events == []
    state.close()


def held(*names: str) -> list[Version]:
    return [Version(name, Path(f"{name}.parquet"), 1, "f.zip", name * 4, "2026-11-23T00:00:00Z") for name in names]


@pytest.mark.parametrize("names, compared, pairs", [
    (["v1"], set(), []),
    (["v1", "v2"], set(), [("v1", "v2")]),
    (["v1", "v2", "v3"], set(), [("v2", "v3")]),
    (["v1", "v2", "v3", "v4"], {"v2"}, [("v2", "v3"), ("v3", "v4")]),
    (["v1", "v2"], {"v2"}, []),
])
def test_pairs_run_from_the_last_compared_version_to_the_newest(names, compared, pairs):
    assert [(older.name, newer.name) for older, newer in pending_pairs(held(*names), compared)] == pairs
