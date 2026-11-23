from __future__ import annotations

from dataclasses import asdict
from datetime import date

import duckdb
import polars as pl
import pytest

from safestart_pipeline.stream.events import ENFORCEMENT_KINDS
from safestart_pipeline.stream.records import BY_NAME, KINDS, event, one_copy_sql
from safestart_pipeline.stream.replay import build_event, read_rows
from safestart_pipeline.watch.publish import ListPublisher
from safestart_pipeline.watch.state import WatchState
from safestart_pipeline.watch.versions import check_versions
from test_watch_versions import (
    COAL_MINE,
    METAL_MINE,
    MINES,
    NEW,
    OLD,
    SCHEMAS,
    UNLISTED_MINE,
    accident,
    add_version,
    inspection,
    order,
    violation,
)

# The details of each kind, as docs/ARCHITECTURE.md lists them.
CONTRACT = {
    "inspection": ["activity_code", "activity", "begin_date", "end_date"],
    "violation": ["event_no", "cfr_section", "part_section", "sig_sub", "likelihood", "injury_illness", "negligence",
                  "proposed_penalty"],
    "accident": ["degree_injury_cd", "degree_injury", "serious", "classification", "accident_type", "no_injuries",
                 "days_lost"],
    "order_107a": ["termination_date"],
}


def record(kind: str, **values) -> dict:
    """A record as `records.mine_sql` reads it: at a listed mine, with every other value null unless given."""
    names = ("own_coal_metal", "own_state", "own_mine_name", *BY_NAME[kind].columns)
    return {name: None for name in names} | {
        "key": "1", "mine_id": COAL_MINE, "occurred": date(2026, 11, 20), "mine_district": "C04", "mine_state": "WV",
        "mine_name": "Highwall Park Surface Mine", "mine_coal_metal": "C"} | values


def live(kind: str, **values):
    return event(BY_NAME[kind], record(kind, **values), mode="live", source={})


@pytest.mark.parametrize("kind", sorted(CONTRACT))
def test_each_kind_gives_the_contracts_details(kind):
    assert list(live(kind).details) == CONTRACT[kind]


@pytest.mark.parametrize(("part", "sig_sub", "section", "title"), [
    ("75.400", "Y", "75.400", "S&S violation of 30 CFR 75.400"),
    ("75.1100-2(b)", "N", "75.1100-2", "Violation of 30 CFR 75.1100-2"),
    ("56.5001(a)/.5005", None, "56.5001", "Violation of 30 CFR 56.5001"),
    ("104(d)(1)", "Y", None, "S&S violation"),  # a section of the Mine Act, not of 30 CFR
    (None, None, None, "Violation"),
])
def test_a_violation_names_the_section_it_cites(part, sig_sub, section, title):
    cited = live("violation", part_section=part, sig_sub=sig_sub)
    assert (cited.details["cfr_section"], cited.details["part_section"]) == (section, part)
    assert cited.details["sig_sub"] is {"Y": True, "N": False}.get(sig_sub)
    assert cited.title == f"{title} at Highwall Park Surface Mine"


@pytest.mark.parametrize(("degree", "serious", "title"), [
    ("01", True, "Fatal accident"), ("02", True, "Permanent disability"), ("03", True, "Days-away injury"),
    ("05", True, "Restricted-duty injury"), ("06", False, "Injury without lost time"),
    ("07", False, "Occupational illness"), (None, False, "Accident"),
])
def test_an_accident_is_serious_by_its_degree_and_named_by_it(degree, serious, title):
    injured = live("accident", degree_injury_cd=degree, classification="POWERED HAULAGE", days_lost=None)
    assert injured.details["serious"] is serious
    assert injured.title == f"{title} at Highwall Park Surface Mine: Powered haulage"


def test_a_record_takes_its_mine_and_its_own_values_only_where_the_mine_is_missing():
    listed = live("order_107a", own_coal_metal="M", own_state="RI", own_mine_name="Portable 4")
    assert (listed.district, listed.state, listed.coal_metal, listed.title) == (
        "C04", "WV", "C", "Imminent-danger order (107(a)) at Highwall Park Surface Mine")
    missing = {"mine_district": None, "mine_state": None, "mine_name": None, "mine_coal_metal": None}
    unlisted = live("order_107a", **missing, own_coal_metal="M", own_state="RI", own_mine_name="Portable 4")
    assert (unlisted.district, unlisted.state, unlisted.coal_metal, unlisted.title) == (
        None, "RI", "M", "Imminent-danger order (107(a)) at Portable 4")
    nameless = live("inspection", **missing, mine_id="3700198", own_coal_metal="X")
    assert (nameless.title, nameless.coal_metal) == ("Inspection at mine 3700198", None)
    assert live("inspection", **missing, mine_id=None).title == "Inspection at an unknown mine"


def test_one_copy_is_kept_per_key_whatever_the_order_of_the_copies(tmp_path):
    copies = [
        violation("1", issued=date(2026, 11, 21)),
        violation("1", likelihood="Unlikely"),
        violation("1", issued=None),
        violation("1", likelihood="Reasonably"),
        violation("2", issued=None, likelihood="Unlikely"),
        violation("2", issued=None),
    ]
    kept = []
    for rows in (copies, copies[::-1]):
        path = tmp_path / f"copies-{len(kept)}.parquet"
        pl.DataFrame(rows, schema=SCHEMAS["violations"]).write_parquet(path)
        sql = one_copy_sql(BY_NAME["violation"], f"read_parquet('{path.as_posix()}')")
        kept.append(duckdb.connect().execute(f"select key, occurred, likelihood from ({sql}) order by key").fetchall())
    # The first by source date, then by the columns read; undated only when every copy is.
    assert kept[0] == kept[1] == [("1", date(2026, 11, 20), "Reasonably"), ("2", None, "Reasonably")]


@pytest.fixture
def lake(tmp_path):
    add_version(tmp_path, "mines", MINES, OLD)
    for dataset, old, new in (
        ("inspections", [inspection("1")], [
            inspection("2", end=None),
            inspection("3", inspection_begin_dt=None, inspection_end_dt=date(2026, 11, 5)),
            inspection("4", inspection_begin_dt=None, end=None),  # no date at all: in neither path
            inspection("5", mine_id=UNLISTED_MINE, coal_metal_ind="C", activity=None),
        ]),
        ("violations", [violation("1")], [
            violation("2", part="75.1100-2(b)"),
            violation("3", METAL_MINE, "56.5001(a)/.5005", None, coal_metal_ind="C"),
            violation("4", part="104(d)(1)", sig_sub="N"),
            violation("5", issued=None, violation_occur_dt=date(2026, 11, 18)),
            # A repeated key: its copies in different months, the later one first in the file.
            violation("6", issued=date(2026, 11, 2), likelihood="Unlikely"),
            violation("6", issued=date(2026, 10, 30)),
            violation("7", UNLISTED_MINE, coal_metal_ind="C"),
        ]),
        ("accidents", [accident("1")], [
            accident("2", "02"), accident("3", "07", None, days_restrict=3), accident("4", None, classification=None),
            accident("5", "01", mine_id=METAL_MINE, coal_metal_ind="C"),
        ]),
        ("orders_107a", [order("1")], [order("2", UNLISTED_MINE), order("3", METAL_MINE)]),
    ):
        add_version(tmp_path, dataset, old, OLD)
        add_version(tmp_path, dataset, old + new, NEW)
    return tmp_path


def comparable(found) -> dict:
    """What both paths must agree on: everything but the mode, the source and the moment it was sent."""
    return {name: value for name, value in asdict(found).items() if name not in ("mode", "source", "emitted_at")}


def test_the_replay_and_the_watcher_make_the_same_event_of_a_record(lake):
    state, publisher = WatchState(lake / "state" / "watch.sqlite"), ListPublisher()
    check_versions(lake, state, publisher)
    state.close()
    watched = {found.event_id: found for found in publisher.events if found.kind in ENFORCEMENT_KINDS}
    versions = {kind.name: NEW for kind in KINDS}
    rows = read_rows(lake / "lake" / "msha_ogd", date(2026, 1, 1))
    replayed = {found.event_id: found for found in (build_event(row, versions) for row in rows)}

    assert sorted(watched) == sorted(set(replayed) - {f"{kind}:1" for kind in CONTRACT}) and len(watched) == 15
    for event_id, found in watched.items():
        again = replayed[event_id]
        assert (found.mode, again.mode) == ("live", "replay")
        assert again.source == found.source | {"through": "2026-11-20"}  # the replay stamps its pass's last day
        assert comparable(found) == comparable(again), event_id

    kept = watched["violation:6"]
    assert (kept.occurred_at, kept.details["likelihood"]) == ("2026-10-30T00:00:00Z", "Reasonably")
    assert watched["violation:5"].occurred_at == "2026-11-18T00:00:00Z"
    assert watched["inspection:3"].occurred_at == "2026-11-05T00:00:00Z"
    assert (watched["violation:3"].coal_metal, watched["violation:3"].title) == (
        "M", "Violation of 30 CFR 56.5001 at O'Neal Quarry & Mill")
    assert (watched["violation:7"].coal_metal, watched["violation:7"].state, watched["violation:7"].title) == (
        "C", None, "S&S violation of 30 CFR 75.400 at Name as cited")
    assert watched["violation:4"].title == "Violation at Highwall Park Surface Mine"
    assert (watched["inspection:5"].title, watched["inspection:5"].coal_metal) == ("Inspection at mine 3700198", "C")
    assert {key: watched[f"accident:{key}"].details["serious"] for key in "2345"} == {
        "2": True, "3": False, "4": False, "5": True}
    assert (watched["accident:5"].coal_metal, watched["accident:4"].title) == (
        "M", "Accident at Highwall Park Surface Mine")
    assert (watched["order_107a:2"].state, watched["order_107a:2"].title) == (
        "RI", "Imminent-danger order (107(a)) at Portable 4")
