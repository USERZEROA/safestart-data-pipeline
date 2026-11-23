from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from ml_fixtures import accident, mine, production, violation, write_lake
from safestart_pipeline import definitions
from safestart_pipeline.ml.features import (
    FEATURES,
    TARGET,
    build_panel,
    cfr_section,
    coal_metal,
    mine_details,
    quarter_label,
    scan,
    serious_accident,
    ss_violation,
)


def quarter(year: int, number: int) -> int:
    return year * 4 + number - 1


def lake_rows() -> dict[str, list[dict]]:
    """Mine 1 reports hours every quarter of 2000 to 2003 and is cited, inspected and hurt in 2002; mine 2 reports only
    in 2000Q1."""
    return {
        "mines": [mine("0100001"), mine("0100002", "M", current_mine_type="Surface", state="AZ", district="M6",
                                        primary_canvass="Stone", current_mine_status="Abandoned")],
        "mines_prod_quarterly": production("0100001", (2000, 1), (2003, 4))
        + production("0100002", (2000, 1), (2000, 1), 500, 2.0, "M"),
        "inspections": [{"event_no": "1", "mine_id": "0100001", "inspection_begin_dt": date(2002, 5, 10)}],
        "violations": [
            violation("v1", "0100001", date(2002, 5, 10), ss="Y", section="75.400(a)", penalty=1000.0,
                      billed=date(2002, 7, 15)),
            violation("v2", "0100001", date(2002, 6, 1), section="75.400", penalty=200.0, billed=date(2002, 8, 1)),
            violation("v3", "0100001", date(2002, 11, 20), section="75.1100-2(e)(2)"),
        ],
        "accidents": [
            accident("a1", "0100001", date(2002, 5, 20), "03"),  # days away, case still open: serious by its degree
            accident("a2", "0100001", date(2002, 9, 1), "06", days_lost=0),  # no lost days: not serious
        ],
    }


@pytest.fixture
def panel(tmp_path) -> pl.DataFrame:
    return build_panel(write_lake(tmp_path, lake_rows())).collect()


def row(panel: pl.DataFrame, mine_id: str, year: int, number: int) -> dict:
    return panel.filter(pl.col("mine_id") == mine_id, pl.col("quarter") == quarter(year, number)).row(0, named=True)


def test_a_quarter_sees_only_the_four_quarters_before_it(panel):
    # The citations, inspection and serious accident of 2002Q2 are not features of 2002Q2 itself, only its target.
    own = row(panel, "0100001", 2002, 2)
    assert (own["violations_4q"], own["inspections_4q"], own["serious_accidents_4q"], own[TARGET]) == (0, 0, 0, True)
    after = row(panel, "0100001", 2002, 3)
    assert (after["violations_4q"], after["ss_violations_4q"], after["cited_sections_4q"]) == (2, 1, 1)
    assert (after["inspections_4q"], after["accidents_4q"], after["serious_accidents_4q"]) == (1, 1, 1)
    assert after[TARGET] is False
    # They stay in the window for four quarters (2002Q3 to 2003Q2) and leave it in 2003Q3.
    assert [row(panel, "0100001", year, number)["violations_4q"] for year, number in
            ((2003, 1), (2003, 2), (2003, 3), (2003, 4))] == [3, 3, 1, 1]
    assert row(panel, "0100001", 2003, 1)["cited_sections_4q"] == 2  # 75.400 and 75.1100-2
    assert row(panel, "0100001", 2003, 3)["cited_sections_4q"] == 1


def test_a_penalty_counts_from_the_quarter_after_its_bill(panel):
    # Both 2002Q2 citations were billed in 2002Q3, so their penalties are known from 2002Q4 until 2003Q3.
    assert [row(panel, "0100001", year, number)["proposed_penalty_4q"] for year, number in
            ((2002, 3), (2002, 4), (2003, 3), (2003, 4))] == [0.0, 1200.0, 1200.0, 0.0]


def test_hours_and_population_come_from_prior_reports(panel):
    first = row(panel, "0100001", 2001, 1)
    hours = ("hours_worked_4q", "hours_worked_1q", "avg_employees_4q", "active_quarters_4q")
    assert [first[name] for name in hours] == [4000.0, 1000.0, 5.0, 4]
    # Mine 2 reported only in 2000Q1, so it is scored only while that report is in the window (from 2001Q1, the first
    # quarter with four quarters of reports before it).
    assert panel.filter(pl.col("mine_id") == "0100002")["quarter"].to_list() == [quarter(2001, 1)]
    other = row(panel, "0100002", 2001, 1)
    attributes = ("coal_metal", "mine_type", "state", "commodity", "active_quarters_4q")
    assert [other[name] for name in attributes] == ["M", "Surface", "AZ", "Stone", 1]
    assert "status" not in panel.columns  # the current status records the future


def test_the_quarter_after_the_newest_report_is_open(panel):
    assert quarter_label(panel["quarter"].max()) == "2004Q1"
    open_quarter = panel.filter(pl.col("quarter") == quarter(2004, 1))
    assert open_quarter[TARGET].null_count() == open_quarter.height == 1
    assert panel.filter(pl.col("quarter") < quarter(2004, 1))[TARGET].null_count() == 0


def test_nothing_from_the_target_quarter_or_later_changes_a_feature(tmp_path):
    rows = lake_rows()
    before = build_panel(write_lake(tmp_path / "before", rows)).collect()
    cutoff = quarter(2002, 3)
    later = {
        **rows,
        "inspections": rows["inspections"]
        + [{"event_no": "2", "mine_id": "0100001", "inspection_begin_dt": date(2002, 7, 1)}],
        "violations": rows["violations"] + [
            violation("v4", "0100001", date(2002, 8, 1), ss="Y", section="77.404(a)", penalty=50.0,
                      billed=date(2002, 9, 1)),
            violation("v5", "0100002", date(2003, 1, 5), ss="Y", penalty=70.0, billed=date(2003, 2, 1)),
        ],
        "accidents": rows["accidents"] + [accident("a3", "0100001", date(2002, 7, 4), "01")],
        "mines_prod_quarterly": rows["mines_prod_quarterly"]
        + production("0100002", (2002, 3), (2004, 2), 800, 3.0, "M"),
    }
    after = build_panel(write_lake(tmp_path / "after", later)).collect()

    def upto(frame: pl.DataFrame, inclusive: bool) -> pl.DataFrame:
        keep = pl.col("quarter") <= cutoff if inclusive else pl.col("quarter") < cutoff
        return frame.filter(keep).sort("mine_id", "quarter")

    assert upto(after, True).select("mine_id", "quarter", *FEATURES).equals(
        upto(before, True).select("mine_id", "quarter", *FEATURES))
    assert upto(after, False).equals(upto(before, False))
    assert row(after, "0100001", 2002, 3)[TARGET] is True  # the new fatality is the target of its own quarter


def test_scan_keeps_one_copy_of_each_key(tmp_path):
    # v1 is listed once per contest docket, the dearer copy first; the copy kept is the first by every column, so the
    # file's order does not decide. Rows without a key cannot be told apart and all stay.
    lake = write_lake(tmp_path, {"violations": [
        violation("v1", "1", date(2020, 1, 1), penalty=300.0) | {"docket_no": "D2"},
        violation("v2", "1", date(2020, 1, 2)),
        violation("v1", "1", date(2020, 1, 1), penalty=100.0) | {"docket_no": "D1"},
        violation(None, "1", date(2020, 1, 3)),
        violation(None, "1", date(2020, 1, 4)),
    ]})
    kept = scan(lake, "violations").collect().sort("violation_no", "violation_issue_dt", nulls_last=True)
    assert kept.select("violation_no", "proposed_penalty", "docket_no").rows() == [
        ("v1", 100.0, "D1"), ("v2", None, None), (None, None, None), (None, None, None)]


def test_a_record_listed_twice_counts_once(tmp_path):
    # MSHA lists a violation once per contest docket and an inspection once per controller; as in the warehouse, each
    # counts once, so the copies change no feature.
    rows = lake_rows()
    v1, *others = rows["violations"]
    listed = rows | {
        "violations": [v1 | {"docket_no": "D1"}, *others, v1 | {"docket_no": "D2"}],
        "inspections": [record | {"controller_id": controller} for record in rows["inspections"]
                        for controller in ("C1", "C2")],
    }
    once = build_panel(write_lake(tmp_path / "once", rows)).collect()
    assert build_panel(write_lake(tmp_path / "listed", listed)).collect().equals(once)


def test_ss_is_read_by_the_shared_mapping(tmp_path, monkeypatch):
    codes = ["Y", "N", None, "y", ""]
    ss = pl.DataFrame({"sig_sub": codes}).select(ss_violation())["sig_sub"].to_list()
    assert ss == [definitions.SIG_SUB.get(code) for code in codes] == [True, False, None, None, None]
    # The panel counts by the mapping itself: were N to mean S&S as well, both citations of 2002Q2 would count.
    monkeypatch.setitem(definitions.SIG_SUB, "N", True)
    assert row(build_panel(write_lake(tmp_path, lake_rows())).collect(), "0100001", 2002, 3)["ss_violations_4q"] == 2


def test_the_details_mark_a_missing_newest_report(tmp_path):
    # The newest quarter with hours is 2003Q4. Mine 1 reported it; mine 2 last reported in 2000Q1 and mine 3, missing
    # from the mines dataset, in 2003Q3; mine 4 reported 2003Q4 with no hours, which is a report, not a missing one.
    rows = lake_rows()
    rows["mines_prod_quarterly"] += [*production("0100003", (2003, 3), (2003, 3)),
                                     *production("0100004", (2003, 4), (2003, 4), hours=0)]
    details = mine_details(write_lake(tmp_path, rows)).collect().sort("mine_id")
    assert details.rows() == [("0100001", "Mine 0100001", "Active", False),
                              ("0100002", "Mine 0100002", "Abandoned", True),
                              ("0100003", None, None, True), ("0100004", None, None, False)]


def test_a_serious_accident_is_read_from_its_degree_alone():
    cases = [("01", None, None), ("02", None, None), ("03", None, None), ("05", None, 0), ("06", 0, 0),
             ("07", 3, None), ("08", 12, 4), ("10", None, None), (None, None, 2), (None, None, None), ("00", 0, None)]
    frame = pl.DataFrame([accident(str(n), "1", date(2020, 1, 1), degree, days_lost=lost, days_restrict=restricted)
                          for n, (degree, lost, restricted) in enumerate(cases)])
    serious = frame.select(serious_accident().alias("serious"))["serious"].to_list()
    assert serious == [definitions.is_serious(degree) for degree, _, _ in cases]
    # An open days-away case is serious before its day counts arrive; an illness with lost days is not.
    assert serious[2] is True and serious[5] is False


def test_a_cited_section_is_the_shared_pattern_match():
    cited = ["56.14107(a)", "75.1100-2(e)(2)", "56.5001(a)/.5005", "75.1107-16-b", " 75.400 ", "104(d)(1)", "", None]
    sections = pl.DataFrame({"part_section": cited}).select(cfr_section())["part_section"].to_list()
    assert sections == [definitions.cfr_section(citation) for citation in cited]
    assert sections == ["56.14107", "75.1100-2", "56.5001", "75.1107-16", "75.400", None, None, None]


def test_the_coal_metal_rule_is_the_shared_one():
    flags = [("C", "M"), ("M", "C"), (None, "C"), ("?", "M"), ("M", None), (None, None), (None, "X")]
    frame = pl.DataFrame(flags, schema={"mine": pl.String, "own": pl.String}, orient="row")
    assert frame.select(coal_metal("mine", "own").alias("flag"))["flag"].to_list() == [
        definitions.commodity(mine_flag, own_flag) for mine_flag, own_flag in flags]


def test_a_mines_flag_comes_from_the_mines_dataset_else_its_newest_report(tmp_path):
    # Mine 1's reports carry the other flag, but the mines dataset decides; mine 3 is missing from the mines dataset,
    # so its newest report's flag stands in, and its other attributes are unknown.
    lake = write_lake(tmp_path, {
        "mines": [mine("0100001", "C")],
        "mines_prod_quarterly": production("0100001", (2000, 1), (2000, 4), coal_metal="M")
        + production("0100003", (2000, 1), (2000, 3), coal_metal="C")
        + production("0100003", (2000, 4), (2000, 4), coal_metal="M"),
    })
    panel = build_panel(lake).collect()
    attributes = ("coal_metal", "mine_type", "state")
    assert [row(panel, "0100001", 2001, 1)[name] for name in attributes] == ["C", "Underground", "WV"]
    assert [row(panel, "0100003", 2001, 1)[name] for name in attributes] == ["M", "Unknown", "Unknown"]
