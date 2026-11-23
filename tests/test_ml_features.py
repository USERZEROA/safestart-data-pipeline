from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from ml_fixtures import accident, mine, production, violation, write_lake
from safestart_pipeline.ml.features import FEATURES, TARGET, build_panel, cfr_section, quarter_label, serious_accident


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


def test_serious_accidents_are_fatal_or_lost_or_restricted_time():
    frame = pl.DataFrame(
        [accident(str(n), "1", date(2020, 1, 1), degree, days_lost=lost, days_restrict=restricted)
         for n, (degree, lost, restricted) in enumerate([
             ("01", None, None), ("02", None, None), ("03", None, None), ("05", None, 0), ("06", 0, 0),
             ("07", 3, None), ("10", None, None), (None, None, 2), (None, None, None), ("00", 0, None)])]
    )
    assert frame.select(serious_accident().alias("serious"))["serious"].to_list() == [
        True, True, True, True, False, True, False, True, False, False]


def test_a_cited_section_drops_its_paragraph():
    cited = pl.DataFrame({"part_section": ["56.14107(a)", "75.1100-2(e)(2)", "75.400", None]})
    assert cited.select(cfr_section())["part_section"].to_list() == ["56.14107", "75.1100-2", "75.400", None]
