"""The dashboard renders every page from a tiny warehouse made of the warehouse's own tables, macros and marts
(schema.sql, build.MACROS, marts.sql) over hand-written rows, and from one the real build made of the warehouse tests'
sources, with PostgreSQL absent, present (through stand-in queries) or real; its data layer turns each missing store
into a message.

The PostgreSQL test runs when TEST_DATABASE_URL names a database, as the consumers' tests do: it replaces the `stream`
schema there with `stream/schema.sql` and synthetic events, and drops it at the end.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import psycopg
import pytest
import streamlit as st
from psycopg.types.json import Jsonb
from streamlit.testing.v1 import AppTest

from safestart_pipeline.dashboard import app as dashboard
from safestart_pipeline.dashboard import charts, data
from safestart_pipeline.stream.events import ENFORCEMENT_KINDS, KINDS, Event
from safestart_pipeline.warehouse.build import MACROS, build
from test_warehouse import write_data_root

PACKAGE = Path(__file__).resolve().parents[1] / "src" / "safestart_pipeline"
APP = PACKAGE / "dashboard" / "app.py"
WAREHOUSE = PACKAGE / "warehouse"
UNREACHABLE = "postgresql://dashboard:secret-password@127.0.0.1:9/nothing"  # nothing listens on port 9
# The number formats Streamlit's frontend writes in the browser's language (Intl.NumberFormat); printf formats and
# "accounting" read the same in every browser.
LOCALE_FORMATS = {"localized", "compact", "percent", "dollar", "euro", "yen", "scientific", "engineering", "bytes"}

# Rows of the warehouse's tables. Section number 7.7 is in both titles, as in the eCFR, and its title-29 row comes
# first, as in the real warehouse; MSHA cites title 30. The abandoned mine's name holds Markdown, as some of MSHA's
# names do ("**Mobile Crushing Unit**").
ROWS = """
insert into mine values
 ('4609612', 'Highwall Park Surface Mine', 'C', 'Surface', 'Active', '2010-05-01', 'WV', 'Boone', 'C04',
  'Bituminous coal', 'Coal (Bituminous)', 38.05, -81.8, 'C001', 'Highwall Holdings', 'P001', 'Highwall Park LLC'),
 ('4601234', '**Highwall Deep Mine No. 2**', 'C', 'Underground', 'Abandoned', '2019-02-01', 'WV', 'Logan', 'C04',
  'Bituminous coal', 'Coal (Bituminous)', 37.8, -81.9, 'C001', 'Highwall Holdings', 'P002', 'Highwall Deep LLC'),
 ('0100003', 'O''Neal Quarry & Mill', 'M', 'Surface', 'Active', '1979-01-22', 'AL', 'Shelby', 'M3', 'Lime', 'Stone',
  33.18, -86.76, 'M00174', 'Martin Marietta Materials Inc', 'L16197', 'Martin Marietta Materials, Inc.');
insert into inspection values
 ('E1', '4609612', '2026-10-01', '2026-10-03', 'E01', 'Regular Safety and Health Inspection', 'C'),
 ('E2', '4609612', '2026-03-02', '2026-03-05', 'E01', 'Regular Safety and Health Inspection', 'C'),
 ('E3', '0100003', '2026-06-10', '2026-06-10', 'E01', 'Regular Safety and Health Inspection', 'M'),
 ('E4', '4609612', '2020-04-01', '2020-04-02', 'E01', 'Regular Safety and Health Inspection', 'C');
insert into violation values
 ('V1', 'E1', '4609612', '2026-10-02', '2026-10-02', '75.400', '75.400', true, 'Reasonably Likely', 'Lost Workdays',
  'Moderate', 1205, 0, '2026-10-05', 'C', 'Surface'),
 ('V2', 'E1', '4609612', '2026-10-02', '2026-10-02', '77.1605(b)', '77.1605', false, 'Unlikely', 'Lost Workdays',
  'Low', 160, 160, '2026-10-04', 'C', 'Surface'),
 ('V3', 'E2', '4609612', '2026-03-03', '2026-03-03', '75.400', '75.400', true, 'Reasonably Likely', 'Lost Workdays',
  'Moderate', 1000, 1000, '2026-03-06', 'C', 'Surface'),
 ('V4', 'E3', '0100003', '2026-11-21', '2026-11-21', '56.14107(a)', '56.14107', true, 'Reasonably Likely',
  'Permanently Disabling', 'High', 5000, 0, null, 'M', 'Surface'),
 ('V5', 'E4', '4609612', '2020-04-01', '2020-04-01', '75.400', '75.400', false, 'Unlikely', 'Lost Workdays', 'Low',
  117, 117, '2020-04-03', 'C', 'Surface'),
 ('V6', 'E3', '0100003', '2026-06-10', '2026-06-10', '56.9999', '56.9999', false, 'Unlikely', 'No Lost Workdays',
  'Low', 100, 100, '2026-06-12', 'M', 'Surface'),
 ('V7', 'E3', '0100003', '2026-06-10', '2026-06-10', '7.7(c)', '7.7', false, 'Unlikely', 'No Lost Workdays', 'Low',
  250, 250, '2026-06-12', 'M', 'Surface');
insert into accident values
 ('A1', '4609612', '2026-09-14', '03', 'DAYS AWAY FROM WORK ONLY', true, 'HANDLING OF MATERIALS',
  'Caught in, under or between', 1, 12, 0, 'STRIP, QUARY, OPEN PIT',
  'Moving a $40 roller *by hand* when [it] slipped.
1. The belt was locked out.', 'C', null),
 ('A2', '4609612', '2025-02-01', '06', 'NO DYS AWY FRM WRK,NO RSTR ACT', false, 'SLIP OR FALL OF PERSON',
  'Fall to the walkway', 1, 0, 0, 'STRIP, QUARY, OPEN PIT', 'Slipped on ice.', 'C', null),
 ('A3', '0100003', '2026-05-05', '01', 'FATALITY', true, 'MACHINERY', 'Struck by', 1, 0, 0,
  'MILL OPERATION/PREPARATION PLANT', 'Fatal injury.', 'M', null);
insert into production_quarter values
 ('4609612', 2025, 4, '03', 40, 21000, 150000, 'C'), ('4609612', 2026, 1, '03', 41, 22000, 160000, 'C'),
 ('4609612', 2026, 2, '03', 42, 23000, 155000, 'C'), ('4609612', 2026, 3, '03', 40, 22500, 158000, 'C'),
 ('0100003', 2026, 1, '03', 30, 15000, null, 'M'), ('0100003', 2026, 2, '03', 30, 15500, null, 'M');
insert into cfr_section values
 ('75.400', 30, 75, 'Accumulation of combustible materials.',
  'Coal dust, loose coal, and other combustible materials shall be cleaned up and not be permitted to accumulate.',
  '2021-07-01'),
 ('77.1605', 30, 77, 'Loading and haulage equipment; installations.',
  '(a) Cab windows shall be of safety glass.
(b) Mobile equipment shall be equipped with adequate brakes.', '2020-01-01'),
 ('56.14107', 30, 56, 'Moving machine parts.',
  '(a) Moving machine parts shall be guarded to protect persons from contacting gears, sprockets and chains.
(b) Guards shall not be required where the exposed moving parts are at least seven feet away.', '2019-07-01'),
 ('7.7', 29, 7, 'Presentations of other interested persons.',
  'Any interested person may present written data, views or arguments.', '1983-06-01'),
 ('7.7', 30, 7, 'Quality assurance.',
  '(a) Applicants shall maintain the quality of each approved product.
(c) Applicants shall keep the records of their inspections and tests.', '2008-04-01');
insert into document values
 ('https://www.msha.gov/', 'msha', 'fetched', 'page', 'text/html', 52000, 'a1', '2026-11-20 10:00:00', true),
 ('https://www.msha.gov/a.pdf', 'msha', 'fetched', 'pdf', 'application/pdf', 1200000, 'a2', '2026-11-20 10:01:00',
  false),
 ('https://www.osha.gov/', 'osha', 'fetched', 'page', 'text/html', 61000, 'a3', '2026-11-21 08:00:00', true),
 ('https://www.osha.gov/b.png', 'osha', 'skipped', 'other', null, null, null, null, false),
 ('https://arlweb.msha.gov/c.pdf', 'arlweb', 'fetched', 'pdf', 'application/pdf', 1200000, 'a2', '2026-11-19 09:00:00',
  false);
insert into dataset_version values
 ('violations', '47b242d1616ee0d7', 3100000, '2026-11-22 15:40:00', false),
 ('violations', '7943d78c6aba1c23', 3101203, '2026-11-23 15:40:00', true),
 ('mines', 'c4ba9edb535e6bff', 92122, '2026-11-23 15:38:42', true);
"""
NOW = pd.Timestamp("2026-11-23 12:00:00", tz="UTC")


def stream_tables(now: pd.Timestamp) -> dict[str, pd.DataFrame]:
    """What the streaming store's queries return (their columns and types), for stand-in queries."""
    minute = pd.Timedelta(minutes=1)
    return {
        "event_counts": pd.DataFrame({"kind": ["violation", "inspection", "regulation"], "events": [120, 40, 2],
                                      "last_received": [now] * 3}),
        "arrivals": pd.DataFrame({"minute": [now - 2 * minute, now - minute] * 2,
                                  "kind": ["violation", "violation", "inspection", "regulation"],
                                  "events": [30, 25, 4, 1]}),
        "latest_events": pd.DataFrame({
            "received_at": [now, now], "occurred_at": [pd.Timestamp("2026-10-02", tz="UTC"), now],
            "kind": ["violation", "regulation"], "mode": ["replay", "live"],
            "title": ["S&S violation of 30 CFR 75.400 at Highwall Park Surface Mine",
                      "MSHA: Respirable crystalline silica; correction"],
            "mine_id": ["4609612", None], "district": ["C04", None], "state": ["WV", None]}),
        "daily_counts": pd.DataFrame({"period": pd.to_datetime(["2026-11-20", "2026-11-20", "2026-11-21"]),
                                      "district": ["C04", "", "C04"], "events": [10, 2, 12], "serious": [3, 0, 4]}),
        "forecast_districts": pd.DataFrame({"district": ["C04", ""], "days": [3, 1], "anomalies": [1, 0]}),
        "forecasts": pd.DataFrame({"day": pd.to_datetime(["2026-11-19", "2026-11-20", "2026-11-21"]),
                                   "observed": [9.0, 10.0, 30.0], "predicted": [9.5, 9.8, 10.1],
                                   "residual": [-0.5, 0.2, 19.9], "anomaly": [False, False, True],
                                   "method": ["ewma"] * 3}),
        "recent_anomalies": pd.DataFrame({"day": pd.to_datetime(["2026-11-21"]), "district": ["C04"],
                                          "observed": [30.0], "predicted": [10.1], "residual": [19.9],
                                          "method": ["ewma"]}),
        "source_freshness": pd.DataFrame({
            "source": ["federal_register", "msha_ogd"], "checked_at": [now - 5 * minute, now],
            "status": ["ok", "error"], "found": [1, 0], "note": ["", "timeout"], "checks_24h": [24, 12],
            "errors_24h": [0, 1], "found_24h": [3, 0]}),
    }


def build_warehouse(path: Path) -> Path:
    """The warehouse's own tables, macros and marts over ROWS, created in the order the build creates them."""
    with duckdb.connect(str(path)) as connection:
        connection.execute((WAREHOUSE / "schema.sql").read_text(encoding="utf-8"))
        connection.execute(ROWS)
        for signature, body in MACROS.items():
            connection.execute(f"create macro {signature} as {body}")
        connection.execute((WAREHOUSE / "marts.sql").read_text(encoding="utf-8"))
    return path


@pytest.fixture
def stores(tmp_path, monkeypatch) -> Path:
    """The fixture warehouse and an unreachable PostgreSQL; Streamlit's caches start empty."""
    monkeypatch.setenv("SAFESTART_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("SAFESTART_WAREHOUSE_PATH", str(build_warehouse(tmp_path / "warehouse.duckdb")))
    monkeypatch.setenv("SAFESTART_POSTGRES_URL", UNREACHABLE)
    st.cache_data.clear()
    return tmp_path


@pytest.fixture(scope="module")
def built_warehouse(tmp_path_factory) -> Path:
    """A warehouse the real build made of the warehouse tests' sources, where section number 1.1 is in both titles
    and mine 0100001 cites it."""
    target = tmp_path_factory.mktemp("built") / "safestart.duckdb"
    build(write_data_root(tmp_path_factory.mktemp("data")), target)
    return target


def stand_in(name: str, frame: pd.DataFrame):
    def query(*args):
        return data.Table(frame.copy())
    query.__name__ = name  # the app caches queries by name
    return query


@pytest.fixture
def stream_rows(stores, monkeypatch) -> None:
    """Stand-ins for the streaming store's queries, as if PostgreSQL held events, forecasts and checks."""
    for name, frame in stream_tables(pd.Timestamp.now(tz="UTC")).items():
        monkeypatch.setattr(data, name, stand_in(name, frame))


def run_page(page: str, **params: str) -> AppTest:
    app = AppTest.from_file(str(APP), default_timeout=60)
    app.query_params.update({"page": page, **params})
    app.run()
    assert not app.exception, [exception.value for exception in app.exception]
    return app


def text_of(app: AppTest) -> str:
    """Everything a run shows as text: headings, captions, messages, metrics, expander labels and table cells."""
    parts = [element.value for kind in ("title", "subheader", "markdown", "caption", "info", "warning")
             for element in getattr(app, kind)]
    parts += [f"{metric.label}: {metric.value}" for metric in app.metric]
    parts += [expander.label for expander in app.expander]
    parts += [frame.value.to_string() for frame in app.dataframe]
    return "\n".join(map(str, parts))


def chart_count(app: AppTest) -> int:
    return len(app.get("vega_lite_chart"))


def column_formats(app: AppTest) -> dict[str, str]:
    """The number format of every column a run's tables configure, by column name."""
    return {name: column["type_config"]["format"] for frame in app.dataframe
            for name, column in json.loads(frame.proto.columns).items() if "format" in column.get("type_config", {})}


def test_every_page_renders_from_the_warehouse_without_postgres(stores):
    shown = {page: run_page(page) for page in dashboard.PAGES}
    for page in ("Live", "Enforcement", "Sources"):
        text = text_of(shown[page])
        assert "The streaming store at 127.0.0.1:9 is not reachable." in text
        assert "secret-password" not in text
    assert chart_count(shown["Enforcement"]) == 1  # the district history comes from the warehouse
    standards = text_of(shown["Standards"])
    assert "75.400" in standards and "Accumulation of combustible materials" in standards
    assert chart_count(shown["Standards"]) == 2
    assert dashboard.CREW_USE in text_of(shown["Mine brief"])
    sources = text_of(shown["Sources"])
    assert "7943d78c6aba1c23" in sources and "47b242d1616ee0d7" not in sources
    assert "4 URLs downloaded" in sources  # the skipped image has no stored copy
    assert chart_count(shown["Sources"]) == 1


def test_every_page_renders_from_a_warehouse_the_build_made(built_warehouse, monkeypatch):
    monkeypatch.setenv("SAFESTART_WAREHOUSE_PATH", str(built_warehouse))
    monkeypatch.setenv("SAFESTART_POSTGRES_URL", UNREACHABLE)
    st.cache_data.clear()
    for page in dashboard.PAGES:
        run_page(page)
    brief = run_page("Mine brief", mine="0100001")
    with duckdb.connect(str(built_warehouse), read_only=True) as connection:
        top, = connection.execute("select top_sections_12m from mart_mine_scorecard where mine_id = '0100001'"
                                  ).fetchone()
        headings = dict(connection.execute("select section, heading from cfr_section where title = 30").fetchall())
    # Section 1.1, cited once here, ranks where the mart ranks it, with the heading and text of 30 CFR 1.1 alone.
    sections = brief.dataframe[0].value.head(len(top))
    assert "1.1" in top and sections["cfr_section"].tolist() == top
    assert sections["heading"].fillna("").tolist() == [headings.get(section) or "" for section in top]
    assert "**30 CFR" in text_of(brief) and "29 CFR" not in text_of(brief)


def test_every_page_says_what_is_missing_without_any_store(tmp_path, monkeypatch):
    monkeypatch.setenv("SAFESTART_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("SAFESTART_WAREHOUSE_PATH", str(tmp_path / "missing.duckdb"))
    monkeypatch.setenv("SAFESTART_POSTGRES_URL", UNREACHABLE)
    st.cache_data.clear()
    for page in dashboard.PAGES:
        app = run_page(page, mine="4609612")
        assert chart_count(app) == 0
        if page != "Live":
            assert "No warehouse yet" in text_of(app), page
    assert not (tmp_path / "missing.duckdb").exists()  # read-only: nothing is created


def test_a_missing_mart_empties_only_its_own_panel(stores):
    with duckdb.connect(str(stores / "warehouse.duckdb")) as connection:
        connection.execute("drop view mart_ss_trend")
    text = text_of(run_page("Standards"))
    assert "The warehouse has no mart_ss_trend yet" in text
    assert "Accumulation of combustible materials" in text


def test_a_failed_warehouse_query_is_asked_again_and_answers_stay_cached(stores, monkeypatch):
    """A passing failure (out of memory, the file held by another process) clears at the next run, not at the next
    build, when the warehouse's stamp in the cache key changes."""
    answer, calls = data.top_cited_sections, []

    def top_cited_sections(limit: int) -> data.Table:
        calls.append(limit)
        return answer(limit) if len(calls) > 1 else data.Table(problem="The warehouse could not answer: Out of Memory")

    monkeypatch.setattr(data, "top_cited_sections", top_cited_sections)
    tables = [dashboard.warehouse(data.top_cited_sections, 100) for _ in range(3)]
    assert [table.problem for table in tables] == ["The warehouse could not answer: Out of Memory", None, None]
    assert tables[2].frame["cfr_section"].iloc[0] == "75.400" and len(calls) == 2  # the answer came from the cache


def test_a_mine_brief_from_a_name(stores):
    app = run_page("Mine brief", mine="highwall")
    choice = app.selectbox[0]
    assert choice.label == "2 mines match" and choice.value == "4609612"  # the active mine first
    text = text_of(app)
    assert "Highwall Park Surface Mine" in text
    assert ("cover the 12 months from 2025-11-21 to 2026-11-21, the newest records; hours worked and the injury rate "
            "cover 2025 Q4 to 2026 Q3, the newest quarters with reported hours.") in text
    for tile in ("Inspections: 2", "Violations: 3", "S&S violations: 2", "Accidents: 1", "Serious accidents: 1",
                 "Hours worked: 88,500", "Injury rate: 2.26"):
        assert tile in text
    sections = app.dataframe[0].value
    assert list(sections["cfr_section"]) == ["75.400", "77.1605"]
    assert sections["heading"].iloc[0] == "Accumulation of combustible materials."
    assert app.expander[0].label == "2026-09-14 · HANDLING OF MATERIALS · DAYS AWAY FROM WORK ONLY"
    narrative = r"Moving a \$40 roller \*by hand\* when \[it\] slipped." + "\n\n" + r"1\. The belt was locked out."
    assert narrative in text  # shown literally, not as Markdown

    choice.select("4601234").run()
    assert app.subheader[0].value == r"\*\*Highwall Deep Mine No. 2\*\*"  # the name as MSHA writes it, not bold
    text = text_of(app)
    assert "No citations at this mine in the 12 months." in text
    assert "No accidents on record for this mine." in text


def test_a_search_with_more_matches_than_it_lists_says_so(stores, monkeypatch):
    monkeypatch.setattr(data, "MINE_MATCHES", 2)
    choice = run_page("Mine brief", mine="highwall").selectbox[0]
    assert choice.label == "The first 2 matches; type more of the name to narrow them"


def test_the_sources_page_counts_urls_and_distinct_files(stores):
    text = text_of(run_page("Sources"))
    assert "4 URLs downloaded from the MSHA and OSHA websites: 3 distinct files, 0.0 GB." in text


def test_a_mine_brief_from_an_id_as_msha_writes_it(stores):
    app = run_page("Mine brief", mine="01-00003")
    text = text_of(app)
    assert "O'Neal Quarry & Mill" in text
    assert "56.14107" in text and "Moving machine parts" in text
    # 7.7, cited once here, counts once and reads as 30 CFR 7.7, though 29 CFR has a section 7.7 too.
    sections = app.dataframe[0].value.set_index("cfr_section")
    assert sections.loc["7.7", ["heading", "citations"]].tolist() == ["Quality assurance.", 1]
    assert "29 CFR" not in text and "Presentations of other interested persons" not in text
    assert "No mine name or ID matches" in text_of(run_page("Mine brief", mine="no such mine"))


def test_numbers_read_the_same_in_every_browser_language(stream_rows):
    formats = {}
    for page in dashboard.PAGES:
        formats |= column_formats(run_page(page, mine="4609612"))
    assert {"citations", "ss_share", "proposed_penalty_total", "proposed_penalty", "observed", "rows",
            "found_24h"} <= set(formats)
    assert not set(formats.values()) & LOCALE_FORMATS, formats
    # A printf format cannot scale, so the Standards table holds shares in percent and penalty totals in millions.
    standards = run_page("Standards").dataframe[0].value.set_index("cfr_section")
    assert standards.loc["75.400", ["ss_share", "proposed_penalty_total"]].tolist() == pytest.approx(
        [200 / 3, 0.002322])


def test_stream_pages_render_events_forecasts_and_checks(stream_rows):
    live = run_page("Live")
    assert {metric.label: metric.value for metric in live.metric}["Violations"] == "120"
    assert "S&S violation of 30 CFR 75.400" in text_of(live)
    assert chart_count(live) == 1

    enforcement = run_page("Enforcement")
    assert chart_count(enforcement) == 4  # counts over time, by district, the forecast and the district history
    assert "Latest anomalies" in text_of(enforcement)
    assert enforcement.selectbox[0].options == ["C04", "No district"]

    sources = run_page("Sources")
    assert r"The newest check failed for msha\_ogd." in text_of(sources)
    assert sources.dataframe[-1].value["age"].tolist() == ["5 minutes ago", "just now"]


def test_duckdb_spills_into_a_folder_of_this_process_not_beside_the_read_only_warehouse(stores):
    settings = data.warehouse_query("select current_setting('temp_directory') as spill,"
                                    " current_setting('memory_limit') as memory").frame.iloc[0]
    spill = Path(settings["spill"])
    assert spill.parent == Path(tempfile.gettempdir()) and spill.name.endswith(str(os.getpid()))
    assert stores not in spill.parents and settings["memory"] == "192.0 MiB"


def test_the_warehouse_is_read_only_and_problems_become_messages(stores, monkeypatch):
    assert data.warehouse_query("select count(*) as n from mine").frame["n"].item() == 3
    assert "could not answer" in data.warehouse_query("insert into mine (mine_id) values ('1')").problem
    assert "could not answer" in data.warehouse_query("select nope from mine").problem
    assert data.warehouse_query("select * from nope").problem.startswith("The warehouse has no nope yet")
    monkeypatch.setenv("SAFESTART_WAREHOUSE_PATH", str(stores / "absent.duckdb"))
    assert data.top_cited_sections().problem.startswith("No warehouse yet")


def test_warehouse_queries_follow_the_contract(stores):
    assert data.find_mines("46-09612").frame["mine_id"].tolist() == ["4609612"]
    assert data.find_mines("HIGHWALL").frame["mine_id"].tolist() == ["4609612", "4601234"]
    window = data.twelve_months().frame.iloc[0]
    assert (window["since"], window["until"]) == (pd.Timestamp("2025-11-21"), pd.Timestamp("2026-11-21"))
    assert window["rate_last_quarter"] == 2026 * 4 + 2  # 2026 Q3, the newest quarter ending by 2026-11-21
    sections = data.mine_sections("4609612").frame
    assert sections[["cfr_section", "citations", "ss_citations"]].values.tolist() == [["75.400", 2, 2],
                                                                                    ["77.1605", 1, 0]]
    assert sections["heading"].iloc[0] == "Accumulation of combustible materials."
    # Section number 7.7 is in both titles: the quarry's one citation counts once, with title 30's heading and text.
    quarry = data.mine_sections("0100003").frame.set_index("cfr_section")
    assert quarry.loc["7.7", ["heading", "citations", "proposed_penalty"]].tolist() == ["Quality assurance.", 1, 250]
    assert data.section_text("7.7").frame[["title", "heading"]].values.tolist() == [[30, "Quality assurance."]]
    assert data.mine_accidents("4609612", limit=1).frame["accident_date"].tolist() == [pd.Timestamp("2026-09-14")]
    assert data.national_injury_rate("M").frame["year"].tolist() == [2026]
    versions = data.dataset_versions().frame.set_index("dataset")
    assert versions.loc["violations", ["versions", "version"]].tolist() == [2, "7943d78c6aba1c23"]
    assert data.document_counts().frame["documents"].sum() == 4  # URLs
    assert data.document_files().frame.iloc[0].tolist() == [3, 52000 + 1200000 + 61000]  # one PDF behind two URLs
    assert set(data.district_years().frame["district"]) == {"C04", "M3"}


def test_mine_ids_are_read_as_msha_writes_them():
    assert data.mine_id_from("4609612") == "4609612"
    assert data.mine_id_from("46-09612") == "4609612"
    assert data.mine_id_from("100003") == "0100003"
    assert data.mine_id_from("Highwall") is None
    assert data.mine_id_from("123456789") is None


def test_text_from_the_sources_is_shown_literally():
    assert dashboard.md("$40 *by hand* [it] #2 a_b") == r"\$40 \*by hand\* \[it\] \#2 a\_b"
    assert dashboard.md("1. first\n- second\n+ third") == "1\\. first\n\\- second\n\\+ third"
    assert dashboard.md("Part 75 (a)-(c), 1.5 tons") == "Part 75 (a)-(c), 1.5 tons"


def test_an_unreachable_stream_is_named_without_its_password(monkeypatch):
    monkeypatch.setenv("SAFESTART_POSTGRES_URL", UNREACHABLE)
    for table in (data.event_counts("replay"), data.latest_events(), data.source_freshness()):
        assert table.empty and table.problem == "The streaming store at 127.0.0.1:9 is not reachable."
    with pytest.raises(ValueError, match="mode"):
        data.latest_events("backfill")
    with pytest.raises(ValueError, match="grain"):
        data.daily_counts("violation", 30, "hour")
    with pytest.raises(ValueError, match="enforcement kind"):
        data.forecasts("regulation", "", 30)


def test_every_kind_has_a_label_and_a_fixed_color():
    assert set(charts.KIND_ORDER) == set(charts.KIND_LABELS) == set(charts.KIND_COLORS) == KINDS
    assert len(set(charts.KIND_COLORS.values())) == len(KINDS)
    assert set(charts.SERIOUS_LABELS) <= ENFORCEMENT_KINDS


def test_charts_keep_an_entitys_color_whatever_else_is_shown():
    rows = stream_tables(NOW)
    spec = charts.arrivals(rows["arrivals"].query("kind == 'inspection'")).to_dict()
    assert spec["encoding"]["color"]["scale"] == {"domain": ["Inspections"],
                                                  "range": [charts.KIND_COLORS["inspection"]]}
    minutes = spec["encoding"]["x"]["scale"]["domain"]  # every minute keeps its place, quiet ones included
    assert len(minutes) == 15 and minutes[-1] == "11:58" and spec["encoding"]["x"]["axis"]["values"][-1] == "11:58"
    trend = pd.DataFrame({"year": [1999, 2000, 2000], "coal_metal": ["C", "C", "M"], "violations": [3, 5831, 6368],
                          "ss_violations": [0, 2365, 1975], "ss_share": [0.0, 0.41, 0.31]})
    assert charts.ss_trend(trend).data["year"].tolist() == [2000, 2000]  # no share from three stragglers
    layers = charts.forecast(rows["forecasts"], "violation").to_dict()["layer"]
    assert all(layer["encoding"]["color"]["legend"] for layer in layers)  # a None on one layer hides the legend
    for chart in (charts.arrivals(rows["arrivals"]),
                  charts.counts_over_time(rows["daily_counts"], "violation", "day"),
                  charts.counts_over_time(rows["daily_counts"], "inspection", "week"),
                  charts.counts_by_district(rows["daily_counts"], "accident", "last 90 days"),
                  charts.forecast(rows["forecasts"], "violation")):
        assert chart.to_dict()["$schema"].startswith("https://vega.github.io/schema/vega-lite/")


@pytest.fixture
def stream_database():
    """A fresh `stream` schema in TEST_DATABASE_URL, committed, since the dashboard reads it over connections of its
    own, and dropped again afterwards. Yields the URL and a connection in autocommit mode."""
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("set TEST_DATABASE_URL to run the PostgreSQL tests")
    with psycopg.connect(url, autocommit=True) as connection:
        connection.execute("drop schema if exists stream cascade")
        connection.execute((PACKAGE / "stream" / "schema.sql").read_text(encoding="utf-8"))
        try:
            yield url, connection
        finally:
            connection.execute("drop schema stream cascade")


def test_stream_queries_run_on_postgresql(stores, stream_database, monkeypatch):
    url, connection = stream_database
    events = [
        Event.new(kind="violation", key="V1", mode="replay", occurred_at=date(2026, 10, 2), mine_id="4609612",
                  district="C04", state="WV", coal_metal="C", title="S&S violation of 30 CFR 75.400",
                  details={"cfr_section": "75.400", "sig_sub": True}),
        Event.new(kind="inspection", key="E1", mode="replay", occurred_at=date(2026, 10, 1), mine_id="4609612",
                  district="C04", state="WV", coal_metal="C", title="Regular inspection at Highwall Park"),
        Event.new(kind="regulation", key="2026-25001", mode="live", occurred_at="2026-11-23T14:05:00-05:00",
                  title="MSHA: Respirable crystalline silica; correction"),
    ]
    for offset, event in enumerate(events):
        connection.execute(
            "insert into stream.events values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, %s)",
            (event.event_id, event.schema_version, event.kind, event.mode, event.occurred_at, event.emitted_at,
             event.mine_id, event.district, event.state, event.coal_metal, event.title, Jsonb(event.details),
             Jsonb(event.source), event.topic, offset))
    connection.execute("""
        insert into stream.daily_counts (day, kind, district, events, serious) values
          ('2026-10-01', 'violation', 'C04', 5, 2), ('2026-10-02', 'violation', 'C04', 7, 3),
          ('2026-10-02', 'violation', '', 1, 0), ('2026-10-09', 'violation', 'C04', 4, 1)""")
    connection.execute("""
        insert into stream.forecasts (day, kind, district, observed, predicted, residual, anomaly, method) values
          ('2026-10-01', 'violation', 'C04', 5, 6, -1, false, 'ewma'),
          ('2026-10-02', 'violation', 'C04', 17, 6.5, 10.5, true, 'ewma')""")
    connection.execute("""
        insert into stream.source_checks (source, checked_at, status, found, note) values
          ('federal_register', now() - interval '2 hours', 'ok', 2, ''),
          ('federal_register', now() - interval '5 minutes', 'unchanged', 0, ''),
          ('msha_ogd', now() - interval '30 hours', 'error', 0, 'timeout')""")
    monkeypatch.setenv("SAFESTART_POSTGRES_URL", url)

    assert data.event_counts().frame.set_index("kind")["events"].to_dict() == {
        "violation": 1, "inspection": 1, "regulation": 1}
    assert data.event_counts("live").frame["kind"].tolist() == ["regulation"]
    assert data.latest_events("replay", 5).frame["kind"].tolist() == ["inspection", "violation"]
    assert data.arrivals().frame["events"].sum() == 3
    weekly = data.daily_counts("violation", 30, "week").frame
    assert weekly["events"].sum() == 17 and str(weekly["period"].dtype).startswith("datetime64")
    assert data.forecast_districts("violation").frame["anomalies"].tolist() == [1]
    assert data.forecasts("violation", "C04", 30).frame["anomaly"].tolist() == [False, True]
    assert data.recent_anomalies("violation").frame["observed"].tolist() == [17.0]
    freshness = data.source_freshness().frame.set_index("source")
    assert freshness.loc["federal_register", ["status", "checks_24h", "found_24h"]].tolist() == ["unchanged", 2, 2]
    assert freshness.loc["msha_ogd", ["status", "errors_24h"]].tolist() == ["error", 0]
    read_only = data.stream_query(
        "insert into stream.source_checks (source, checked_at, status) values ('x', now(), 'ok')")
    assert "could not answer" in read_only.problem

    st.cache_data.clear()
    for page in ("Live", "Enforcement", "Sources"):
        assert "not reachable" not in text_of(run_page(page))
