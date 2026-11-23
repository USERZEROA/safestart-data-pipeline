import duckdb
import pytest

from safestart_pipeline import definitions as d


@pytest.mark.parametrize(("cited", "section"), [
    ("56.14107(a)", "56.14107"),
    ("75.1100-2(b)", "75.1100-2"),
    ("77.216-3", "77.216-3"),
    ("56.5001(a)/.5005", "56.5001"),
    (" 75.400 ", "75.400"),
    ("104(d)(1)", None),
    ("", None),
    (None, None),
])
def test_cited_section_drops_the_paragraph_in_python_and_sql(cited, section):
    assert d.cfr_section(cited) == section
    sql = duckdb.connect().execute(f"select {d.cfr_section_sql('$cited')}", {"cited": cited}).fetchone()[0]
    assert sql == section


@pytest.mark.parametrize(("degree", "fatal", "serious", "injury"), [
    ("00", False, False, False),
    ("01", True, True, True),
    ("02", False, True, True),
    ("03", False, True, True),
    ("05", False, True, True),
    ("06", False, False, True),
    ("07", False, False, False),
    ("10", False, False, False),
    (None, False, False, False),
])
def test_degree_rules_nest_and_agree_with_sql(degree, fatal, serious, injury):
    assert (d.is_fatal(degree), d.is_serious(degree), d.is_injury(degree)) == (fatal, serious, injury)
    row = duckdb.connect().execute(f"select {d.serious_sql('$degree')}, {d.injury_sql('$degree')}",
                                   {"degree": degree}).fetchone()
    assert row == (serious, injury)


def test_every_serious_degree_is_an_injury_degree():
    assert set(d.SERIOUS_DEGREES) <= set(d.INJURY_DEGREES)
    assert d.FATAL_DEGREE in d.SERIOUS_DEGREES


@pytest.mark.parametrize(("mine", "own", "expected"), [
    ("C", "M", "C"),
    ("M", "C", "M"),
    (None, "C", "C"),
    ("?", "M", "M"),
    (None, None, None),
    (None, "X", None),
])
def test_commodity_prefers_the_mines_flag(mine, own, expected):
    assert d.commodity(mine, own) == expected
    query = f"select {d.commodity_sql('$mine', '$own')}"
    assert duckdb.connect().execute(query, {"mine": mine, "own": own}).fetchone()[0] == expected
