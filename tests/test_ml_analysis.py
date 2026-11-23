from __future__ import annotations

from datetime import date

import polars as pl

from ml_fixtures import accident, mine, production, violation, write_lake
from safestart_pipeline.ml.analysis import write_tables


def test_the_tables_for_the_docs(tmp_path):
    # Mine 0300001 is missing from the mines dataset, so its records' own flag (M) stands in; the other records that
    # carry a flag unlike their mine's (v6, a2 and the 2002Q1 report) count under the mine's.
    lake = write_lake(tmp_path, {
        "mines": [mine("0100001"), mine("0200001", "M")],
        "mines_prod_quarterly": production("0100001", (2001, 1), (2001, 4))
        + production("0100001", (2002, 1), (2002, 1), coal_metal="M")
        + production("0100001", (2002, 2), (2002, 4))
        + production("0200001", (2002, 1), (2002, 2), 2000, coal_metal="M")
        + production("0300001", (2002, 1), (2002, 1), 500, coal_metal="M"),
        "violations": [
            violation("v1", "0100001", date(2002, 5, 10), ss="Y", section="75.400(a)", penalty=1000.0),
            violation("v2", "0100001", date(2002, 6, 1), section="75.400", penalty=200.6),
            violation("v3", "0100001", date(2001, 2, 1), section="75.202(a)"),
            violation("v4", "0200001", date(2002, 3, 1), ss="Y", section="56.14107(a)", coal_metal="M"),
            violation("v5", "0200001", date(1999, 3, 1), ss="Y", section="56.14107(a)", coal_metal="M"),
            violation("v6", "0100001", date(2002, 7, 1), section="56.5001(a)/.5005", coal_metal="M"),
            violation("v7", "0300001", date(2002, 8, 1), ss="Y", section="104(d)(1)", coal_metal="M"),
        ],
        "accidents": [
            accident("a1", "0100001", date(2002, 5, 20), "03"),  # days away, case still open: serious by its degree
            accident("a2", "0100001", date(2002, 9, 1), "06", days_lost=0, coal_metal="M"),
            accident("a3", "0100001", date(2002, 9, 2), "03", contractor="C1"),  # contractor hours are not reported
            accident("a4", "0100001", date(2002, 9, 3), "00"),  # an accident without injury
            accident("a5", "0100001", date(2003, 1, 3), "01"),  # after the newest production report
            accident("a6", "0100001", date(2002, 10, 1), "07", days_lost=5),  # an illness with lost days: no injury
            accident("a7", "0300001", date(2002, 4, 1), "05", coal_metal="M"),
        ],
    })
    rows = write_tables(lake, tmp_path / "analysis")
    assert rows == {"ss_share_by_year": 3, "injury_rate_by_year": 3, "top_cited_sections": 4}

    ss = pl.read_csv(tmp_path / "analysis" / "ss_share_by_year.csv")
    assert ss.rows() == [(2001, "C", 1, 0, 0.0), (2002, "C", 3, 1, 0.3333), (2002, "M", 2, 2, 1.0)]  # 1999 left out
    rates = pl.read_csv(tmp_path / "analysis" / "injury_rate_by_year.csv")
    assert rates.rows() == [
        (2001, "C", 0, 0, 4000, 0.0, 0.0),
        (2002, "C", 2, 1, 4000, 100.0, 50.0),  # 2 injuries in 4,000 hours: 100 per 200,000 hours
        (2002, "M", 1, 1, 4500, 44.44, 44.44),
    ]
    # Read as text: a CSV reader would otherwise take section 75.400 for the number 75.4. v7 names no section, and
    # 75.400's penalties add up to $1,200.60, shown as whole dollars.
    sections = pl.read_csv(tmp_path / "analysis" / "top_cited_sections.csv",
                           schema_overrides={"cfr_section": pl.String})
    assert sections.select("cfr_section", "citations", "ss_citations", "mines", "proposed_penalty").rows() == [
        ("56.14107", 2, 2, 1, 0), ("75.400", 2, 1, 1, 1201), ("56.5001", 1, 0, 1, 0), ("75.202", 1, 0, 1, 0)]
    assert "| 75.400" in (tmp_path / "analysis" / "tables.md").read_text(encoding="utf-8")
