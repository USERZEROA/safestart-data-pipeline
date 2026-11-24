"""The dashboard's plain HTML before Streamlit sanitizes it."""

from dataclasses import replace

import pytest

from safestart_pipeline.dashboard import view


@pytest.mark.parametrize("value, expected", [(None, "–"), (0, "0"), (999_999, "999,999"), (3_123_456, "3.12M")])
def test_tile_numbers(value, expected):
    assert view.whole(value) == expected


def test_only_changed_digits_roll_on_a_new_observation():
    assert view.odometer(1234, None) == view.odometer(1234, 1234) == "1,234"
    assert view.odometer(1234, 1233).count('class="ss-d"') == 1
    assert "--from:3;--to:4" in view.odometer(1234, 1233)
    assert "--from:0;--to:1" in view.odometer(1000, 999)
    assert "--from:0;--to:9" in view.odometer(999, 1000)
    assert view.odometer(1_000_001, 1_000_000) == "1.00M"


def test_tile_markup_has_an_exact_count_and_escapes_the_source_text():
    item = view.Tile("page_change", "Page changes", "#0ca30c", 1_234_567, None, "+1 in 24 h", "accent",
                     [("2026-11-23", 0), ("2026-11-24", 1)], ["OSHA & MSHA"], note='1,234,567 stored. <note> "')
    html = view.kpi_row([("sources", "safety.sources", "live watchers", [item])])
    assert 'aria-label="Page changes: 1.23M. 1,234,567 stored.' in html
    assert 'title="2026-11-24: 1"' in html and '--h:0.000' in html
    assert 'OSHA &amp; MSHA' in html and '&lt;note&gt; &quot;' in html
    assert "ss-anim" not in html and "ss-anim changed" in view.tile(replace(item, previous=1_234_566))
    assert "Trend unavailable" in view.tile(replace(item, days=[]))


def test_event_titles_urls_and_dates_are_literal_and_new_rows_alone_move():
    row = view.EventRow("Page changes", "#0ca30c", 'A < B & "C"', ["2026-11-24"], "live", "1 h ago",
                        "2026-11-24 11:05:00 UTC", link='https://www.osha.gov/?a=1&b=2')
    html = view.event_list([row])
    assert 'href="https://www.osha.gov/?a=1&amp;b=2"' in html and 'A &lt; B &amp; &quot;C&quot;' in html
    assert 'title="2026-11-24 11:05:00 UTC"' in html and "ss-anim" not in html
    changed = view.event_list([replace(row, fresh=True, link="javascript:void(0)")])
    assert "fresh ss-anim" in changed and "href=" not in changed
