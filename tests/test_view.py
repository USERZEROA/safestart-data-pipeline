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
    assert "--from:9;--to:10" in view.odometer(1000, 999)
    assert "--from:0;--to:9" in view.odometer(999, 1000)
    assert view.odometer(1_000_001, 1_000_000) == "1.00M"


def test_tile_markup_has_an_exact_count_and_escapes_the_source_text():
    item = view.Tile("page_change", "Page changes", "#0ca30c", 1_234_567, None, "+1 in 24 h", "accent",
                     [("2026-11-23", 0), ("2026-11-24", 1)], ["OSHA & MSHA"], note='1,234,567 stored. <note> "')
    html = view.kpi_row([("sources", "safety.sources", "live watchers", [item])])
    assert 'aria-label="Page changes: 1.23M. 1,234,567 stored.' in html
    assert 'title="2026-11-24: 1"' in html and '--h:0.000' in html
    assert 'OSHA &amp; MSHA' in html and '&lt;note&gt; &quot;' in html
    assert "ss-odo" in html and "ss-anim changed" not in html and "ss-new" not in html
    assert "ss-odo" not in view.tile(replace(item, previous=item.value))
    assert "ss-anim changed" not in view.tile(replace(item, previous=item.value, fresh=1))
    assert "ss-anim changed" not in view.tile(replace(item, previous=item.value, value=None))
    assert "ss-anim changed" in view.tile(replace(item, previous=1_234_566))
    assert "Trend unavailable" in view.tile(replace(item, days=[]))


def test_event_titles_urls_and_dates_are_literal_and_new_rows_alone_move():
    row = view.EventRow("Page changes", "#0ca30c", 'A < B & "C"', ["2026-11-24"], "live", "1 h ago",
                        "2026-11-24 11:05:00 UTC", link='https://www.osha.gov/?a=1&b=2')
    html = view.event_list([row])
    assert 'href="https://www.osha.gov/?a=1&amp;b=2"' in html and 'A &lt; B &amp; &quot;C&quot;' in html
    assert 'title="2026-11-24 11:05:00 UTC"' in html and "ss-anim" not in html
    changed = view.event_list([replace(row, fresh=True, link="javascript:void(0)")])
    assert "fresh ss-anim" in changed and "href=" not in changed


def test_a_status_table_escapes_every_cell_keeps_a_notes_lines_and_dots_its_status():
    columns = (view.Column("Source"), view.Column("Status", "nowrap"), view.Column("Requests", "num"),
               view.Column("Note", "note"))
    note = "sitemap not read: <html> & more" + chr(10) + "the host served an HTML page"
    html = view.table(columns, [["msha", view.Cell("refused", "warning"), "3,000", note]], label='Runs "now"',
                      minimum=700)
    assert 'aria-label="Runs &quot;now&quot;"' in html and "--min:700px" in html
    assert 'class="num">Requests</th>' in html
    # each cell carries its column's label for a phone's card, and holds its whole value in one child
    assert '<td role="cell" class="num" data-label="Requests"><span>3,000</span></td>' in html
    assert '<span><span class="ss-dot warning" aria-hidden="true"></span>refused</span></td>' in html
    assert ('data-label="Note"><span>sitemap not read: &lt;html&gt; &amp; more<br>the host served an HTML page'
            '</span></td>') in html
    with pytest.raises(ValueError):  # a row with a missing cell is a mistake, not a shorter row
        view.table(columns, [["msha", "ok"]], label="Runs")
