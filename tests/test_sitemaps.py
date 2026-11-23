import gzip

import pytest

from safestart_pipeline.crawl.sitemaps import parse_sitemap

URLSET = b"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc> https://www.example.test/a </loc><lastmod>2026-11-01</lastmod></url>
  <!-- a comment -->
  <url><loc>https://www.example.test/b</loc></url>
  <url><lastmod>2026-11-02</lastmod></url>
</urlset>"""

INDEX = b"""<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>https://www.example.test/sitemap.xml?page=1</loc><lastmod>2026-11-03</lastmod></sitemap>
  <sitemap><loc>https://www.example.test/sitemap.xml?page=2</loc></sitemap>
</sitemapindex>"""


def test_urlset_plain_and_gzip():
    for content in (URLSET, gzip.compress(URLSET)):
        sitemap = parse_sitemap(content)
        assert sitemap.kind == "urlset"
        assert [(entry.loc, entry.lastmod) for entry in sitemap.entries] == [
            ("https://www.example.test/a", "2026-11-01"),
            ("https://www.example.test/b", None),
        ]


def test_index():
    sitemap = parse_sitemap(INDEX)
    assert sitemap.kind == "index"
    assert [entry.loc for entry in sitemap.entries] == [
        "https://www.example.test/sitemap.xml?page=1",
        "https://www.example.test/sitemap.xml?page=2",
    ]


def test_rejects_documents_that_are_not_sitemaps():
    with pytest.raises(ValueError):
        parse_sitemap(b"<html><body>Access denied</body></html>")
    with pytest.raises(ValueError):
        parse_sitemap(b"not xml at all")


def corrupt_deflate(packed: bytes) -> bytes:
    damaged = bytearray(packed)
    damaged[12:20] = b"\xff" * 8  # inside the compressed stream, after the 10-byte header
    return bytes(damaged)


@pytest.mark.parametrize("damage", [lambda packed: packed[:-10], lambda packed: packed[:2] + b"not gzip at all",
                                    corrupt_deflate], ids=["truncated", "bad header", "corrupt stream"])
def test_a_damaged_gzip_sitemap_is_a_value_error_like_any_other_unreadable_one(damage):
    """gzip raises EOFError, BadGzipFile (an OSError) or zlib.error here; the readers catch ValueError alone."""
    with pytest.raises(ValueError, match="not a readable gzip sitemap"):
        parse_sitemap(damage(gzip.compress(URLSET)))


def test_external_entities_are_never_read(tmp_path):
    secret = tmp_path / "secret.txt"
    secret.write_text("TOP-SECRET", encoding="utf-8")
    hostile = (
        '<?xml version="1.0"?><!DOCTYPE urlset [<!ENTITY x SYSTEM "' + secret.as_uri() + '">]>'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"><url><loc>https://www.example.test/&x;</loc></url>'
        "</urlset>"
    ).encode()
    try:
        sitemap = parse_sitemap(hostile)
    except ValueError:
        return
    assert all("TOP-SECRET" not in entry.loc for entry in sitemap.entries)
