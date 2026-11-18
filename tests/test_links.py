from safestart_pipeline.crawl.links import extract_links

PAGE = b"""<html><head><base href="https://www.example.test/docs/"></head><body>
<a href="a.html">A</a> <a href="/b#part">B</a> <a href="mailto:someone@example.test">mail</a>
<a href="a.html">again</a> <iframe src="https://video.example.test/embed/1"></iframe>
<map><area href="c.pdf"></map> <a>no href</a> <a href="javascript:void(0)">js</a>
</body></html>"""


def test_links_resolve_against_base_and_skip_non_web_targets():
    assert extract_links(PAGE, "https://www.example.test/index.html") == [
        "https://www.example.test/docs/a.html",
        "https://www.example.test/b",
        "https://video.example.test/embed/1",
        "https://www.example.test/docs/c.pdf",
    ]


def test_links_without_base_use_the_page_url():
    page = b'<html><body><a href="next">next</a></body></html>'
    assert extract_links(page, "https://www.example.test/dir/page") == ["https://www.example.test/dir/next"]


def test_empty_or_binary_content_has_no_links():
    assert extract_links(b"", "https://www.example.test/") == []
    assert extract_links(b"   ", "https://www.example.test/") == []
