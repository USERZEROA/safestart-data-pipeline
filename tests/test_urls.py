from safestart_pipeline.crawl.urls import extension, is_media, normalize

BASE = "https://www.example.test/a/x/"


def test_normalize_resolves_relative_links_and_drops_fragments():
    assert normalize("../b/c.html#top", BASE) == "https://www.example.test/a/b/c.html"
    assert normalize("/q?x=1&y=2", BASE) == "https://www.example.test/q?x=1&y=2"


def test_normalize_lowercases_host_and_drops_default_ports():
    assert normalize("HTTPS://WWW.Example.TEST:443/Path") == "https://www.example.test/Path"
    assert normalize("http://example.test:80/") == "http://example.test/"
    assert normalize("http://example.test:8080/x") == "http://example.test:8080/x"


def test_normalize_escapes_unsafe_characters_once():
    assert normalize("/a b.pdf", BASE) == "https://www.example.test/a%20b.pdf"
    assert normalize("/a%20b.pdf", BASE) == "https://www.example.test/a%20b.pdf"


def test_normalize_rejects_everything_that_is_not_a_web_url():
    for value in ("mailto:a@example.test", "JavaScript:void(0)", "tel:123", "", "   ", "ftp://example.test/f"):
        assert normalize(value, BASE) is None


def test_extension_and_media():
    assert extension("https://x.test/a/B.PDF") == ".pdf"
    assert extension("https://x.test/a/") == ""
    assert extension("https://x.test/.hidden") == ""
    assert is_media("https://x.test/logo.PNG")
    assert not is_media("https://x.test/doc.pdf")
    assert not is_media("https://x.test/page")
