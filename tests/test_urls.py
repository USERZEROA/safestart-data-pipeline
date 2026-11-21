from safestart_pipeline.crawl.urls import extension, is_asset, is_page, normalize

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


def test_normalize_gives_each_url_one_spelling():
    # RFC 3986: escaped unreserved characters decoded, other escapes in upper case, an escaped dot segment resolved.
    assert normalize("/index%2Ephp/news", BASE) == normalize("/index%2ephp/news", BASE) == "https://www.example.test/index.php/news"
    assert normalize("/%7Euser/%41-%5f.pdf", BASE) == "https://www.example.test/~user/A-_.pdf"
    assert normalize("/a%2fb?x=%26%3d&y=%2E", BASE) == "https://www.example.test/a%2Fb?x=%26%3D&y=."
    assert normalize("/a/%2E%2E/b", BASE) == "https://www.example.test/b"
    assert normalize("/a/b/%2e", BASE) == "https://www.example.test/a/b/"
    assert normalize("/%2E%2E", BASE) == "https://www.example.test/"


def test_normalize_rejects_everything_that_is_not_a_web_url():
    for value in ("mailto:a@example.test", "JavaScript:void(0)", "tel:123", "", "   ", "ftp://example.test/f"):
        assert normalize(value, BASE) is None


def test_extension_and_page_assets():
    assert extension("https://x.test/a/B.PDF") == ".pdf"
    assert extension("https://x.test/a/") == ""
    assert extension("https://x.test/.hidden") == ""
    assert is_asset("https://x.test/theme/site.CSS") and is_asset("https://x.test/app.js")
    assert not is_asset("https://x.test/logo.png")  # images, audio and video are content
    assert not is_asset("https://x.test/briefing.wav")
    assert not is_asset("https://x.test/doc.pdf")
    assert not is_asset("https://x.test/page")


def test_pages_and_files():
    for url in ("https://x.test/a", "https://x.test/a/", "https://x.test/a.html", "https://x.test/old.aspx",
                "https://x.test/standardnumber/1910/1910.1025"):
        assert is_page(url), url
    for url in ("https://x.test/a.pdf", "https://x.test/deck.PPTX", "https://x.test/photo.jpg", "https://x.test/data.zip"):
        assert not is_page(url), url
