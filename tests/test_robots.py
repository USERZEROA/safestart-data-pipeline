from safestart_pipeline.config import USER_AGENT
from safestart_pipeline.crawl.robots import Robots

# The same rule shapes as www.osha.gov/robots.txt on 2026-11-17.
OSHA_LIKE = """User-agent: *
Crawl-delay: 10
Allow: /core/*.css$
Disallow: /core/
Disallow: /search/
Disallow: /data/
Disallow: /*?
Disallow: /*&
Sitemap: https://www.example.test/sitemap.xml
"""


def test_patterns_delay_and_sitemaps():
    robots = Robots(200, OSHA_LIKE, USER_AGENT)
    assert robots.allowed("https://www.example.test/laws-regs/regulations/standardnumber/1910")
    assert not robots.allowed("https://www.example.test/search/results")
    assert not robots.allowed("https://www.example.test/data/inspection")
    assert not robots.allowed("https://www.example.test/page?x=1")
    assert robots.allowed("https://www.example.test/core/site.css")
    assert not robots.allowed("https://www.example.test/core/site.js")
    assert robots.crawl_delay() == 10.0
    assert robots.sitemaps() == ["https://www.example.test/sitemap.xml"]


def test_missing_robots_allows_everything():
    robots = Robots(404, "", USER_AGENT)
    assert robots.allowed("https://www.example.test/anything?x=1")
    assert robots.crawl_delay() is None and not robots.unreachable


def test_unreachable_robots_disallows_everything():
    for status in (503, None):
        robots = Robots(status, "", USER_AGENT)
        assert robots.unreachable
        assert not robots.allowed("https://www.example.test/")
