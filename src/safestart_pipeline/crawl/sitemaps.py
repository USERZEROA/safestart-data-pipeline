"""Sitemap index and URL-set parsing (sitemaps.org 0.9), plain or gzip, with entity expansion and network access off.

Anything that is not a sitemap raises ValueError, a damaged gzip file included (gzip raises EOFError, OSError or
zlib.error for one), so a reader that skips a bad sitemap and goes on catches one exception for all of them.
"""

from __future__ import annotations

import gzip
import zlib
from dataclasses import dataclass

from lxml import etree

GZIP_MAGIC = bytes([0x1F, 0x8B])


@dataclass(frozen=True)
class SitemapEntry:
    loc: str
    lastmod: str | None


@dataclass(frozen=True)
class Sitemap:
    kind: str  # "index" or "urlset"
    entries: list[SitemapEntry]


def _parser() -> etree.XMLParser:
    return etree.XMLParser(resolve_entities=False, no_network=True, load_dtd=False)


def parse_sitemap(content: bytes) -> Sitemap:
    """Entries of a sitemap index (child sitemaps) or a URL set (pages). Raises ValueError for anything else."""
    if content[:2] == GZIP_MAGIC:
        try:
            content = gzip.decompress(content)
        except (OSError, EOFError, zlib.error) as problem:
            raise ValueError(f"not a readable gzip sitemap: {problem}") from problem
    try:
        root = etree.fromstring(content, parser=_parser())
    except etree.XMLSyntaxError as problem:
        raise ValueError(f"not well-formed XML: {problem}") from problem
    kind = etree.QName(root).localname
    if kind not in ("sitemapindex", "urlset"):
        raise ValueError(f"not a sitemap: <{kind}>")
    child = "sitemap" if kind == "sitemapindex" else "url"
    entries: list[SitemapEntry] = []
    for item in root:
        if not isinstance(item.tag, str) or etree.QName(item).localname != child:
            continue
        loc = lastmod = None
        for field in item:
            if not isinstance(field.tag, str):
                continue
            name = etree.QName(field).localname
            if name == "loc":
                loc = (field.text or "").strip()
            elif name == "lastmod":
                lastmod = (field.text or "").strip() or None
        if loc:
            entries.append(SitemapEntry(loc, lastmod))
    return Sitemap("index" if kind == "sitemapindex" else "urlset", entries)
