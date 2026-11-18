"""Outgoing links of an HTML page, resolved against the page URL and any <base href>."""

from __future__ import annotations

from lxml import etree, html

from .urls import normalize

LINK_ATTRIBUTES = {"a": "href", "area": "href", "iframe": "src", "frame": "src", "embed": "src", "object": "data"}


def extract_links(content: bytes, page_url: str) -> list[str]:
    """Distinct absolute http(s) links in document order (navigation included; the link graph keeps every edge)."""
    if not content.strip():
        return []
    try:
        document = html.fromstring(content)
    except (etree.ParserError, ValueError):
        return []
    base = page_url
    for element in document.iter("base"):
        href = element.get("href")
        if href:
            base = normalize(href, page_url) or page_url
            break
    found: dict[str, None] = {}
    for element in document.iter(*LINK_ATTRIBUTES):
        value = element.get(LINK_ATTRIBUTES[element.tag])
        if value:
            url = normalize(value, base)
            if url:
                found.setdefault(url, None)
    return list(found)
