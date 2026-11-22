"""URL normalization and the file-type rules every crawl shares."""

from __future__ import annotations

import re
import string
from urllib.parse import quote, urljoin, urlsplit, urlunsplit

# Page assets (style sheets, scripts, fonts) are the site's own machinery, not content: they are recorded as skipped
# "asset" and never downloaded. Images, audio and video linked from the sites are content and are downloaded like any
# other file, so each copy is complete (owner, 2026-11-18).
ASSET_EXTENSIONS = frozenset(".css .js .mjs .map .woff .woff2 .ttf .otf .eot".split())
# Extensions of downloadable files. Anything else is a web page that can link onward, including what follows a dot in
# a section's name ('/1910.1025', '/1910.7(b)', '/1910.1001AppC').
FILE_EXTENSIONS = ASSET_EXTENSIONS | frozenset(
    ".pdf .doc .docx .docm .dot .rtf .txt .odt .wpd .xls .xlsx .xlsm .csv .ods .ppt .pptx .pps .ppsx .odp .xml .json"
    " .zip .gz .tgz .tar .7z .rar .exe .msi .dat .mdb .accdb .kml .kmz .dwg .swf .eps .ai .psd .jpg .jpeg .png .gif"
    " .bmp .tif .tiff .svg .webp .ico .mp3 .wav .m4a .wma .aac .ogg .mp4 .m4v .mov .wmv .avi .flv .webm .mpg .mpeg"
    " .mobi .epub".split()
)

_SAFE_PATH = "/%:@!$&'()*+,;=-._~"
_SAFE_QUERY = _SAFE_PATH + "?"
_NOT_WEB = ("mailto:", "tel:", "javascript:", "data:", "ftp:", "file:")
_UNRESERVED = frozenset(string.ascii_letters + string.digits + "-._~")
_ESCAPE = re.compile(r"%([0-9A-Fa-f]{2})")


def _tidy_escapes(component: str) -> str:
    """One spelling per URL (RFC 3986, 6.2.2): an escaped unreserved character is decoded ('%2E' and '%2e' are '.'),
    and the escapes that remain are written with upper-case hex digits ('%2f' is '%2F')."""

    def fix(match: re.Match[str]) -> str:
        char = chr(int(match.group(1), 16))
        return char if char in _UNRESERVED else "%" + match.group(1).upper()

    return _ESCAPE.sub(fix, component)


def _remove_dot_segments(path: str) -> str:
    """RFC 3986, 5.2.4, for an absolute path: '.' segments dropped, each '..' removing the segment before it."""
    segments = path.split("/")[1:]
    kept: list[str] = []
    for segment in segments:
        if segment == "..":
            if kept:
                kept.pop()
        elif segment != ".":
            kept.append(segment)
    result = "/" + "/".join(kept)
    if segments and segments[-1] in (".", "..") and not result.endswith("/"):
        result += "/"
    return result


def normalize(url: str, base: str | None = None) -> str | None:
    """An absolute http(s) URL with no fragment, no default port, unsafe characters escaped and escapes in one spelling;
    None otherwise."""
    raw = url.strip()
    if not raw or raw.lower().startswith(_NOT_WEB):
        return None
    try:
        parts = urlsplit(urljoin(base, raw) if base else raw)
        port = parts.port
    except ValueError:
        return None
    if parts.scheme not in ("http", "https") or not parts.hostname:
        return None
    host = parts.hostname.lower().rstrip(".")
    netloc = host if port in (None, 80, 443) else f"{host}:{port}"
    quoted = quote(parts.path or "/", safe=_SAFE_PATH)
    path = _tidy_escapes(quoted)
    if path != quoted and any(segment in (".", "..") for segment in path.split("/")):
        path = _remove_dot_segments(path)  # a dot segment that arrived escaped ('/%2E%2E/')
    query = _tidy_escapes(quote(parts.query, safe=_SAFE_QUERY))
    return urlunsplit((parts.scheme, netloc, path, query, ""))


def extension(url: str) -> str:
    """The lower-case extension of the last path segment ('.pdf'), or '' when it has none."""
    last = urlsplit(url).path.rsplit("/", 1)[-1]
    dot = last.rfind(".")
    return last[dot:].lower() if dot > 0 else ""


def is_asset(url: str) -> bool:
    return extension(url) in ASSET_EXTENSIONS


def is_page(url: str) -> bool:
    return extension(url) not in FILE_EXTENSIONS
