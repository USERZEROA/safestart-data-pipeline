"""URL normalization and the file-type rules every crawl shares."""

from __future__ import annotations

from urllib.parse import quote, urljoin, urlsplit, urlunsplit

# Page assets and media are not part of the regulatory corpus: they are recorded as skipped and never downloaded.
MEDIA_EXTENSIONS = frozenset(
    (
        ".jpg .jpeg .png .gif .svg .webp .avif .ico .bmp .tif .tiff .heic "
        ".mp4 .m4v .mov .avi .wmv .webm .mpg .mpeg .flv .swf .mp3 .wav .m4a .ogg .wma "
        ".css .js .map .woff .woff2 .ttf .otf .eot"
    ).split()
)

_SAFE_PATH = "/%:@!$&'()*+,;=-._~"
_SAFE_QUERY = _SAFE_PATH + "?"
_NOT_WEB = ("mailto:", "tel:", "javascript:", "data:", "ftp:", "file:")


def normalize(url: str, base: str | None = None) -> str | None:
    """An absolute http(s) URL with no fragment, no default port and unsafe characters escaped; None otherwise."""
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
    path = quote(parts.path or "/", safe=_SAFE_PATH)
    query = quote(parts.query, safe=_SAFE_QUERY)
    return urlunsplit((parts.scheme, netloc, path, query, ""))


def extension(url: str) -> str:
    """The lower-case extension of the last path segment ('.pdf'), or '' when it has none."""
    last = urlsplit(url).path.rsplit("/", 1)[-1]
    dot = last.rfind(".")
    return last[dot:].lower() if dot > 0 else ""


def is_media(url: str) -> bool:
    return extension(url) in MEDIA_EXTENSIONS
