"""One GET: conditional headers, no automatic redirects, a size cap enforced while streaming, and the body stored by its
SHA-256 so identical files are kept once."""

from __future__ import annotations

import hashlib
import os
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from pathlib import Path

import httpx

MAX_BYTES = 50 * 1024 * 1024
MAX_SECONDS = 600.0
SAMPLE_BYTES = 64 * 1024


@dataclass
class FetchResult:
    status: int | None
    content_type: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    location: str | None = None
    retry_after: float | None = None
    sha256: str | None = None
    size: int | None = None
    path: Path | None = None
    too_large: bool = False
    error: str | None = None
    sample: bytes = b""


class ObjectStore:
    """Content-addressed files: <root>/ab/cd/<sha256>."""

    def __init__(self, root: Path):
        self.root = root
        self.tmp = root / "tmp"
        self.tmp.mkdir(parents=True, exist_ok=True)
        for leftover in self.tmp.iterdir():  # partial downloads from an interrupted run
            leftover.unlink(missing_ok=True)

    def path_for(self, digest: str) -> Path:
        return self.root / digest[:2] / digest[2:4] / digest

    def temp_path(self) -> Path:
        return self.tmp / uuid.uuid4().hex

    def commit(self, temp: Path, digest: str) -> Path:
        final = self.path_for(digest)
        if final.exists():
            temp.unlink(missing_ok=True)
        else:
            final.parent.mkdir(parents=True, exist_ok=True)
            os.replace(temp, final)
        return final


def parse_retry_after(value: str | None, now: Callable[[], float] = time.time) -> float | None:
    """Seconds to wait from a Retry-After header (delta-seconds or HTTP date); None when absent or unreadable."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        return max(0.0, parsedate_to_datetime(value).timestamp() - now())
    except (TypeError, ValueError):
        return None


def fetch(
    client: httpx.Client,
    url: str,
    objects: ObjectStore,
    *,
    etag: str | None = None,
    last_modified: str | None = None,
    max_bytes: int = MAX_BYTES,
    max_seconds: float = MAX_SECONDS,
    clock: Callable[[], float] = time.monotonic,
) -> FetchResult:
    headers = {}
    if etag:
        headers["If-None-Match"] = etag
    if last_modified:
        headers["If-Modified-Since"] = last_modified
    try:
        with client.stream("GET", url, headers=headers) as response:
            result = FetchResult(
                status=response.status_code,
                content_type=response.headers.get("content-type"),
                etag=response.headers.get("etag"),
                last_modified=response.headers.get("last-modified"),
                location=response.headers.get("location"),
                retry_after=parse_retry_after(response.headers.get("retry-after")),
            )
            if not 200 <= response.status_code < 300:
                sample = bytearray()
                for chunk in response.iter_bytes():
                    sample.extend(chunk)
                    if len(sample) >= SAMPLE_BYTES:
                        break
                result.sample = bytes(sample[:SAMPLE_BYTES])
                return result
            declared = response.headers.get("content-length", "")
            if declared.isdigit() and int(declared) > max_bytes:
                result.too_large, result.size = True, int(declared)
                return result
            digest, size, started = hashlib.sha256(), 0, clock()
            temp = objects.temp_path()
            try:
                with temp.open("wb") as handle:
                    for chunk in response.iter_bytes():
                        size += len(chunk)
                        if size > max_bytes:
                            result.too_large, result.size = True, size
                            break
                        if clock() - started > max_seconds:
                            result.error = f"download exceeded {int(max_seconds)} s"
                            break
                        digest.update(chunk)
                        handle.write(chunk)
                if result.too_large or result.error:
                    temp.unlink(missing_ok=True)
                    return result
            except BaseException:
                temp.unlink(missing_ok=True)
                raise
            result.sha256, result.size = digest.hexdigest(), size
            result.path = objects.commit(temp, result.sha256)
            return result
    except httpx.HTTPError as problem:
        return FetchResult(status=None, error=f"{type(problem).__name__}: {problem}"[:500])
