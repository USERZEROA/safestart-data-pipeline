"""Bulk files: list them from their publisher, download each politely and conditionally, verify it, and keep every
distinct verified version (SHA-256) so weekly replacements and regulation updates become a version history."""

from __future__ import annotations

import json
import sqlite3
import time
import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx
from lxml import etree, html

from ..config import USER_AGENT
from ..crawl.crawler import MAX_RETRY_AFTER_SECONDS
from ..crawl.fetcher import FetchResult, ObjectStore, fetch
from ..crawl.robots import Robots
from ..crawl.urls import extension, normalize

MAX_BYTES = 1536 * 1024 * 1024
MAX_SECONDS = 3600.0
TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
MSHA_LISTING = "https://arlweb.msha.gov/OpenGovernmentData/OGIMSHA.asp"
MSHA_PREFIX = "https://arlweb.msha.gov/OpenGovernmentData/DataSets/"
ECFR_TITLES = (29, 30)

SCHEMA = """
create table if not exists files (
  url text primary key,
  source text not null,
  name text not null,
  listed_at text not null,
  state text not null,
  http_status integer,
  content_type text,
  content_length integer,
  sha256 text,
  etag text,
  last_modified text,
  fetched_at text,
  verified integer,
  verify_note text,
  details text,
  error text
);
create table if not exists versions (
  url text not null,
  sha256 text not null,
  content_length integer,
  last_modified text,
  first_seen_at text not null,
  primary key (url, sha256)
);
create table if not exists listings (
  source text not null,
  listed_at text not null,
  page_url text,
  page_sha256 text,
  files integer not null
);
"""


@dataclass(frozen=True)
class BulkFile:
    source: str
    url: str

    @property
    def name(self) -> str:
        return self.url.rsplit("/", 1)[-1]


def utc_iso() -> str:
    return datetime.now(UTC).strftime(TIME_FORMAT)


def msha_dataset_links(page: bytes, page_url: str = MSHA_LISTING) -> list[str]:
    """Every data set (.zip) and definition file (.txt) the MSHA open-data page links to, in page order."""
    document = html.fromstring(page)
    found: dict[str, None] = {}
    for anchor in document.iter("a"):
        url = normalize(anchor.get("href") or "", page_url)
        if url and url.lower().startswith(MSHA_PREFIX.lower()) and extension(url) in (".zip", ".txt"):
            found.setdefault(url, None)
    return list(found)


def ecfr_files() -> list[BulkFile]:
    return [
        BulkFile("ecfr-govinfo", f"https://www.govinfo.gov/bulkdata/ECFR/title-{title}/ECFR-title{title}.xml")
        for title in ECFR_TITLES
    ]


def verify(path: Path, name: str) -> tuple[bool, str, dict]:
    """Check a downloaded file is what its name says: an intact zip, well-formed XML, or a non-empty text file."""
    suffix = extension("https://x/" + name)
    try:
        if suffix == ".zip":
            with zipfile.ZipFile(path) as archive:
                broken = archive.testzip()
                members = [{"name": info.filename, "size": info.file_size} for info in archive.infolist()]
            if broken:
                return False, f"corrupt member {broken}", {"members": members}
            return bool(members), f"{len(members)} member(s)", {"members": members}
        if suffix == ".xml":
            root_tag, elements, sections = None, 0, 0
            for _, element in etree.iterparse(str(path), events=("end",), huge_tree=True, resolve_entities=False):
                elements += 1
                if element.tag == "DIV8" and element.get("TYPE") == "SECTION":
                    sections += 1
                if element.getparent() is None:
                    root_tag = element.tag
                element.clear()
            return True, f"root <{root_tag}>, {elements} elements, {sections} sections", {
                "root": root_tag, "elements": elements, "sections": sections,
            }
        data = path.read_bytes()
        lines = data.count(b"\n") + (0 if data.endswith(b"\n") or not data else 1)
        return bool(data.strip()), f"{lines} line(s)", {"lines": lines, "bytes": len(data)}
    except (zipfile.BadZipFile, etree.XMLSyntaxError, OSError) as problem:
        return False, f"{type(problem).__name__}: {problem}"[:300], {}


class BulkStore:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        # One file at a time, but an orchestrator may run the tasks on a worker thread.
        self.db = sqlite3.connect(path, timeout=60, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("pragma journal_mode=wal")
        self.db.executescript(SCHEMA)

    def close(self) -> None:
        self.db.commit()
        self.db.close()

    def get(self, url: str) -> sqlite3.Row | None:
        return self.db.execute("select * from files where url = ?", (url,)).fetchone()

    def version_count(self, url: str) -> int:
        return self.db.execute("select count(*) from versions where url = ?", (url,)).fetchone()[0]

    def listed(self, source: str, files: list[BulkFile], *, page_url: str | None, page_sha256: str | None) -> None:
        now = utc_iso()
        self.db.execute(
            "insert into listings (source, listed_at, page_url, page_sha256, files) values (?, ?, ?, ?, ?)",
            (source, now, page_url, page_sha256, len(files)),
        )
        for item in files:
            self.db.execute(
                "insert into files (url, source, name, listed_at, state) values (?, ?, ?, ?, 'pending')"
                " on conflict(url) do update set listed_at = excluded.listed_at",
                (item.url, item.source, item.name, now),
            )
        self.db.commit()

    def record(self, url: str, **fields: object) -> None:
        assignments = ", ".join(f"{name} = ?" for name in fields)
        self.db.execute(f"update files set {assignments} where url = ?", (*fields.values(), url))

    def add_version(self, url: str, sha256: str, size: int | None, last_modified: str | None) -> bool:
        cursor = self.db.execute(
            "insert or ignore into versions (url, sha256, content_length, last_modified, first_seen_at)"
            " values (?, ?, ?, ?, ?)",
            (url, sha256, size, last_modified, utc_iso()),
        )
        return cursor.rowcount == 1

    def summary(self) -> dict[str, object]:
        rows = self.db.execute("select source, state, count(*), sum(verified) from files group by 1, 2").fetchall()
        return {f"{source}/{state}": {"files": count, "verified": verified or 0}
                for source, state, count, verified in rows}


def refusal(robots: Robots) -> str:
    """Why robots.txt keeps a URL from being asked: it is unreachable, or it disallows the URL."""
    return "robots.txt unreachable" if robots.unreachable else "disallowed by robots.txt"


class BulkAcquirer:
    """Downloads listed bulk files one at a time, as the crawlers ask: nothing robots.txt disallows (the open-data page
    included), and each request at least the acquirer's delay or the host's Crawl-delay after the one before, longer
    when the host asks for it with Retry-After (at most an hour). An unreachable robots.txt allows nothing; it is a
    failed check, asked again by the next request, so a retry goes ahead once the host answers.

    A copy held stays current until a new download of its file is verified: an error, a refusal or a download that
    fails verification changes only the file's state, so the lake and the warehouse keep building from the copy held,
    and the next check asks with that copy's validators instead of getting a 304 for the rejected download."""

    def __init__(
        self,
        data_root: Path,
        client: httpx.Client,
        *,
        delay: float = 1.0,
        sleep: Callable[[float], None] = time.sleep,
        max_bytes: int = MAX_BYTES,
    ):
        self.store = BulkStore(data_root / "state" / "bulk.sqlite")
        self.objects = ObjectStore(data_root / "raw" / "bulk" / "objects")
        self.client = client
        self.delay = delay
        self.sleep = sleep
        self.max_bytes = max_bytes
        self._robots: dict[str, Robots] = {}

    def robots_for(self, url: str) -> Robots:
        """The rules of the URL's host, read once (again while its robots.txt is unreachable)."""
        host = httpx.URL(url).host
        if host not in self._robots or self._robots[host].unreachable:
            try:
                response = self.client.get(f"https://{host}/robots.txt", follow_redirects=True)
                status, text = response.status_code, response.text if response.status_code < 300 else ""
            except httpx.HTTPError:
                status, text = None, ""
            self._robots[host] = Robots(status, text, USER_AGENT)
            self.pause(url)
        return self._robots[host]

    def pause(self, url: str, retry_after: float | None = None) -> None:
        """Wait before the next request: the acquirer's delay or the host's Crawl-delay, whichever is longer, or the
        Retry-After the host asked for when that is longer still (at most an hour)."""
        robots = self._robots.get(httpx.URL(url).host)
        crawl_delay = (robots.crawl_delay() if robots else None) or 0.0
        self.sleep(max(self.delay, crawl_delay, min(retry_after or 0.0, MAX_RETRY_AFTER_SECONDS)))

    def list_msha(self) -> list[BulkFile]:
        """The data sets and definition files MSHA's open-data page lists, the page asked only if robots.txt allows."""
        robots = self.robots_for(MSHA_LISTING)
        if not robots.allowed(MSHA_LISTING):
            raise RuntimeError(f"MSHA open-data page not asked: {refusal(robots)}")
        result = fetch(self.client, MSHA_LISTING, self.objects, max_bytes=5 * 1024 * 1024)
        self.pause(MSHA_LISTING, result.retry_after)
        if result.path is None:
            raise RuntimeError(f"MSHA open-data page unavailable: {result.status or result.error}")
        files = [BulkFile("msha-ogd", url) for url in msha_dataset_links(result.path.read_bytes())]
        if not files:
            raise RuntimeError("MSHA open-data page lists no data sets; its layout may have changed")
        self.store.listed("msha-ogd", files, page_url=MSHA_LISTING, page_sha256=result.sha256)
        return files

    def list_ecfr(self) -> list[BulkFile]:
        files = ecfr_files()
        self.store.listed("ecfr-govinfo", files, page_url=None, page_sha256=None)
        return files

    def download(self, item: BulkFile) -> dict[str, object]:
        robots = self.robots_for(item.url)
        if not robots.allowed(item.url):
            # A disallow is final until robots.txt changes; an unreachable robots.txt is a check that failed.
            state = "error" if robots.unreachable else "skipped"
            self.store.record(item.url, state=state, error=refusal(robots), fetched_at=utc_iso())
            self.store.db.commit()
            return {"url": item.url, "state": state}
        previous = self.store.get(item.url)
        result = fetch(
            self.client, item.url, self.objects,
            etag=previous["etag"] if previous and previous["sha256"] else None,
            last_modified=previous["last_modified"] if previous and previous["sha256"] else None,
            max_bytes=self.max_bytes, max_seconds=MAX_SECONDS,
        )
        outcome = self._apply(item, result)
        self.store.db.commit()
        self.pause(item.url, result.retry_after)
        return outcome

    def _apply(self, item: BulkFile, result: FetchResult) -> dict[str, object]:
        now = utc_iso()
        common = {"http_status": result.status, "fetched_at": now}
        if result.status == 304:
            self.store.record(item.url, state="unchanged", error=None, **common)
            return {"url": item.url, "state": "unchanged"}
        if result.path is not None:
            ok, note, details = verify(result.path, item.name)
            if not ok:
                # Not a version: the row keeps the copy held (its digest, validators and verification), and the error
                # names the rejected download.
                error = f"download {result.sha256} failed verification: {note}"
                self.store.record(item.url, state="error", error=error, **common)
                return {"url": item.url, "state": "error", "verified": False, "note": note}
            new_version = self.store.add_version(item.url, result.sha256, result.size, result.last_modified)
            self.store.record(
                item.url, state="fetched", content_type=result.content_type, content_length=result.size,
                sha256=result.sha256, etag=result.etag, last_modified=result.last_modified, verified=1,
                verify_note=note, details=json.dumps(details), error=None, **common,
            )
            return {"url": item.url, "state": "fetched", "verified": True, "note": note, "new_version": new_version}
        if result.too_large:
            self.store.record(item.url, state="skipped", error=f"over the {self.max_bytes} byte cap", **common)
            return {"url": item.url, "state": "skipped"}
        state = "not_found" if result.status in (404, 410) else "error"
        self.store.record(item.url, state=state, error=result.error or f"HTTP {result.status}", **common)
        return {"url": item.url, "state": state, "status": result.status}
