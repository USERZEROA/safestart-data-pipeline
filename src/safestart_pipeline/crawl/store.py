"""Crawl state in SQLite: every known URL with its latest outcome, the link graph, each fetch, each sitemap read, the
editions of every held URL, the changes found, and the runs that found them.

A URL is held when the store keeps a verified copy of it: its row then has a content hash (content_sha256: the
normalized hash for a page, the SHA-256 for a file, see content.py) and its sha256, content_type, content_length,
detected_type, etag and last_modified describe that copy. A held URL stays held when later answers fail: only a 404 or
410 retires it (gone_since, and purge_after 30 days on), and a later usable answer restores it. Rows recorded before
content hashes existed have a sha256 but no content hash until their copy is examined. last_checked_at and
next_check_at keep the rolling re-check of held URLs, and dropped_from_sitemap_at marks a URL a complete sitemap read
no longer lists.

editions holds every distinct content a URL has had (url, content hash), the object holding it and when it was first
seen; changes holds every new, changed, gone, restored, moved and rejected answer, with the run that saw it. A store of
an earlier version gains the new columns, tables and indexes when it is opened; existing columns and states keep their
meaning.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

FINAL_STATES = ("fetched", "unchanged", "redirect", "not_found", "error", "skipped", "rejected")
WAITING_STATES = ("pending", "retry")
CHANGES = ("new", "changed", "gone", "restored", "moved", "rejected")

SCHEMA = """
create table if not exists urls (
  url text primary key,
  source text not null,
  in_sitemap integer not null default 0,
  sitemap_lastmod text,
  first_seen_at text not null,
  discovered_from text,
  state text not null,
  skip_reason text,
  http_status integer,
  content_type text,
  content_length integer,
  sha256 text,
  location text,
  etag text,
  last_modified text,
  fetched_at text,
  attempts integer not null default 0,
  next_attempt_at text,
  error text,
  content_sha256 text,
  detected_type text,
  next_check_at text,
  last_checked_at text,
  gone_since text,
  purge_after text,
  dropped_from_sitemap_at text
);
create index if not exists urls_by_state on urls (state, next_attempt_at);
create table if not exists links (
  from_url text not null,
  to_url text not null,
  primary key (from_url, to_url)
) without rowid;
create index if not exists links_by_target on links (to_url);
create table if not exists fetches (
  url text not null,
  fetched_at text not null,
  http_status integer,
  sha256 text,
  content_length integer,
  etag text,
  last_modified text,
  error text
);
create index if not exists fetches_by_url on fetches (url, fetched_at);
create table if not exists sitemaps (
  url text primary key,
  parent text,
  fetched_at text,
  http_status integer,
  kind text,
  entries integer,
  sha256 text,
  error text
);
create table if not exists meta (key text primary key, value text);
create table if not exists editions (
  url text not null,
  content_sha256 text not null,
  sha256 text not null,
  bytes integer,
  content_type text,
  detected_type text,
  first_seen_at text not null,
  primary key (url, content_sha256)
) without rowid;
create table if not exists changes (
  id integer primary key,
  url text not null,
  change text not null check (change in ('new', 'changed', 'gone', 'restored', 'moved', 'rejected')),
  sha256 text,
  content_sha256 text,
  previous_content_sha256 text,
  bytes integer,
  content_type text,
  detected_type text,
  http_status integer,
  target text,
  reason text,
  note text,
  observed_at text not null,
  run_id text
);
create index if not exists changes_by_time on changes (observed_at);
create index if not exists changes_by_url on changes (url, observed_at);
create table if not exists runs (
  run_id text primary key,
  site text not null,
  started_at text not null,
  ended_at text,
  stop_reason text,
  requests integer
);
"""
# Columns urls gained after the first version, added in place to a store that lacks them.
ADDED_URL_COLUMNS = ("content_sha256", "detected_type", "next_check_at", "last_checked_at", "gone_since",
                     "purge_after", "dropped_from_sitemap_at")

UPDATABLE = frozenset(
    "state skip_reason http_status content_type content_length sha256 location etag last_modified fetched_at attempts "
    "next_attempt_at error".split()
) | frozenset(ADDED_URL_COLUMNS)
CHANGE_FIELDS = frozenset("sha256 content_sha256 previous_content_sha256 bytes content_type detected_type http_status"
                          " target reason note".split())


def unexamined(row: sqlite3.Row) -> bool:
    """Whether a row holds a copy recorded before content hashes existed, not yet examined."""
    return (row["sha256"] is not None and row["content_sha256"] is None and row["detected_type"] is None
            and row["state"] != "rejected")


def holds(row: sqlite3.Row) -> bool:
    """Whether the store keeps a copy of the URL: a verified one, or one recorded before content hashes existed."""
    return row["content_sha256"] is not None or unexamined(row)


class Store:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.db = sqlite3.connect(path, timeout=60)
        self.db.row_factory = sqlite3.Row
        self.db.execute("pragma journal_mode=wal")
        self.db.execute("pragma synchronous=normal")
        self.db.executescript(SCHEMA)
        self.migrate()

    def migrate(self) -> None:
        """Bring a store of an earlier version up to this one: the urls columns it lacks (empty in every row). They are
        looked for again under the write lock, so two processes opening an old store at once add each column once; a
        store already up to date is not written."""
        if self.missing_columns():
            self.db.execute("begin immediate")
            for column in self.missing_columns():
                self.db.execute(f"alter table urls add column {column} text")
            self.db.commit()

    def missing_columns(self) -> list[str]:
        present = {row[1] for row in self.db.execute("pragma table_info(urls)")}
        return [column for column in ADDED_URL_COLUMNS if column not in present]

    def close(self) -> None:
        self.db.commit()
        self.db.close()

    def commit(self) -> None:
        self.db.commit()

    # URLs -------------------------------------------------------------------------------------------------------------

    def get(self, url: str) -> sqlite3.Row | None:
        return self.db.execute("select * from urls where url = ?", (url,)).fetchone()

    def insert_url(
        self,
        url: str,
        *,
        source: str,
        state: str,
        now: str,
        skip_reason: str | None = None,
        in_sitemap: bool = False,
        lastmod: str | None = None,
        discovered_from: str | None = None,
    ) -> None:
        self.db.execute(
            "insert or ignore into urls (url, source, in_sitemap, sitemap_lastmod, first_seen_at, discovered_from, state,"
            " skip_reason) values (?, ?, ?, ?, ?, ?, ?, ?)",
            (url, source, int(in_sitemap), lastmod, now, discovered_from, state, skip_reason),
        )

    def mark_in_sitemap(self, url: str, lastmod: str | None) -> None:
        self.db.execute("update urls set in_sitemap = 1, sitemap_lastmod = ? where url = ?", (lastmod, url))

    def update(self, url: str, **fields: object) -> None:
        unknown = set(fields) - UPDATABLE
        if unknown:
            raise ValueError(f"not updatable: {sorted(unknown)}")
        assignments = ", ".join(f"{name} = ?" for name in fields)
        self.db.execute(f"update urls set {assignments} where url = ?", (*fields.values(), url))

    def next_due(self, now: str) -> sqlite3.Row | None:
        """A due retry first, then pending sitemap URLs, then other pending URLs, each in discovery order."""
        return self.db.execute(
            "select * from urls where state = 'pending' or (state = 'retry' and next_attempt_at <= ?)"
            " order by case state when 'retry' then 0 else 1 end, in_sitemap desc, rowid limit 1",
            (now,),
        ).fetchone()

    def earliest_retry(self) -> str | None:
        row = self.db.execute("select min(next_attempt_at) from urls where state = 'retry'").fetchone()
        return row[0] if row else None

    def counts(self) -> dict[str, int]:
        rows = self.db.execute("select state, count(*) from urls group by state").fetchall()
        return {state: count for state, count in rows}

    def skip_reasons(self) -> dict[str, int]:
        rows = self.db.execute(
            "select skip_reason, count(*) from urls where state = 'skipped' group by skip_reason"
        ).fetchall()
        return {reason: count for reason, count in rows}

    def discovered_count(self) -> int:
        return self.db.execute("select count(*) from urls where in_sitemap = 0").fetchone()[0]

    def fetched_bytes(self) -> int:
        row = self.db.execute("select coalesce(sum(content_length), 0) from urls where state = 'fetched'").fetchone()
        return int(row[0])

    def linking_pages(self, url: str) -> list[str]:
        return [row[0] for row in self.db.execute("select from_url from links where to_url = ?", (url,))]

    def waiting(self) -> list[sqlite3.Row]:
        marks = ", ".join("?" for _ in WAITING_STATES)
        return self.db.execute(f"select * from urls where state in ({marks})", WAITING_STATES).fetchall()

    def skipped(self, reasons: tuple[str, ...]) -> list[sqlite3.Row]:
        marks = ", ".join("?" for _ in reasons)
        query = f"select * from urls where state = 'skipped' and skip_reason in ({marks})"
        return self.db.execute(query, reasons).fetchall()

    def recheck(self) -> int:
        """Queue every downloaded or unchanged URL again; the stored validators make the requests conditional."""
        cursor = self.db.execute(
            "update urls set state = 'pending', attempts = 0, next_attempt_at = null"
            " where state in ('fetched', 'unchanged')"
        )
        self.db.commit()
        return cursor.rowcount

    def unexamined_after(self, rowid: int, limit: int) -> list[sqlite3.Row]:
        """Up to `limit` rows after `rowid`, in rowid order, holding a copy not yet examined (see `unexamined`)."""
        return self.db.execute(
            "select rowid, * from urls where rowid > ? and sha256 is not null and content_sha256 is null"
            " and detected_type is null and state != 'rejected' order by rowid limit ?", (rowid, limit)).fetchall()

    def unexamined_count(self) -> int:
        return self.db.execute("select count(*) from urls where sha256 is not null and content_sha256 is null"
                               " and detected_type is null and state != 'rejected'").fetchone()[0]

    # Links, fetches, sitemaps, meta -----------------------------------------------------------------------------------

    def add_links(self, from_url: str, to_urls: list[str]) -> None:
        self.db.executemany(
            "insert or ignore into links (from_url, to_url) values (?, ?)", [(from_url, to) for to in to_urls]
        )

    def add_fetch(
        self,
        url: str,
        fetched_at: str,
        *,
        status: int | None,
        sha256: str | None,
        size: int | None,
        etag: str | None,
        last_modified: str | None,
        error: str | None,
    ) -> None:
        self.db.execute(
            "insert into fetches (url, fetched_at, http_status, sha256, content_length, etag, last_modified, error)"
            " values (?, ?, ?, ?, ?, ?, ?, ?)",
            (url, fetched_at, status, sha256, size, etag, last_modified, error),
        )

    def record_sitemap(
        self,
        url: str,
        *,
        parent: str | None,
        fetched_at: str,
        status: int | None,
        kind: str | None,
        entries: int | None,
        sha256: str | None,
        error: str | None,
    ) -> None:
        self.db.execute(
            "insert or replace into sitemaps (url, parent, fetched_at, http_status, kind, entries, sha256, error)"
            " values (?, ?, ?, ?, ?, ?, ?, ?)",
            (url, parent, fetched_at, status, kind, entries, sha256, error),
        )

    def set_meta(self, key: str, value: str) -> None:
        self.db.execute("insert or replace into meta (key, value) values (?, ?)", (key, value))

    def get_meta(self, key: str) -> str | None:
        row = self.db.execute("select value from meta where key = ?", (key,)).fetchone()
        return row[0] if row else None

    # Editions, changes, runs ------------------------------------------------------------------------------------------

    def add_edition(self, url: str, *, content_sha256: str, sha256: str, size: int | None, content_type: str | None,
                    detected_type: str | None, first_seen_at: str) -> bool:
        """Record a content of the URL the first time it is seen; False when the URL had that content before."""
        cursor = self.db.execute(
            "insert or ignore into editions (url, content_sha256, sha256, bytes, content_type, detected_type,"
            " first_seen_at) values (?, ?, ?, ?, ?, ?, ?)",
            (url, content_sha256, sha256, size, content_type, detected_type, first_seen_at))
        return cursor.rowcount == 1

    def editions(self, url: str) -> list[sqlite3.Row]:
        """The URL's editions, the first seen first."""
        return self.db.execute("select * from editions where url = ? order by first_seen_at, content_sha256",
                               (url,)).fetchall()

    def add_change(self, url: str, change: str, *, observed_at: str, run_id: str | None, **fields: object) -> None:
        unknown = set(fields) - CHANGE_FIELDS
        if unknown:
            raise ValueError(f"not a change field: {sorted(unknown)}")
        names = ["url", "change", "observed_at", "run_id", *fields]
        marks = ", ".join("?" for _ in names)
        self.db.execute(f"insert into changes ({', '.join(names)}) values ({marks})",
                        (url, change, observed_at, run_id, *fields.values()))

    def changes(self, *, since: str | None = None, until: str | None = None) -> list[sqlite3.Row]:
        """The changes observed at or after `since` and before `until` (either may be left open), oldest first."""
        return self.db.execute(
            "select * from changes where observed_at >= coalesce(?, '') and observed_at < coalesce(?, '~')"
            " order by observed_at, id", (since, until)).fetchall()

    def start_run(self, run_id: str, site: str, started_at: str) -> None:
        self.db.execute("insert into runs (run_id, site, started_at) values (?, ?, ?)", (run_id, site, started_at))

    def end_run(self, run_id: str, *, ended_at: str, stop_reason: str, requests: int) -> None:
        self.db.execute("update runs set ended_at = ?, stop_reason = ?, requests = ? where run_id = ?",
                        (ended_at, stop_reason, requests, run_id))

    def get_run(self, run_id: str) -> sqlite3.Row | None:
        return self.db.execute("select * from runs where run_id = ?", (run_id,)).fetchone()
