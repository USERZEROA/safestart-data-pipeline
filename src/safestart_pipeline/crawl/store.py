"""Crawl state in SQLite: every known URL with its latest outcome, the link graph, each fetch, and each sitemap read."""

from __future__ import annotations

import sqlite3
from pathlib import Path

FINAL_STATES = ("fetched", "unchanged", "redirect", "not_found", "error", "skipped")
WAITING_STATES = ("pending", "retry")

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
  error text
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
"""

UPDATABLE = frozenset(
    "state skip_reason http_status content_type content_length sha256 location etag last_modified fetched_at attempts "
    "next_attempt_at error".split()
)


class Store:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.db = sqlite3.connect(path, timeout=60)
        self.db.row_factory = sqlite3.Row
        self.db.execute("pragma journal_mode=wal")
        self.db.execute("pragma synchronous=normal")
        self.db.executescript(SCHEMA)

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

    def recheck(self) -> int:
        """Queue every downloaded or unchanged URL again; the stored validators make the requests conditional."""
        cursor = self.db.execute(
            "update urls set state = 'pending', attempts = 0, next_attempt_at = null"
            " where state in ('fetched', 'unchanged')"
        )
        self.db.commit()
        return cursor.rowcount

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
