"""The daily run's own record: `<data root>/state/official.sqlite`.

- runs: one row per run of `ssp official run` or of the official flow: its start, its deadline, its end and why it
  ended ("done", "errors", "interrupted"; no end while it runs or when its process died).
- sources: one row per source a run asked (osha, arlweb, msha, ecfr, federal_register), with the status the feed
  reports for it (ok, partial, refused, unavailable, error, skipped), a note, the requests sent, the stop reason, the
  crawl store's run (crawl-<site>.sqlite runs.run_id, whose changes rows name it) and the counts as JSON. A row whose
  source is still "running" with no end was interrupted.
- hints: the eCFR titles whose govinfo bulk edition changed (title, issue date, the previous one, the file's SHA-256)
  and the Federal Register final rules that amend title 29 or 30 (title, document number, effective date), each
  recorded once, with the run that found it.
- marks: how far each hint source has been read, so the next run starts there.
- publishes: every attempt to publish the feed (feed.py): when, by what, its outcome ("published", "unsigned" when no
  signing key is set and nothing was written, "error"), the manifests it wrote, their window and counts, and a note.
The crawl sites' own findings (new, changed, gone, restored, moved and rejected URLs) stay in their crawl stores; the
feed's manifests are files in exports/official/, which alone say what was published.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

STATUSES = ("ok", "partial", "refused", "unavailable", "error", "skipped")
# The host each source asks: the www.msha.gov probe, the govinfo bulk eCFR, the Federal Register API and the two crawls.
HOSTS = {"msha": "www.msha.gov", "ecfr": "www.govinfo.gov", "federal_register": "www.federalregister.gov",
         "arlweb": "arlweb.msha.gov", "osha": "www.osha.gov"}
PUBLISH_OUTCOMES = ("published", "unsigned", "error")

SCHEMA = """
create table if not exists runs (
  run_id text primary key,
  started_at text not null,
  until text not null,
  ended_at text,
  stop_reason text,
  trigger text not null
);
create table if not exists sources (
  run_id text not null,
  source text not null,
  host text,
  status text not null check (status in ('running', 'ok', 'partial', 'refused', 'unavailable', 'error', 'skipped')),
  note text,
  requests integer not null default 0,
  stop_reason text,
  started_at text not null,
  ended_at text,
  crawl_run_id text,
  details text,
  primary key (run_id, source)
) without rowid;
create table if not exists hints (
  id integer primary key,
  source text not null check (source in ('ecfr', 'federal_register')),
  title integer not null,
  issue_date text,
  previous_issue_date text,
  sha256 text,
  document_number text,
  effective_on text,
  publication_date text,
  observed_at text not null,
  run_id text not null
);
create unique index if not exists hints_once on hints (source, title, coalesce(sha256, ''),
                                                      coalesce(document_number, ''));
create index if not exists hints_by_time on hints (observed_at);
create table if not exists marks (name text primary key, value text not null);
create table if not exists publishes (
  id integer primary key,
  attempted_at text not null,
  trigger text not null,
  run_id text,
  outcome text not null check (outcome in ('published', 'unsigned', 'error')),
  first_sequence integer,
  last_sequence integer,
  window_start text,
  window_end text,
  counts text,
  note text
);
"""


class OfficialState:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        # A Prefect flow may run its tasks on a worker thread; one task at a time uses the connection.
        self.db = sqlite3.connect(path, timeout=60, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("pragma journal_mode=wal")
        self.db.executescript(SCHEMA)

    def close(self) -> None:
        self.db.commit()
        self.db.close()

    # Runs and sources -------------------------------------------------------------------------------------------------

    def start_run(self, run_id: str, *, started_at: str, until: str, trigger: str) -> None:
        self.db.execute("insert into runs (run_id, started_at, until, trigger) values (?, ?, ?, ?)",
                        (run_id, started_at, until, trigger))
        self.db.commit()

    def end_run(self, run_id: str, *, ended_at: str, stop_reason: str) -> None:
        self.db.execute("update runs set ended_at = ?, stop_reason = ? where run_id = ?",
                        (ended_at, stop_reason, run_id))
        self.db.commit()

    def start_source(self, run_id: str, source: str, *, host: str, started_at: str) -> None:
        """A source begun ("running"); a retry of it in the same run starts its row again."""
        self.db.execute("insert or replace into sources (run_id, source, host, status, started_at)"
                        " values (?, ?, ?, 'running', ?)", (run_id, source, host, started_at))
        self.db.commit()

    def end_source(self, run_id: str, source: str, *, ended_at: str, status: str, note: str, requests: int,
                   stop_reason: str | None, crawl_run_id: str | None = None, details: dict | None = None) -> None:
        if status not in STATUSES:
            raise ValueError(f"status must be one of {STATUSES}, not {status!r}")
        self.db.execute(
            "update sources set ended_at = ?, status = ?, note = ?, requests = ?, stop_reason = ?, crawl_run_id = ?,"
            " details = ? where run_id = ? and source = ?",
            (ended_at, status, note[:2000], requests, stop_reason, crawl_run_id,
             json.dumps(details or {}, sort_keys=True, default=str), run_id, source))
        self.db.commit()

    # Hints and marks --------------------------------------------------------------------------------------------------

    def add_hint(self, source: str, title: int, *, observed_at: str, run_id: str, **fields: object) -> bool:
        """Record a hint the first time it is found; False when it was recorded before."""
        names = ["source", "title", "observed_at", "run_id", *fields]
        cursor = self.db.execute(
            f"insert or ignore into hints ({', '.join(names)}) values ({', '.join('?' for _ in names)})",
            (source, title, observed_at, run_id, *fields.values()))
        return cursor.rowcount == 1

    def mark(self, name: str) -> str | None:
        row = self.db.execute("select value from marks where name = ?", (name,)).fetchone()
        return row[0] if row else None

    def set_mark(self, name: str, value: str) -> None:
        self.db.execute("insert or replace into marks (name, value) values (?, ?)", (name, value))

    def commit(self) -> None:
        self.db.commit()

    # The feed ---------------------------------------------------------------------------------------------------------

    def add_publish(self, *, attempted_at: str, trigger: str, outcome: str, note: str, run_id: str | None = None,
                    first_sequence: int | None = None, last_sequence: int | None = None,
                    window_start: str | None = None, window_end: str | None = None,
                    counts: dict | None = None) -> None:
        if outcome not in PUBLISH_OUTCOMES:
            raise ValueError(f"outcome must be one of {PUBLISH_OUTCOMES}, not {outcome!r}")
        self.db.execute(
            "insert into publishes (attempted_at, trigger, run_id, outcome, first_sequence, last_sequence,"
            " window_start, window_end, counts, note) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (attempted_at, trigger, run_id, outcome, first_sequence, last_sequence, window_start, window_end,
             None if counts is None else json.dumps(counts, sort_keys=True), note[:2000]))
        self.db.commit()
