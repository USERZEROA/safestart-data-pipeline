"""The record of every watcher check: a stream.source_checks row when PostgreSQL is reachable, and a log line always.

A watcher never waits on the database: the connection has a short timeout, and an unreachable database (or one without
the stream schema yet) costs only the row, which the log keeps instead.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from ..config import postgres_url

STATUSES = frozenset({"ok", "unchanged", "error", "skipped"})
CONNECT_TIMEOUT_SECONDS = 5
INSERT = (
    "insert into stream.source_checks (source, checked_at, status, found, note) values (%s, %s, %s, %s, %s)"
    " on conflict (source, checked_at) do nothing"
)
log = logging.getLogger("watch")


@dataclass(frozen=True)
class Check:
    """One check of one source, as stream.source_checks holds it: ok (new events found), unchanged, error or skipped."""

    source: str
    status: str
    found: int = 0
    note: str = ""
    checked_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        if self.status not in STATUSES:
            raise ValueError(f"status must be one of {sorted(STATUSES)}, not {self.status!r}")
        if self.found < 0:
            raise ValueError("found cannot be negative")


def record_check(check: Check, *, url: str | None = None, connect: Callable[..., Any] | None = None) -> bool:
    """Log the check and store it in stream.source_checks; False when the database could not take it."""
    log.info("%s: %s, %d new; %s", check.source, check.status, check.found, check.note)
    try:
        import psycopg  # the stream extra; without it the log is the record
    except ImportError:
        log.warning("check of %s not stored: psycopg is not installed", check.source)
        return False
    try:
        with (connect or psycopg.connect)(url or postgres_url(), connect_timeout=CONNECT_TIMEOUT_SECONDS) as connection:
            connection.execute(INSERT, (check.source, check.checked_at, check.status, check.found, check.note))
        return True
    except psycopg.Error as problem:
        log.warning("check of %s not stored in PostgreSQL: %s", check.source, problem)
        return False
