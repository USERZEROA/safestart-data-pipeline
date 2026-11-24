"""Content hashes for the copies a crawl store held before it recorded them (`ssp official backfill-hashes <site>`).

Each held row without a content hash gets one from its stored object, examined as the crawler examines an answer
(content.py): a usable copy gets its content hash, its detected type and its first edition (first seen when it was
downloaded), and is taken as checked then, so the daily re-check starts from the download times; a copy that fails the
check gets its detected type, the state "rejected" and the reason in its error, and no edition, so it is never offered
as a file. The objects are only read, from any folder (`objects`): on the workstation the data root's own raw archive,
which the VM does not have. Rows are taken in order and committed in batches; an interrupted backfill keeps the batches
it finished, drops the one it was in, and resumes where it stopped. A row whose object is missing stays as it was, page
or file: nothing is recorded as usable unless its bytes were examined. The crawler examines such a copy when the site
answers with the very same bytes (`same_bytes`). No change rows are written: these copies are the baseline the daily run
compares with, not news.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path

from .content import HASH_FORMAT, examine
from .store import Store

TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
# How long a confirmed copy is taken to stay current before it is due to be asked about again: pages about monthly,
# files about weekly (OSHA's 10 s Crawl-delay allows about 8,300 requests a day for some 41,000 held URLs).
RECHECK_PAGE = timedelta(days=30)
RECHECK_FILE = timedelta(days=7)
BATCH = 500
EXAMPLES = 50
log = logging.getLogger(__name__)


def object_path(objects: Path, digest: str) -> Path:
    return objects / digest[:2] / digest[2:4] / digest


def due_after(checked_at: str, detected_type: str | None) -> str:
    """When a URL confirmed at `checked_at` is next due to be asked about: a month on for a page, a week for a file."""
    moment = datetime.strptime(checked_at, TIME_FORMAT).replace(tzinfo=UTC)
    return (moment + (RECHECK_PAGE if detected_type == "html" else RECHECK_FILE)).strftime(TIME_FORMAT)


def examine_held(store: Store, objects: Path, row: sqlite3.Row, *,
                 same_bytes: Path | None = None) -> tuple[str, str | None, str]:
    """Examine and record the held copy of a row that has none recorded yet, from its object in `objects`, or else from
    `same_bytes`, a file known to hold the very same bytes (an answer whose SHA-256 is the row's). Returns (outcome,
    detected type, note): outcome "usable", one of the refusal reasons, or "missing" when neither is there (the row is
    then left as it was)."""
    url, digest = row["url"], row["sha256"]
    checked_at = row["last_checked_at"] or row["fetched_at"] or row["first_seen_at"]
    path = object_path(objects, digest)
    if not path.exists():
        if same_bytes is None:
            return "missing", None, "object not found"
        path = same_bytes
    verdict = examine(path, url=url, content_type=row["content_type"], sha256=digest)
    if verdict.usable:
        record_usable(store, row, verdict.content_sha256, verdict.detected_type, checked_at)
        return "usable", verdict.detected_type, verdict.note
    store.update(url, detected_type=verdict.detected_type or "empty", state="rejected", next_attempt_at=None,
                 error=f"rejected {verdict.reason}: {verdict.note}"[:500])
    return verdict.reason, verdict.detected_type, verdict.note


def record_usable(store: Store, row: sqlite3.Row, content_sha256: str, detected_type: str | None,
                  checked_at: str) -> None:
    store.update(row["url"], content_sha256=content_sha256, detected_type=detected_type, last_checked_at=checked_at,
                 next_check_at=row["next_check_at"] or due_after(checked_at, detected_type))
    store.add_edition(row["url"], content_sha256=content_sha256, sha256=row["sha256"], size=row["content_length"],
                      content_type=row["content_type"], detected_type=detected_type, first_seen_at=checked_at)


def backfill_hashes(store: Store, objects: Path, *, batch: int = BATCH) -> dict:
    """Examine every held copy the store has no content hash for; returns the counts, the refused copies (up to
    EXAMPLES of them) and the time taken."""
    started = time.monotonic()
    outcomes: Counter[str] = Counter()
    types: Counter[str] = Counter()
    refused: list[dict] = []
    before = store.unexamined_count()
    store.set_meta("content_hash_format", HASH_FORMAT)
    last = 0
    try:
        while rows := store.unexamined_after(last, batch):
            for row in rows:
                last = row["rowid"]
                outcome, detected, note = examine_held(store, objects, row)
                outcomes[outcome] += 1
                types[detected or "unknown"] += 1
                if outcome not in ("usable", "missing") and len(refused) < EXAMPLES:
                    refused.append({"url": row["url"], "reason": outcome, "detected_type": detected, "note": note})
            store.commit()
            log.info("backfill: %d of %d examined, %s", sum(outcomes.values()), before, dict(outcomes))
    except BaseException:
        store.db.rollback()  # the batch it was in, whole: never a content hash without its edition
        raise
    store.set_meta("backfilled_at", datetime.now(UTC).strftime(TIME_FORMAT))
    store.commit()
    return {"examined": sum(outcomes.values()), "usable": outcomes.pop("usable", 0),
            "missing_objects": outcomes.pop("missing", 0), "rejected": dict(outcomes), "detected_types": dict(types),
            "still_unexamined": store.unexamined_count(), "rejected_examples": refused,
            "seconds": round(time.monotonic() - started, 1)}
