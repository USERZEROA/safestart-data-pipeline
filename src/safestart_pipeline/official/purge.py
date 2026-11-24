"""Deleting the originals of retired URLs on this host: the official flow, after its feed, and `ssp official purge`.

An object (raw/<site>/objects/ab/cd/<sha256>) is deleted once every URL and every edition that references its SHA-256
belongs to a URL retired for 30 days: one a 404 or 410 retired (gone_since) whose purge_after has passed and that no
usable answer has restored since (a restore clears both). Anything else that references the SHA-256 keeps the object: a
held current copy, an earlier edition of a URL still current (kept as its history), a URL moved, refused or retired more
recently, a sitemap's body. An object nothing references (a body kept by a run that died before it recorded it, or the
downloads of an inventory since replaced) is counted, never deleted.

A site is purged under its crawl lock, waited for up to LOCK_WAIT (a watcher's check holds it for under a minute), so no
crawl keeps a new copy of the same bytes meanwhile; when another process holds it longer (a crawl), the site is skipped
until the next run. Only this host's copies are deleted. The bucket's and the workstation's raw archives are never
touched, since the server can only read the bucket, and the server's boot copy (deploy/gcp/startup.sh) never brings
raw/<site>/ over, so nothing purged comes back. The flow deletes only on the server, the host whose orchestrator holds
the feed's signing key (`deletes_here`); anywhere else, a workstation that runs the orchestrator on its archive among
them, the flow's purge is a dry run. `ssp official purge` reports what it would delete unless it is given --delete.
Each site's outcome is recorded in state/official.sqlite (purges), with every object it deleted, and each deletion is
logged.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from collections.abc import Callable, Iterable, Mapping
from contextlib import ExitStack
from datetime import datetime, timedelta
from pathlib import Path

from ..crawl.backfill import object_path
from ..crawl.crawler import iso, utc_now
from ..crawl.locks import wait_for_lock
from ..verify import read_only
from .feed import KEY_VARIABLE
from .state import OfficialState

SITES = ("osha", "msha", "arlweb")
LOCK_WAIT = timedelta(minutes=10)
EXAMPLES = 20
# Each SHA-256 that URLs or editions reference, with how many reference it, when it is free: when every reference
# belongs to a URL retired past its purge_after, and no sitemap's body has it.
CANDIDATES = """
with refs as (
  select url, sha256 from urls where sha256 is not null
  union all
  select url, sha256 from editions
)
select r.sha256, count(*) as refs, min(r.url) as url, max(u.gone_since) as gone_since, max(u.purge_after) as purge_after
from refs r join urls u on u.url = r.url
group by r.sha256
having sum(u.gone_since is null or u.purge_after is null or u.purge_after > :now) = 0
   and r.sha256 not in (select sha256 from sitemaps where sha256 is not null)
order by r.sha256
"""
REFERENCED = ("select sha256 from urls where sha256 is not null union select sha256 from editions"
              " union select sha256 from sitemaps where sha256 is not null")
log = logging.getLogger(__name__)


def deletes_here(environ: Mapping[str, str] = os.environ) -> bool:
    """Whether the official flow deletes retired originals on this host: only on the server, the one host whose
    orchestrator is given the feed's signing key (KEY_VARIABLE, which only the server's .env holds)."""
    return bool((environ.get(KEY_VARIABLE) or "").strip())


def migrated(path: Path) -> bool:
    """Whether a crawl store records retirements (a store of the first version does not, and is only read here)."""
    db = read_only(path)
    try:
        columns = {row[1] for row in db.execute("pragma table_info(urls)")}
        tables = {row[0] for row in db.execute("select name from sqlite_master where type = 'table'")}
    finally:
        db.close()
    return {"gone_since", "purge_after"} <= columns and "editions" in tables


def unreferenced(objects: Path, referenced: set[str]) -> dict:
    """The objects in the store's folder that nothing references: how many, their bytes, a few of them."""
    files = size = 0
    examples: list[str] = []
    if objects.is_dir():
        for first in objects.iterdir():
            if not first.is_dir() or len(first.name) != 2:  # tmp/, where downloads in progress wait
                continue
            for second in first.iterdir():
                for path in second.iterdir() if second.is_dir() else ():
                    if path.name not in referenced:
                        files += 1
                        size += path.stat().st_size
                        if len(examples) < EXAMPLES:
                            examples.append(path.name)
    return {"files": files, "bytes": size, "examples": examples}


def purge_site(root: Path, site: str, *, delete: bool, now: Callable[[], datetime],
               sleep: Callable[[float], None]) -> dict:
    """One site's purge (deleting only with `delete`); returns its outcome, counts and examples, and with `delete`
    every object it deleted and every one it could not ("error" then, the others deleted all the same)."""
    store = root / "state" / f"crawl-{site}.sqlite"
    if not store.exists():
        return {"outcome": "skipped", "note": "no crawl inventory on this host"}
    if not migrated(store):
        return {"outcome": "skipped", "note": "an inventory of the first version, which records no retirements"}
    with ExitStack() as stack:
        if delete and not wait_for_lock(stack, root / "state" / f"crawl-{site}.lock", give_up=now() + LOCK_WAIT,
                                        now=now, sleep=sleep):
            return {"outcome": "skipped", "note": f"another process held crawl-{site}.lock for"
                                                  f" {LOCK_WAIT.seconds // 60} minutes (a crawl?); nothing was deleted"}
        moment = iso(now())
        db = read_only(store)
        try:
            free = db.execute(CANDIDATES, {"now": moment}).fetchall()
            referenced = {row[0] for row in db.execute(REFERENCED)}
        finally:
            db.close()
        objects = root / "raw" / site / "objects"
        chosen, failed, absent = [], [], 0
        for row in free:
            path = object_path(objects, row["sha256"])
            try:
                size = path.stat().st_size
            except FileNotFoundError:  # deleted before, or never on this host (the server lacks the crawls' archive)
                absent += 1
                continue
            found = {"sha256": row["sha256"], "bytes": size, "url": row["url"], "references": row["refs"],
                     "goneSince": row["gone_since"], "purgeAfter": row["purge_after"]}
            if delete:
                try:
                    path.unlink()
                except OSError as problem:
                    failed.append({**found, "error": f"{type(problem).__name__}: {problem}"[:300]})
                    log.error("purge %s: could not delete %s: %s", site, row["sha256"], problem)
                    continue
                log.info("purge %s: deleted %s, %s bytes, retired %s: %s", site, row["sha256"], size,
                         row["gone_since"], row["url"])
                for folder in (path.parent, path.parent.parent):
                    try:
                        folder.rmdir()  # only when empty
                    except OSError:
                        break
            chosen.append(found)
        left = unreferenced(objects, referenced)
    total = sum(found["bytes"] for found in chosen)
    verb = "deleted" if delete else "a dry run: would delete"
    note = (f"{verb} {len(chosen):,} objects, {total:,} bytes, whose every URL and edition was retired more than 30"
            f" days ago; {absent:,} such objects not on this host; {left['files']:,} objects nothing references"
            " (kept)" + (f"; {len(failed):,} could not be deleted" if failed else ""))
    report = {"outcome": "error" if failed else "done" if delete else "dry_run", "at": moment, "free": len(free),
              "selected": len(chosen) + len(failed), "deleted": len(chosen) if delete else 0, "bytes": total,
              "absent": absent, "unreferenced": left, "examples": chosen[:EXAMPLES], "note": note}
    if delete:  # what was deleted, every object of it, and what could not be
        report |= {"deleted_objects": chosen, "failed": failed}
    return report


def purge(root: Path, *, delete: bool, sites: Iterable[str] = SITES, now: Callable[[], datetime] | None = None,
          sleep: Callable[[float], None] | None = None) -> dict:
    """Every site's purge (on the wall clock unless `now` is given), each recorded in state/official.sqlite. A site
    that fails is recorded as an error and the others still run; then RuntimeError names the failures."""
    now, sleep = now or utc_now, sleep or time.sleep
    report: dict[str, dict] = {}
    state = OfficialState(root / "state" / "official.sqlite")
    try:
        for site in sites:
            try:
                found = purge_site(root, site, delete=delete, now=now, sleep=sleep)
            except (OSError, sqlite3.Error) as problem:
                log.exception("purge of %s failed", site)
                found = {"outcome": "error", "note": f"{type(problem).__name__}: {problem}"}
            report[site] = found
            deleted = found.get("deleted", 0)
            details = {key: value for key, value in found.items() if key not in ("outcome", "note", "deleted", "at")}
            state.add_purge(purged_at=found.get("at") or iso(now()), site=site, outcome=found["outcome"],
                            note=found["note"], deleted=deleted, deleted_bytes=found["bytes"] if deleted else 0,
                            details=details)
            log.info("purge %s: %s", site, found["note"])
    finally:
        state.close()
    if failed := [site for site, found in report.items() if found["outcome"] == "error"]:
        raise RuntimeError("purge failed for " + ", ".join(f"{site} ({report[site]['note']})" for site in failed))
    return report


def latest_purges(root: Path) -> dict[str, dict]:
    """Each site's latest purge (state/official.sqlite), only read."""
    path = root / "state" / "official.sqlite"
    if not path.exists():
        return {}
    db = read_only(path)
    try:
        rows = db.execute("select * from purges where id in (select max(id) from purges group by site)").fetchall()
    except sqlite3.OperationalError:  # a record from before the purge
        return {}
    finally:
        db.close()
    return {row["site"]: {"purged_at": row["purged_at"], "outcome": row["outcome"], "deleted": row["deleted"],
                          "deleted_bytes": row["deleted_bytes"], "note": row["note"],
                          "details": json.loads(row["details"] or "{}")} for row in rows}
