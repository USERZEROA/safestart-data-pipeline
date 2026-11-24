"""The signed daily feed of the official files that SafeStart reads: `<data root>/exports/official/`, served
read-only at https://<host>/feed/official/ (deploy/Caddyfile). docs/OFFICIAL_FILES.md is the contract, format FORMAT.

The files:
- manifests/<sequence>.json and manifests/<sequence>.json.sig: every manifest and its signature, kept KEEP (45 days);
  the newest is never removed, and a manifest, once written, is never written again;
- public-key.txt: the Ed25519 public key, base64 of its 32 raw bytes;
- latest.json and latest.json.sig: the newest manifest's exact bytes and its signature.
A signature is Ed25519 over a file's exact bytes, in base64 without a line break. Every file is written as
<name>.partial and moved into place with os.replace, under state/official.lock, in this order: each new manifest and its
signature, then the public key (when it changed), then latest.json and latest.json.sig last. A publish that was
interrupted is finished by the next one: the newest manifest on disk gets whatever of its signature, the public key and
latest.json it lacks, and the next window goes on from it, so a missed or interrupted day still publishes the next
sequence.

The signing key is OFFICIAL_FEED_SIGNING_KEY, base64 of the 32 raw bytes of an Ed25519 private key; `ssp official
keygen` writes a new pair to a file the operator names and prints only the public key. It is read from the environment
only and never appears in a log or a message. Without it, a publish writes nothing and records the outcome "unsigned",
a warning in the log, in the run's report, in `ssp official status` and on the dashboard: the findings wait in the
stores for the first signed manifest.

What a manifest lists. Its window starts where the previous manifest's window ended (the first one's at the start of the
first official run recorded) and ends at the publish, held back to the start of work another process still has under
way (a crawl run whose site lock is held, a source of another official run before its deadline), whose findings then go
into the next manifest. Times are UTC to the second, and a window holds what was observed at or after its start and
before its end. From each crawl store (osha, msha, arlweb) it lists:
- entries: the new, changed, gone, restored and moved answers (the changes rows), each as observed, in the order
  observed; a change of a picture, a recording or another file the feed does not describe (content.py OTHER_TYPES) is
  only counted (otherTypes). detectedType is null for a copy recorded before content hashes and never examined;
- checked: the held URLs confirmed in the window (last_checked_at) whose copy is of a feed type, still current (neither
  gone nor moved) and unchanged in the window, sorted;
- rejected: the bodies refused as editions (the rejected rows), the latest per URL;
and from state/official.sqlite the eCFR and Federal Register hints observed in the window (ecfr) and each source's
outcome (sources: the runs of it that ended in the window, the latest one that asked anything giving the status, the
requests added up; a source no run asked is "skipped"), each crawl store's re-check cycle as it stands at the publish.
A window whose findings would pass MAX_ENTRIES entries, MAX_CHECKED checked URLs or MAX_BYTES bytes is split by time
into several manifests, published together in sequence; the findings of one second are never split.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import re
import sqlite3
import time
from collections.abc import Callable, Mapping
from contextlib import ExitStack
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

from ..crawl.content import FEED_TYPES
from ..crawl.crawler import iso, parse_iso, utc_now
from ..crawl.locks import lock_state, wait_for_lock
from ..verify import read_only
from .daily import CURRENT_SQL, cycle
from .state import HOSTS, OfficialState

FORMAT = "safestart-official-updates/1"
KEY_VARIABLE = "OFFICIAL_FEED_SIGNING_KEY"
SOURCES = ("osha", "msha", "arlweb", "ecfr", "federal_register")  # the order of a manifest's sources
CRAWLED = ("osha", "msha", "arlweb")  # the sources whose crawl stores hold entries, checked URLs and refused files
CHANGES = ("new", "changed", "gone", "restored", "moved")
COUNTED = (*CHANGES, "checked", "rejected", "hints", "otherTypes")
MAX_ENTRIES = 5_000
MAX_CHECKED = 20_000
MAX_BYTES = 2_000_000  # under the 2 MiB SafeStart reads of one file
# What MAX_BYTES keeps for all but the lists: the header, the counts and the five sources, whose notes are cut to
# NOTE_CHARACTERS (at most 6 bytes each once escaped), so the lists' budget always holds.
HEADER_BYTES = 64 * 1024
NOTE_CHARACTERS = 1_000
KEEP = timedelta(days=45)
STALE_AFTER = timedelta(hours=36)  # SafeStart's and the dashboard's warning: no manifest for a day and a half
LOCK_WAIT = timedelta(minutes=10)
NUMBER = re.compile(r"[1-9][0-9]*")
GENERATED_AT = re.compile(rb'"generatedAt":"([^"]+)"')
# Held URLs confirmed in the window, current documents of a feed type (daily.CURRENT_SQL, which the run's own count of
# its checks uses too), and with no change but a refusal in the window.
CHECKED_SQL = f"""
select url, last_checked_at from urls u
where {CURRENT_SQL}
  and last_checked_at >= :start and last_checked_at < :end
  and not exists (select 1 from changes c where c.url = u.url and c.change != 'rejected'
                  and c.observed_at >= :start and c.observed_at < :end)
"""
log = logging.getLogger(__name__)


class FeedError(Exception):
    """The feed on disk cannot be continued as it stands (a file that is not a manifest, two different newest
    manifests, findings of one second that no manifest can hold): nothing more is published until someone looks."""


@dataclass(frozen=True)
class Limits:
    """What one manifest may hold: entries, checked URLs, and bytes in all."""

    entries: int
    checked: int
    bytes: int

    @classmethod
    def contract(cls) -> Limits:
        return cls(MAX_ENTRIES, MAX_CHECKED, MAX_BYTES)


# The signing key ------------------------------------------------------------------------------------------------------
# cryptography is imported where it is used: it comes with the orchestration extra, which the crawler's own small
# environment (scripts/start-crawl.ps1) does not install, and that environment imports this module through the CLI.


def signing_key(environ: Mapping[str, str] = os.environ):
    """The Ed25519 private key that OFFICIAL_FEED_SIGNING_KEY holds, or None when it is not set. ValueError when it is
    not base64 of 32 bytes, or when cryptography is not installed; no message ever quotes the key."""
    value = (environ.get(KEY_VARIABLE) or "").strip()
    if not value:
        return None
    try:
        seed = base64.b64decode(value, validate=True)
    except ValueError:  # binascii.Error, whose message would not quote it either
        raise ValueError(f"{KEY_VARIABLE} is not base64") from None
    if len(seed) != 32:
        raise ValueError(f"{KEY_VARIABLE} holds {len(seed)} bytes; an Ed25519 private key is 32")
    try:
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    except ImportError:
        raise ValueError("signing the feed needs cryptography: uv sync --extra orchestration") from None
    return Ed25519PrivateKey.from_private_bytes(seed)


def public_key(key) -> str:
    """The public key of a private key, as public-key.txt holds it: base64 of its 32 raw bytes."""
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

    return base64.b64encode(key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)).decode("ascii")


def sign(key, data: bytes) -> bytes:
    """A .sig file's bytes: the Ed25519 signature of `data`, base64."""
    return base64.b64encode(key.sign(data))


def verify(public: str, data: bytes, signature: bytes) -> bool:
    """Whether `signature` (a .sig file's bytes) is the signature of `data` by the key `public` (base64)."""
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

    try:
        Ed25519PublicKey.from_public_bytes(base64.b64decode(public, validate=True)).verify(
            base64.b64decode(signature, validate=True), data)
    except (InvalidSignature, ValueError):
        return False
    return True


def keygen(path: Path, now: datetime) -> str:
    """A new key pair, written to `path`, which must not exist yet, readable by its owner only: the private key as a
    line for the server's .env, the public key in a comment. Returns the public key."""
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives.serialization import Encoding, NoEncryption, PrivateFormat

    key = Ed25519PrivateKey.generate()
    seed = base64.b64encode(key.private_bytes(Encoding.Raw, PrivateFormat.Raw, NoEncryption())).decode("ascii")
    public = public_key(key)
    text = (f"# The signing key of the official-files feed, made {iso(now)}. It belongs in the server's .env and\n"
            f"# nowhere else. Its public key (public-key.txt; SafeStart pins it): {public}\n"
            f"{KEY_VARIABLE}={seed}\n")
    handle = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(handle, "w", encoding="utf-8", newline="\n") as file:
        file.write(text)
    return public


# The folder -----------------------------------------------------------------------------------------------------------


def sync_folder(folder: Path) -> None:
    """Make the renames in a folder durable (POSIX: fsync the folder itself; Windows cannot open a folder this way and
    needs no such step)."""
    if os.name != "posix":
        return
    handle = os.open(folder, os.O_RDONLY)
    try:
        os.fsync(handle)
    finally:
        os.close(handle)


class Folder:
    """exports/official/, and the files a publish wrote there, in order (names as the feed serves them)."""

    def __init__(self, root: Path):
        self.path = root / "exports" / "official"
        self.manifests = self.path / "manifests"
        self.written: list[str] = []

    def read(self, name: str) -> bytes | None:
        try:
            return (self.path / name).read_bytes()
        except FileNotFoundError:
            return None

    def write(self, name: str, data: bytes) -> None:
        """The file's new bytes: written beside it as <name>.partial, synced, then moved into place in one step, and the
        folder synced, so that after a power cut a file is there whole or as it was, in the order written."""
        path = self.path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        partial = path.with_name(path.name + ".partial")
        with partial.open("wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(partial, path)
        sync_folder(path.parent)
        self.written.append(name)

    def clear_partials(self) -> list[str]:
        """Remove what an interrupted write left (no other publisher writes while official.lock is held)."""
        left = [*self.path.glob("*.partial"), *self.manifests.glob("*.partial")]
        for path in left:
            path.unlink(missing_ok=True)
        return [path.name for path in left]

    def numbers(self) -> list[int]:
        """The sequences of the numbered manifests on disk, in order."""
        if not self.manifests.is_dir():
            return []
        return sorted(int(path.stem) for path in self.manifests.glob("*.json") if NUMBER.fullmatch(path.stem))


@dataclass(frozen=True)
class Head:
    """The newest manifest: its sequence, its exact bytes and what they say."""

    sequence: int
    data: bytes
    manifest: dict

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.data).hexdigest()

    @property
    def window_end(self) -> str:
        end = (self.manifest.get("window") or {}).get("end")
        try:
            parse_iso(end)
        except (TypeError, ValueError):
            raise FeedError(f"manifest {self.sequence} has no window end in the form 2026-11-24T16:30:00Z") from None
        return end


def parse_manifest(data: bytes, where: str) -> dict:
    try:
        manifest = json.loads(data)
    except ValueError as problem:
        raise FeedError(f"{where} is not JSON ({problem})") from None
    if not isinstance(manifest, dict) or manifest.get("format") != FORMAT or not isinstance(manifest.get("sequence"),
                                                                                             int):
        raise FeedError(f"{where} is not a {FORMAT} manifest")
    return manifest


def chain_head(folder: Folder) -> Head | None:
    """The newest manifest on disk: the highest numbered one, or latest.json when it is newer still (its numbered copy
    was lost, and is written again from it). FeedError for a file that is not a manifest of its number, or a latest.json
    of the same sequence with other bytes."""
    head = None
    if numbers := folder.numbers():
        data = (folder.manifests / f"{numbers[-1]}.json").read_bytes()
        manifest = parse_manifest(data, f"manifests/{numbers[-1]}.json")
        if manifest["sequence"] != numbers[-1]:
            raise FeedError(f"manifests/{numbers[-1]}.json holds sequence {manifest['sequence']}")
        head = Head(numbers[-1], data, manifest)
    if (data := folder.read("latest.json")) is not None:
        manifest = parse_manifest(data, "latest.json")
        if head is None or manifest["sequence"] > head.sequence:
            head = Head(manifest["sequence"], data, manifest)
        elif manifest["sequence"] == head.sequence and data != head.data:
            raise FeedError(f"latest.json and manifests/{head.sequence}.json are two different manifests"
                            f" {head.sequence}")
    return head


def finish(folder: Folder, head: Head, key, public: str) -> None:
    """Write what an interrupted publish left out of the newest manifest's files: its numbered copy, its signature,
    the public key, latest.json and latest.json.sig. A signature by another key is made again with this one."""
    name = f"manifests/{head.sequence}.json"
    if folder.read(name) is None:
        folder.write(name, head.data)
    if not verify(public, head.data, folder.read(f"{name}.sig") or b""):
        folder.write(f"{name}.sig", sign(key, head.data))
    if folder.read("public-key.txt") != public.encode("ascii"):
        folder.write("public-key.txt", public.encode("ascii"))
    if folder.read("latest.json") != head.data:
        folder.write("latest.json", head.data)
    if not verify(public, head.data, folder.read("latest.json.sig") or b""):
        folder.write("latest.json.sig", sign(key, head.data))


def generated_at(path: Path) -> str:
    """When a manifest was generated (its generatedAt, near its start), or else when its file was last written."""
    with path.open("rb") as handle:
        found = GENERATED_AT.search(handle.read(1024))
    if found:
        return found[1].decode("ascii", "replace")
    return iso(datetime.fromtimestamp(path.stat().st_mtime, UTC))


def remove_old(folder: Folder, newest: int, now: datetime, keep: timedelta = KEEP) -> list[int]:
    """Remove the numbered manifests (and their signatures) generated more than `keep` ago, never the newest."""
    cutoff, removed = iso(now - keep), []
    for number in folder.numbers():
        path = folder.manifests / f"{number}.json"
        if number >= newest or generated_at(path) >= cutoff:
            continue
        path.unlink(missing_ok=True)
        (folder.manifests / f"{number}.json.sig").unlink(missing_ok=True)
        removed.append(number)
    return removed


# What a window holds --------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Item:
    """One finding: when it was observed (what places it in a window), how it sorts among findings of the same second,
    what it is ("entry", "checked", "rejected", "hint", or "other", a change only counted), its source, its JSON form
    and that form's encoded length with its separator."""

    at: str
    order: tuple[int, int, int, str]
    kind: str
    source: str
    body: object = None
    size: int = 0


def item(at: str, order: tuple[int, int, int, str], kind: str, source: str, body: object = None) -> Item:
    size = len(json.dumps(body, separators=(",", ":"))) + 1 if body is not None else 0
    return Item(at, order, kind, source, body, size)


def entry_body(source: str, row: sqlite3.Row) -> dict:
    body = {"source": source, "url": row["url"], "change": row["change"], "sha256": row["sha256"],
            "contentSha256": row["content_sha256"], "previousContentSha256": row["previous_content_sha256"],
            "bytes": row["bytes"], "contentType": row["content_type"], "detectedType": row["detected_type"],
            "httpStatus": row["http_status"], "observedAt": row["observed_at"]}
    if row["change"] == "moved":
        body["target"] = row["target"]
    elif row["change"] == "gone":
        body["goneSince"] = row["observed_at"]
    return body


def rejected_body(source: str, row: sqlite3.Row) -> dict:
    return {"source": source, "url": row["url"], "reason": row["reason"], "httpStatus": row["http_status"],
            "contentType": row["content_type"], "detectedType": row["detected_type"]}


def crawl_items(root: Path, source: str, start: str, end: str) -> list[Item]:
    """A crawl store's findings in the window: its changes and the URLs it confirmed unchanged."""
    path = root / "state" / f"crawl-{source}.sqlite"
    if not path.exists():
        return []
    rank = SOURCES.index(source)
    db = read_only(path)
    try:
        try:
            changes = db.execute("select * from changes where observed_at >= ? and observed_at < ? order by id",
                                 (start, end)).fetchall()
        except sqlite3.OperationalError:  # a store of the first version, which records no changes
            return []
        checked = db.execute(CHECKED_SQL, {"start": start, "end": end}).fetchall()
    finally:
        db.close()
    items = []
    for row in changes:
        if row["change"] == "rejected":
            items.append(item(row["observed_at"], (rank, 2, row["id"], ""), "rejected", source,
                              rejected_body(source, row)))
        elif row["detected_type"] is None or row["detected_type"] in FEED_TYPES:
            items.append(item(row["observed_at"], (rank, 0, row["id"], ""), "entry", source, entry_body(source, row)))
        else:
            items.append(item(row["observed_at"], (rank, 0, row["id"], ""), "other", source))
    items += [item(row["last_checked_at"], (rank, 1, 0, row["url"]), "checked", source, row["url"]) for row in checked]
    return items


def hint_items(root: Path, start: str, end: str) -> list[Item]:
    """The eCFR and Federal Register hints observed in the window."""
    path = root / "state" / "official.sqlite"
    if not path.exists():
        return []
    db = read_only(path)
    try:
        rows = db.execute("select * from hints where observed_at >= ? and observed_at < ? order by id",
                          (start, end)).fetchall()
    finally:
        db.close()
    items = []
    for row in rows:
        if row["source"] == "ecfr":
            body = {"title": row["title"], "issueDate": row["issue_date"],
                    "previousIssueDate": row["previous_issue_date"]}
        else:
            body = {"title": row["title"], "documentNumber": row["document_number"], "effectiveOn": row["effective_on"]}
        items.append(item(row["observed_at"], (SOURCES.index(row["source"]), 3, row["id"], ""), "hint", row["source"],
                          body))
    return items


def outcomes(root: Path, start: str, end: str) -> list[dict]:
    """The sources' outcomes that ended in the window (state/official.sqlite), the first ended first."""
    path = root / "state" / "official.sqlite"
    if not path.exists():
        return []
    db = read_only(path)
    try:
        return [dict(row) for row in db.execute(
            "select source, host, status, note, requests, stop_reason, ended_at from sources where status != 'running'"
            " and ended_at >= ? and ended_at < ? order by ended_at, run_id", (start, end))]
    finally:
        db.close()


def cycles(root: Path, now: datetime) -> dict[str, dict | None]:
    """Each crawl store's re-check cycle as it stands (daily.cycle); None without a store of this version."""
    found: dict[str, dict | None] = {}
    for source in CRAWLED:
        path = root / "state" / f"crawl-{source}.sqlite"
        found[source] = None
        if path.exists():
            db = read_only(path)
            try:
                found[source] = cycle(db, now)
            except sqlite3.OperationalError:  # a store of the first version: no re-check yet
                pass
            finally:
                db.close()
    return found


def first_start(root: Path, end: str) -> str:
    """Where the first manifest's window starts: the first official run recorded, or else at its end (empty)."""
    path = root / "state" / "official.sqlite"
    if not path.exists():
        return end
    db = read_only(path)
    try:
        started = db.execute("select min(started_at) from runs").fetchone()[0]
    finally:
        db.close()
    return min(started, end) if started else end


def settled_end(root: Path, now: datetime, start: str, run_id: str | None) -> tuple[str, list[str]]:
    """The window's end: now, or the start of work another process still has under way (a crawl run whose site lock is
    held, a source of another official run before that run's deadline), whose findings may not all be recorded yet;
    never before `start`. Returns it and the work it waited for. The time is taken before anything is looked at, so
    work that begins meanwhile has findings no older than it. Only a store's newest crawl run can be under way: a
    site's runs take its lock one at a time, and a run whose process died (killed, or the machine stopped) never
    records its end, so any run after it shows it dead."""
    end, waiting = iso(now), []
    for source in CRAWLED:
        path = root / "state" / f"crawl-{source}.sqlite"
        if not path.exists():
            continue
        db = read_only(path)
        try:
            newest = db.execute("select started_at, ended_at from runs order by rowid desc limit 1").fetchone()
        except sqlite3.OperationalError:  # a store of the first version records no runs
            newest = None
        finally:
            db.close()
        started = newest["started_at"] if newest is not None and newest["ended_at"] is None else None
        if started and lock_state(root / "state" / f"crawl-{source}.lock") == "held":
            end = min(end, started)
            waiting.append(f"a {source} crawl run under way since {started}")
    path = root / "state" / "official.sqlite"
    if path.exists():
        db = read_only(path)
        try:
            rows = db.execute(
                "select s.source, s.started_at from sources s join runs r on r.run_id = s.run_id where s.status ="
                " 'running' and r.ended_at is null and r.until > ? and r.run_id != ?", (end, run_id or "")).fetchall()
        finally:
            db.close()
        for row in rows:
            end = min(end, row["started_at"])
            waiting.append(f"another official run's {row['source']} under way since {row['started_at']}")
    return max(end, start), waiting


def collect(root: Path, start: str, end: str) -> list[Item]:
    """Every finding in the window, in the order a manifest lists them."""
    found = [*hint_items(root, start, end)]
    for source in CRAWLED:
        found += crawl_items(root, source, start, end)
    return sorted(found, key=lambda finding: (finding.at, finding.order))


@dataclass
class Tally:
    """A page's entries, checked URLs and bytes of findings, against the limits of one manifest."""

    limits: Limits
    entries: int = 0
    checked: int = 0
    size: int = 0

    def add(self, finding: Item) -> None:
        self.entries += finding.kind == "entry"
        self.checked += finding.kind == "checked"
        self.size += finding.size

    def fits(self, finding: Item) -> bool:
        return (self.entries + (finding.kind == "entry") <= self.limits.entries
                and self.checked + (finding.kind == "checked") <= self.limits.checked
                and self.size + finding.size <= self.limits.bytes - HEADER_BYTES)


def paginate(items: list[Item], start: str, end: str, limits: Limits) -> list[tuple[str, str, list[Item]]]:
    """The window split by time into pages that each fit one manifest, (start, end, findings) for each, at least one.
    A page ends where the first finding it cannot hold was observed, so that finding's second starts the next page."""
    pages: list[tuple[str, str, list[Item]]] = []
    page: list[Item] = []
    page_start, tally = start, Tally(limits)
    for finding in items:
        if not tally.fits(finding):
            kept = [held for held in page if held.at < finding.at]
            moved = [held for held in page if held.at >= finding.at]  # the same second as `finding`
            tally = Tally(limits)
            for held in moved:
                tally.add(held)
            if not kept or not tally.fits(finding):
                raise FeedError(f"the findings observed at {finding.at} alone are more than one manifest holds")
            pages.append((page_start, finding.at, kept))
            page, page_start = moved, finding.at
        page.append(finding)
        tally.add(finding)
    pages.append((page_start, end, page))
    return pages


def source_outcomes(rows: list[dict], start: str, end: str, cycle_now: Mapping[str, dict | None]) -> list[dict]:
    """The manifest's sources: for each, the runs of it that ended in [start, end), its note cut to NOTE_CHARACTERS."""
    listed = []
    for source in SOURCES:
        runs = [row for row in rows if row["source"] == source and start <= row["ended_at"] < end]
        asked = [row for row in runs if row["status"] != "skipped"]
        if not runs:
            described = {"status": "skipped", "note": "no run of this source ended in this window", "requests": 0,
                         "stopReason": "not asked"}
        else:
            chosen = (asked or runs)[-1]
            note = (chosen["note"] or "")[:NOTE_CHARACTERS]
            note += f" ({len(runs)} runs of it ended in this window)" if len(runs) > 1 else ""
            described = {"status": chosen["status"], "note": note, "requests": sum(row["requests"] for row in runs),
                         "stopReason": chosen["stop_reason"]}
        listed.append({"source": source, "host": HOSTS[source], **described, "cycle": cycle_now.get(source)})
    return listed


def manifest(findings: list[Item], *, sequence: int, previous: tuple[int, str] | None, generated: str, start: str,
             end: str, sources: list[dict]) -> dict:
    """A manifest of these findings (the contract's fields, in its order); `previous` is the sequence and SHA-256 of
    the manifest before it, None for the first."""
    entries, checked, hints, rejected = [], [], [], {}
    counts = {name: dict.fromkeys(COUNTED, 0) for name in ("all", *SOURCES)}
    for finding in findings:
        if finding.kind == "rejected":
            rejected.pop((finding.source, finding.body["url"]), None)  # the latest per URL, in the order of the latest
            rejected[finding.source, finding.body["url"]] = finding.body
            continue
        if finding.kind == "entry":
            entries.append(finding.body)
            counted = finding.body["change"]
        elif finding.kind == "checked":
            checked.append(finding.body)
            counted = "checked"
        elif finding.kind == "hint":
            hints.append(finding.body)
            counted = "hints"
        else:
            counted = "otherTypes"
        counts[finding.source][counted] += 1
        counts["all"][counted] += 1
    for source, _ in rejected:
        counts[source]["rejected"] += 1
        counts["all"]["rejected"] += 1
    return {"format": FORMAT, "sequence": sequence, "previousSequence": previous[0] if previous else None,
            "previousSha256": previous[1] if previous else None, "generatedAt": generated,
            "window": {"start": start, "end": end}, "sources": sources, "entries": entries, "ecfr": hints,
            "checked": sorted(checked), "rejected": list(rejected.values()), "counts": counts}


def encode(document: dict) -> bytes:
    """A manifest's exact bytes: compact JSON in ASCII, with a final line break."""
    return (json.dumps(document, separators=(",", ":")) + "\n").encode("ascii")


# Publishing -----------------------------------------------------------------------------------------------------------


@dataclass
class Published:
    sequences: list[int] = field(default_factory=list)
    manifests: list[dict] = field(default_factory=list)  # each one's sequence, window, size and counts
    counts: dict[str, int] = field(default_factory=lambda: dict.fromkeys(COUNTED, 0))


def publish(root: Path, *, trigger: str, run_id: str | None = None, now: Callable[[], datetime] = utc_now,
            sleep: Callable[[float], None] = time.sleep, environ: Mapping[str, str] = os.environ,
            limits: Limits | None = None) -> dict:
    """Publish the next manifest (several when the window is too large for one), as the module docstring says, and
    record the attempt in state/official.sqlite; returns what was done. Without a signing key nothing is written and
    the outcome is "unsigned". An error is recorded, then raised: ValueError for a key that is not one, FeedError for a
    feed on disk that cannot be continued, OSError for a file that cannot be written or read."""
    attempted = iso(now())
    try:
        key = signing_key(environ)
    except ValueError as problem:
        record(root, attempted, trigger, run_id, "error", str(problem))
        raise
    if key is None:
        note = (f"{KEY_VARIABLE} is not set on this host, so nothing was published; the findings wait in the stores"
                " for the first signed manifest")
        log.warning("unsigned feed: %s", note)
        record(root, attempted, trigger, run_id, "unsigned", note)
        return {"outcome": "unsigned", "note": note}
    try:
        with ExitStack() as stack:
            if not wait_for_lock(stack, root / "state" / "official.lock", give_up=now() + LOCK_WAIT, now=now,
                                 sleep=sleep):
                raise FeedError("another process held state/official.lock for 10 minutes, so nothing was published")
            report = publish_held(root, key, next_second(now, sleep), run_id, limits or Limits.contract())
    except Exception as problem:
        record(root, attempted, trigger, run_id, "error", f"{type(problem).__name__}: {problem}")
        raise
    record(root, attempted, trigger, run_id, "published", report["note"], first_sequence=report["sequences"][0],
           last_sequence=report["sequences"][-1], window_start=report["window"]["start"],
           window_end=report["window"]["end"], counts=report["counts"])
    log.info("feed: %s", report["note"])
    return report


def next_second(now: Callable[[], datetime], sleep: Callable[[float], None]) -> datetime:
    """The time once the clock has passed into the next whole second. Findings and outcomes are recorded to the second,
    so everything recorded before this call has a time before the one returned: a window that ends there holds all of
    it, the run's own last outcome included, and nothing recorded later falls before it."""
    sleep(1.0 - now().microsecond / 1_000_000)
    return now()


def publish_held(root: Path, key, now: datetime, run_id: str | None, limits: Limits) -> dict:
    """The publish itself, under official.lock, at `now` (a whole second after everything it is to list)."""
    folder = Folder(root)
    cleared = folder.clear_partials()
    public = public_key(key)
    head = chain_head(folder)
    if head is not None:
        finish(folder, head, key, public)
        finished = list(folder.written)
        start = head.window_end
    else:
        finished, start = [], first_start(root, iso(now))
    end, waiting = settled_end(root, now, start, run_id)
    pages = paginate(collect(root, start, end), start, end, limits)
    ended = outcomes(root, start, end)
    cycle_now = cycles(root, now)
    generated, done, data = iso(now), Published(), b""
    previous = (head.sequence, head.sha256) if head is not None else None
    for page_start, page_end, findings in pages:
        sequence = (previous[0] if previous else 0) + 1
        document = manifest(findings, sequence=sequence, previous=previous, generated=generated, start=page_start,
                            end=page_end, sources=source_outcomes(ended, page_start, page_end, cycle_now))
        data = encode(document)
        if len(data) > limits.bytes:
            raise FeedError(f"manifest {sequence} came to {len(data):,} bytes, over the {limits.bytes:,} a manifest"
                            " may have")
        folder.write(f"manifests/{sequence}.json", data)
        folder.write(f"manifests/{sequence}.json.sig", sign(key, data))
        done.sequences.append(sequence)
        done.manifests.append({"sequence": sequence, "window": document["window"], "bytes": len(data),
                               "counts": document["counts"]["all"]})
        for name, number in document["counts"]["all"].items():
            done.counts[name] += number
        previous = (sequence, hashlib.sha256(data).hexdigest())
    if folder.read("public-key.txt") != public.encode("ascii"):
        folder.write("public-key.txt", public.encode("ascii"))
    folder.write("latest.json", data)  # the last page's bytes
    folder.write("latest.json.sig", sign(key, data))
    removed = remove_old(folder, done.sequences[-1], now)
    first, last = done.sequences[0], done.sequences[-1]
    found = ", ".join(f"{done.counts[name]:,} {name}" for name in COUNTED if done.counts[name]) or "nothing new"
    note = (f"manifest {first}" if first == last else f"manifests {first} to {last}") + f" ({start} to {end}): {found}"
    if waiting:
        note += "; the window ends where " + "; ".join(waiting) + " began"
    return {"outcome": "published", "sequences": done.sequences, "window": {"start": start, "end": end},
            "manifests": done.manifests, "counts": done.counts, "written": folder.written[len(finished):],
            "finished": finished, "partials_removed": cleared, "removed": removed, "waiting": waiting, "note": note}


def record(root: Path, attempted: str, trigger: str, run_id: str | None, outcome: str, note: str, **fields) -> None:
    state = OfficialState(root / "state" / "official.sqlite")
    try:
        state.add_publish(attempted_at=attempted, trigger=trigger, run_id=run_id, outcome=outcome, note=note, **fields)
    finally:
        state.close()


# Status ---------------------------------------------------------------------------------------------------------------


def feed_status(root: Path, now: datetime) -> dict:
    """The newest manifest (latest.json: its sequence, when it was generated and how many hours ago, its window and
    counts, and whether that is more than STALE_AFTER) and the latest attempt to publish one; both only read."""
    report: dict[str, object] = {"latest": None, "last_attempt": None}
    path = root / "exports" / "official" / "latest.json"
    if path.exists():
        try:
            latest = json.loads(path.read_bytes())
            age = now - parse_iso(latest["generatedAt"])
            report["latest"] = {"sequence": latest["sequence"], "generatedAt": latest["generatedAt"],
                                "window": latest["window"], "counts": latest["counts"]["all"],
                                "ageHours": round(age.total_seconds() / 3600, 1), "stale": age > STALE_AFTER}
        except (OSError, ValueError, KeyError, TypeError) as problem:
            report["latest"] = {"problem": f"latest.json cannot be read: {type(problem).__name__}: {problem}"[:300],
                                "stale": True}
    state = root / "state" / "official.sqlite"
    if state.exists():
        db = read_only(state)
        try:
            row = db.execute("select * from publishes order by id desc limit 1").fetchone()
        except sqlite3.OperationalError:  # a record from before the feed
            row = None
        finally:
            db.close()
        if row is not None:
            report["last_attempt"] = {**{key: row[key] for key in ("attempted_at", "trigger", "run_id", "outcome",
                                                                   "first_sequence", "last_sequence", "window_start",
                                                                   "window_end", "note")},
                                      "counts": json.loads(row["counts"]) if row["counts"] else None}
    return report
