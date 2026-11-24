"""The official files' feed on the stream: each new signed manifest in `<data root>/exports/official/`
(official/feed.py, docs/OFFICIAL_FILES.md) becomes page_change events, one for each page or file the daily run found
new, changed, gone, restored or moved, so the dashboard's page changes move when a daily run finds something.

The check only reads the feed, as SafeStart's planned importer is to, and trusts nothing it has not verified: a
manifest's Ed25519 signature (manifests/<n>.json.sig) against the feed's published key (public-key.txt), its sequence
against its file name, and its previousSha256 against the manifest before it while that one is still on disk. A manifest
that fails (no signature, a signature that does not verify, a broken chain, not a manifest at all) stops the check
there: nothing from it on is announced, the check is an "error" whose note names the manifest and why, and the next
check tries again. A host where no manifest was ever published (no signing key there, so every publish was
"unsigned", or no daily run yet) leaves the check "skipped".

The watcher remembers the last manifest it finished (WatchState marks), so a check reads only newer ones. Manifests
removed before a check could read them (the feed keeps 45 days) are named in the note; a feed folder rebuilt from 1
(the newest sequence on disk below the mark) is read again from its start. Either way an event's id is fixed by its
finding, the URL, the change, the content's SHA-256 and the time the run observed it, so nothing is announced twice
(publish.publish_new, then storage's ON CONFLICT and the analytics consumer's counted_events), while a page that
changes back and forth is announced at each change.

What is not a page change is counted in the note, not announced: the URLs a run confirmed unchanged (checked), the
bodies it refused as editions (rejected), the eCFR and Federal Register hints, and changes of files the feed does not
describe (otherTypes). A new page is announced once: the sitemap check may announce it first ("added": listed today,
new to the crawl's inventory), and the daily run then downloads it ("new"); whichever of the two checks comes second
leaves it out (sitemaps.announced does the same the other way round). A page dropped from a sitemap and a page that
answers 404 or 410 (gone) stay two events: different facts, and a dropped page often still answers.
"""

from __future__ import annotations

import hashlib
from collections import Counter
from pathlib import Path

from ..official.feed import CHANGES, FeedError, Folder, parse_manifest, verify
from ..stream.events import Event
from .checks import Check
from .publish import Publisher, publish_new
from .state import WatchState

SOURCE = "feed-official"
MARK = f"{SOURCE}:sequence"  # the last manifest the check finished
KEY_PREFIX = "official|"  # every event this check publishes has an id starting page_change:official|
LABELS = {"osha": "OSHA", "msha": "MSHA", "arlweb": "MSHA legacy host"}
DETAILS = ("sha256", "contentSha256", "previousContentSha256", "detectedType", "httpStatus", "bytes", "target")
NOTE_CHARACTERS = 500


class Untrusted(Exception):
    """A manifest the check does not trust; the message says why."""


def entry_key(entry: dict) -> str:
    """A page_change event's source key for one finding of the feed: its URL, its change, the SHA-256 of the content
    it found (none for a page that is gone) and when it was observed."""
    content = entry.get("contentSha256") or entry.get("sha256") or ""
    return f"{KEY_PREFIX}{entry['url']}|{entry['change']}|{content}|{entry['observedAt']}"


def entry_event(entry: dict, sequence: int) -> Event:
    """The page_change event of one entry of manifest `sequence`; it happened when the daily run observed it. ValueError
    for an entry that is not a finding of the feed's kinds."""
    if entry["change"] not in CHANGES or not isinstance(entry["url"], str):
        raise ValueError(f"an entry of change {entry['change']!r} for {entry['url']!r}")
    site = str(entry.get("source") or "")
    what = f"{LABELS.get(site, site.upper())} {'page' if entry.get('detectedType') == 'html' else 'file'}"
    return Event.new(
        kind="page_change", key=entry_key(entry), mode="live", occurred_at=entry["observedAt"],
        title=(f"New {what}: {entry['url']}" if entry["change"] == "new"
               else f"{what} {entry['change']}: {entry['url']}"),
        details={"site": site, "url": entry["url"], "change": entry["change"],
                 **{name: entry.get(name) for name in DETAILS}, "manifest": sequence, "origin": "official feed"},
        source={"feed": "official", "manifest": str(sequence), "site": site})


def trusted(folder: Folder, number: int, public: str) -> dict:
    """Manifest `number`, once its signature, its sequence and its link to the manifest before it hold; Untrusted (or
    FeedError, for a file that is not a manifest) otherwise. FileNotFoundError when it is gone from the disk."""
    name = f"manifests/{number}.json"
    data = (folder.manifests / f"{number}.json").read_bytes()
    signature = folder.read(f"{name}.sig")
    if signature is None:
        raise Untrusted(f"it has no signature ({name}.sig)")
    if not public:
        raise Untrusted("the feed has no public key (public-key.txt) to verify it with")
    if not verify(public, data, signature):
        raise Untrusted("its signature does not verify with the feed's public key")
    manifest = parse_manifest(data, name)
    if manifest["sequence"] != number:
        raise Untrusted(f"{name} holds sequence {manifest['sequence']}")
    previous = folder.read(f"manifests/{number - 1}.json") if number > 1 else None
    if previous is not None and (manifest.get("previousSequence") != number - 1
                                 or manifest.get("previousSha256") != hashlib.sha256(previous).hexdigest()):
        raise Untrusted(f"its previousSha256 is not the SHA-256 of manifests/{number - 1}.json")
    return manifest


def spans(numbers: list[int]) -> str:
    """Sequences as people write them: "3", "3 and 4", "3 to 6"."""
    if len(numbers) == 1:
        return str(numbers[0])
    if numbers == list(range(numbers[0], numbers[-1] + 1)):
        return f"{numbers[0]} and {numbers[1]}" if len(numbers) == 2 else f"{numbers[0]} to {numbers[-1]}"
    return ", ".join(map(str, numbers))


def moment(text: object) -> str:
    """A feed time (2026-11-24T16:30:00Z) as the note writes it: 2026-11-24 16:30."""
    return str(text).replace("T", " ")[:16]


def check_feed(root: Path, state: WatchState, publisher: Publisher) -> Check:
    folder = Folder(root)
    numbers = folder.numbers()
    if not numbers and folder.read("latest.json") is None:
        return Check(SOURCE, "skipped", note="no feed on this host: no daily run has published a signed manifest here")
    public = (folder.read("public-key.txt") or b"").decode("ascii", "replace").strip()
    last, remarks = int(state.mark(MARK) or 0), []
    if numbers and numbers[-1] < last:  # the folder was made again from 1
        remarks.append(f"the feed starts again below manifest {last}, so it was read again from its start")
        last = 0
    held = set(numbers)
    removed = [number for number in range(last + 1, max(numbers, default=0)) if number not in held]
    read: list[int] = []
    windows: list[dict] = []
    counts: Counter[str] = Counter()
    events_sent = 0
    problem = ""
    for number in (number for number in numbers if number > last):
        try:
            manifest = trusted(folder, number, public)
            events, already = [], 0
            for entry in manifest.get("entries") or []:
                if (entry["change"] == "new"
                        and state.any_emitted_starting(f"page_change:{entry['url']}|added|")):
                    already += 1  # the sitemap check announced this page already
                    continue
                events.append(entry_event(entry, number))
        except FileNotFoundError:  # removed meanwhile: older than the feed keeps
            removed.append(number)
            continue
        except (Untrusted, FeedError, ImportError) as failure:
            problem = f"manifest {number} not trusted: {failure}; nothing from it on was announced"
            break
        except (KeyError, TypeError, ValueError, AttributeError) as failure:
            problem = (f"manifest {number} holds a finding that is not one ({type(failure).__name__}: {failure});"
                       " nothing from it on was announced")
            break
        events_sent += publish_new(events, publisher, state)
        state.set_mark(MARK, str(number))
        state.commit()
        read.append(number)
        windows.append(manifest.get("window") or {})
        counts.update(entry["change"] for entry in manifest.get("entries") or [])
        counts.update({"checked": len(manifest.get("checked") or []), "rejected": len(manifest.get("rejected") or []),
                       "hints": len(manifest.get("ecfr") or []), "already": already,
                       "otherTypes": int(((manifest.get("counts") or {}).get("all") or {}).get("otherTypes") or 0)})
    parts = [describe(read, windows, counts, events_sent, last, numbers, problem)]
    if removed:
        parts.append(f"manifest{'s' if len(removed) > 1 else ''} {spans(sorted(removed))} removed before this check"
                     " could read them")
    parts += [*remarks, problem]
    status = "error" if problem else "ok" if events_sent else "unchanged"
    return Check(SOURCE, status, events_sent, "; ".join(part for part in parts if part)[:NOTE_CHARACTERS])


def describe(read: list[int], windows: list[dict], counts: Counter[str], sent: int, last: int, numbers: list[int],
             problem: str) -> str:
    """What the check read and found, for its note."""
    if not read:
        if problem:
            return ""
        if not numbers:
            return "no numbered manifest yet"
        return f"manifest {numbers[-1]}, the newest, was read before" if last >= numbers[-1] else "no new manifest"
    window = f"{moment(windows[0].get('start'))} to {moment(windows[-1].get('end'))} UTC"
    changes = ", ".join(f"{counts[name]:,} {name}" for name in CHANGES if counts[name]) or "no page changes"
    note = f"manifest{'s' if len(read) > 1 else ''} {spans(read)} ({window}): {changes}; {sent:,} announced"
    if counts["already"]:
        pages = "page" if counts["already"] == 1 else "pages"
        note += f", {counts['already']:,} new {pages} the sitemap check announced already"
    others = [(counts["checked"], "confirmed unchanged"), (counts["rejected"], "refused files"),
              (counts["hints"], "eCFR or Federal Register hints"), (counts["otherTypes"], "other files changed")]
    if any(number for number, _ in others):
        note += "; " + ", ".join(f"{number:,} {what}" for number, what in others if number)
    return note
