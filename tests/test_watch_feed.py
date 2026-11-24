"""The official files' feed on the stream (watch/feed.py): manifests written and signed here as feed.publish writes
them, and one feed the real daily run published (test_feed's mocked sites). Each entry is announced once as a
page_change event, whatever is read again; a manifest that is not signed by the feed's published key, or that breaks
the chain, is never trusted, nor anything after it; what is not a page change is only counted; a new page is
announced once, whether the sitemap check or the feed sees it first."""

from __future__ import annotations

import hashlib
import json
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from safestart_pipeline.crawl.crawler import iso
from safestart_pipeline.crawl.sites import Site
from safestart_pipeline.official import feed
from safestart_pipeline.stream.events import Event
from safestart_pipeline.watch import feed as watch_feed
from safestart_pipeline.watch import sitemaps
from safestart_pipeline.watch.feed import check_feed
from safestart_pipeline.watch.publish import ListPublisher, publish_new
from safestart_pipeline.watch.state import WatchState
from test_crawler import pdf_bytes
from test_feed import feeds, key_pair, signing  # noqa: F401 (the real daily run's fixtures)
from test_official import PDF

OSHA = "https://www.osha.gov"
RUN = datetime(2026, 11, 24, 16, 30, tzinfo=UTC)
A, B, C = "a" * 64, "b" * 64, "c" * 64


def entry(path: str, change: str, observed: str, content: str | None, *, previous: str | None = None,
          detected: str | None = "html", source: str = "osha", **extra: object) -> dict:
    """One finding as a manifest lists it (official/feed.py entry_body)."""
    status = {"gone": 404, "moved": 301}.get(change, 200)
    found = {"source": source, "url": OSHA + path, "change": change, "sha256": content, "contentSha256": content,
             "previousContentSha256": previous, "bytes": 1200 if content else None,
             "contentType": "text/html" if detected == "html" else "application/pdf", "detectedType": detected,
             "httpStatus": status, "observedAt": observed}
    return found | extra


class FeedFolder:
    """exports/official/ written as feed.publish writes it: each manifest and its signature, the public key, then
    latest.json and its signature; every manifest names the one before by its sequence and SHA-256."""

    def __init__(self, root: Path):
        self.root, self.folder = root, feed.Folder(root)
        self.key = feed.signing_key({feed.KEY_VARIABLE: key_pair()[0]})
        self.public = feed.public_key(self.key)
        self.sequence, self.previous, self.end = 0, None, RUN

    def publish(self, entries: list[dict] = (), *, checked: list[str] = (), rejected: list[dict] = (),
                hints: list[dict] = (), other: int = 0, hours: float = 12) -> int:
        start, self.end = self.end, self.end + timedelta(hours=hours)
        self.sequence += 1
        counts = dict.fromkeys(feed.COUNTED, 0) | {"checked": len(checked), "rejected": len(rejected),
                                                   "hints": len(hints), "otherTypes": other}
        for found in entries:
            counts[found["change"]] += 1
        document = {"format": feed.FORMAT, "sequence": self.sequence,
                    "previousSequence": self.previous[0] if self.previous else None,
                    "previousSha256": self.previous[1] if self.previous else None, "generatedAt": iso(self.end),
                    "window": {"start": iso(start), "end": iso(self.end)}, "sources": [], "entries": list(entries),
                    "ecfr": list(hints), "checked": sorted(checked), "rejected": list(rejected),
                    "counts": {"all": counts}}
        data = feed.encode(document)
        self.folder.write(f"manifests/{self.sequence}.json", data)
        self.folder.write(f"manifests/{self.sequence}.json.sig", feed.sign(self.key, data))
        self.folder.write("public-key.txt", self.public.encode("ascii"))
        self.folder.write("latest.json", data)
        self.folder.write("latest.json.sig", feed.sign(self.key, data))
        self.previous = (self.sequence, hashlib.sha256(data).hexdigest())
        return self.sequence

    def path(self, name: str) -> Path:
        return self.folder.path / name


@pytest.fixture
def state(tmp_path):
    state = WatchState(tmp_path / "watch-state" / "watch.sqlite")
    yield state
    state.close()


@pytest.fixture
def folder(tmp_path) -> FeedFolder:
    return FeedFolder(tmp_path / "data")


def ids(publisher: ListPublisher) -> list[str]:
    return [event.event_id for event in publisher.events]


def test_each_entry_of_a_new_manifest_is_announced_once_and_the_rest_only_counted(folder, state):
    folder.publish([entry("/hazcom", "new", "2026-11-24T17:02:11Z", A),
                    entry("/files/b.pdf", "changed", "2026-11-24T18:40:00Z", B, previous=A, detected="pdf"),
                    entry("/files/c.pdf", "gone", "2026-11-24T19:00:05Z", C, detected="pdf", goneSince="x"),
                    entry("/report.pdf", "moved", "2026-11-24T19:30:00Z", A, detected="pdf",
                          target=f"{OSHA}/reports/2026.pdf")],
                   checked=[f"{OSHA}/a", f"{OSHA}/b", f"{OSHA}/c"],
                   rejected=[{"source": "osha", "url": f"{OSHA}/e.pdf", "reason": "soft_error"}],
                   hints=[{"title": 29, "documentNumber": "2026-24001", "effectiveOn": "2027-01-01"}], other=2)
    publisher = ListPublisher()
    check = check_feed(folder.root, state, publisher)
    assert (check.source, check.status, check.found) == ("feed-official", "ok", 4)
    assert check.note == ("manifest 1 (2026-11-24 16:30 to 2026-11-25 04:30 UTC): 1 new, 1 changed, 1 gone, 1 moved;"
                          " 4 announced; 3 confirmed unchanged, 1 refused files, 1 eCFR or Federal Register hints,"
                          " 2 other files changed")
    assert ids(publisher) == [
        f"page_change:official|{OSHA}/hazcom|new|{A}|2026-11-24T17:02:11Z",
        f"page_change:official|{OSHA}/files/b.pdf|changed|{B}|2026-11-24T18:40:00Z",
        f"page_change:official|{OSHA}/files/c.pdf|gone|{C}|2026-11-24T19:00:05Z",
        f"page_change:official|{OSHA}/report.pdf|moved|{A}|2026-11-24T19:30:00Z"]
    new, changed, gone, moved = publisher.events
    assert (new.title, new.kind, new.mode, new.topic) == (f"New OSHA page: {OSHA}/hazcom", "page_change", "live",
                                                          "safety.sources")
    assert changed.title == f"OSHA file changed: {OSHA}/files/b.pdf"
    assert changed.occurred_at == "2026-11-24T18:40:00Z"  # when the run observed it
    assert changed.details == {"site": "osha", "url": f"{OSHA}/files/b.pdf", "change": "changed", "sha256": B,
                               "contentSha256": B, "previousContentSha256": A, "detectedType": "pdf",
                               "httpStatus": 200, "bytes": 1200, "target": None, "manifest": 1,
                               "origin": "official feed"}
    assert changed.source == {"feed": "official", "manifest": "1", "site": "osha"}
    assert gone.details["httpStatus"] == 404 and moved.details["target"] == f"{OSHA}/reports/2026.pdf"
    assert all(Event.from_json(event.to_json()) == event for event in publisher.events)  # the contract holds

    again = check_feed(folder.root, state, publisher)
    assert (again.status, again.found, again.note) == ("unchanged", 0, "manifest 1, the newest, was read before")
    folder.publish([entry("/hazcom", "changed", "2026-11-25T17:00:00Z", B, previous=A)], checked=[f"{OSHA}/a"])
    later = check_feed(folder.root, state, publisher)
    assert (later.status, later.found) == ("ok", 1) and len(publisher.events) == 5
    assert later.note == ("manifest 2 (2026-11-25 04:30 to 2026-11-25 16:30 UTC): 1 changed; 1 announced;"
                          " 1 confirmed unchanged")


def test_a_quiet_day_reads_its_heartbeat_manifest_and_announces_nothing(folder, state):
    folder.publish(checked=[f"{OSHA}/a", f"{OSHA}/b"])
    check = check_feed(folder.root, state, ListPublisher())
    assert (check.status, check.found) == ("unchanged", 0)
    assert check.note == ("manifest 1 (2026-11-24 16:30 to 2026-11-25 04:30 UTC): no page changes; 0 announced;"
                          " 2 confirmed unchanged")


def test_a_page_that_changes_back_and_forth_is_announced_at_each_change(folder, state):
    folder.publish([entry("/hazcom", "changed", "2026-11-24T17:00:00Z", B, previous=A),
                    entry("/hazcom", "changed", "2026-11-24T21:00:00Z", A, previous=B)])
    folder.publish([entry("/hazcom", "changed", "2026-11-25T17:00:00Z", B, previous=A)])
    publisher = ListPublisher()
    check = check_feed(folder.root, state, publisher)
    assert (check.found, len(set(ids(publisher)))) == (3, 3)
    assert check.note.startswith("manifests 1 and 2 (2026-11-24 16:30 to 2026-11-25 16:30 UTC): 3 changed;")


def tamper(folder: FeedFolder) -> None:
    path = folder.path("manifests/2.json")
    path.write_bytes(path.read_bytes().replace(b'"observedAt":"2026-11-25', b'"observedAt":"2026-11-26'))


def sign_elsewhere(folder: FeedFolder) -> None:
    other = feed.signing_key({feed.KEY_VARIABLE: key_pair()[0]})
    folder.path("manifests/2.json.sig").write_bytes(feed.sign(other, folder.path("manifests/2.json").read_bytes()))


def drop_signature(folder: FeedFolder) -> None:
    folder.path("manifests/2.json.sig").unlink()


def break_chain(folder: FeedFolder) -> None:
    """Manifest 2 rewritten and signed again with the feed's own key, but no longer naming manifest 1's bytes."""
    document = json.loads(folder.path("manifests/2.json").read_bytes())
    document["previousSha256"] = "0" * 64
    data = feed.encode(document)
    folder.path("manifests/2.json").write_bytes(data)
    folder.path("manifests/2.json.sig").write_bytes(feed.sign(folder.key, data))


def not_a_manifest(folder: FeedFolder) -> None:
    data = b'{"format":"something else","sequence":2}\n'
    folder.path("manifests/2.json").write_bytes(data)
    folder.path("manifests/2.json.sig").write_bytes(feed.sign(folder.key, data))


def another_public_key(folder: FeedFolder) -> None:
    """The published key is not the one that signed the feed: nothing verifies, from the first manifest on."""
    folder.path("public-key.txt").write_text(key_pair()[1], encoding="ascii")


@pytest.mark.parametrize("damage, first, why", [
    (tamper, 2, "its signature does not verify with the feed's public key"),
    (sign_elsewhere, 2, "its signature does not verify with the feed's public key"),
    (drop_signature, 2, "it has no signature (manifests/2.json.sig)"),
    (break_chain, 2, "its previousSha256 is not the SHA-256 of manifests/1.json"),
    (not_a_manifest, 2, "manifests/2.json is not a safestart-official-updates/1 manifest"),
    (another_public_key, 1, "its signature does not verify with the feed's public key"),
])
def test_a_manifest_that_is_not_signed_by_the_published_key_or_breaks_the_chain_stops_the_check(folder, state, damage,
                                                                                               first, why):
    for day in range(3):
        folder.publish([entry(f"/page-{day}", "changed", f"2026-11-{24 + day}T18:00:00Z", B, previous=A)])
    damage(folder)
    publisher = ListPublisher()
    check = check_feed(folder.root, state, publisher)
    assert check.status == "error" and check.found == first - 1
    assert f"manifest {first} not trusted: {why}; nothing from it on was announced" in check.note
    announced = [event.details["manifest"] for event in publisher.events]
    assert announced == list(range(1, first))  # only what came before the manifest that failed
    assert state.mark(watch_feed.MARK) == (str(first - 1) if first > 1 else None)
    again = check_feed(folder.root, state, publisher)  # tried again, and refused again: nothing more is announced
    assert again.status == "error" and len(publisher.events) == first - 1


def test_failed_flush_leaves_the_manifest_for_the_next_check(folder, state):
    class FailingPublisher(ListPublisher):
        def flush(self):
            raise RuntimeError("broker unavailable")

    folder.publish([entry("/one", "changed", "2026-11-24T17:00:00Z", B, previous=A)])
    with pytest.raises(RuntimeError, match="broker unavailable"):
        check_feed(folder.root, state, FailingPublisher())
    assert state.mark(watch_feed.MARK) is None
    publisher = ListPublisher()
    assert check_feed(folder.root, state, publisher).found == 1
    assert state.mark(watch_feed.MARK) == "1"


def test_no_feed_on_this_host_leaves_the_check_skipped(tmp_path, state):
    check = check_feed(tmp_path, state, ListPublisher())
    assert (check.status, check.note) == ("skipped",
                                          "no feed on this host: no daily run has published a signed manifest here")


def test_numbered_files_wait_for_the_publish_to_finish(folder, state):
    folder.publish([entry("/one", "new", "2026-11-24T17:00:00Z", A)])
    latest = folder.path("latest.json")
    first = latest.read_bytes()
    latest.unlink()
    publisher = ListPublisher()
    assert check_feed(folder.root, state, publisher).status == "skipped"
    assert state.mark(watch_feed.MARK) is None
    folder.publish([entry("/two", "restored", "2026-11-25T17:00:00Z", B)])
    second = latest.read_bytes()
    latest.write_bytes(first)
    assert check_feed(folder.root, state, publisher).found == 1
    assert state.mark(watch_feed.MARK) == "1"
    latest.write_bytes(second)
    assert check_feed(folder.root, state, publisher).found == 1
    assert [event.details["change"] for event in publisher.events] == ["new", "restored"]


def test_manifests_removed_before_they_were_read_are_named(folder, state):
    folder.publish([entry("/one", "changed", "2026-11-24T18:00:00Z", B, previous=A)])
    publisher = ListPublisher()
    check_feed(folder.root, state, publisher)
    for day in range(1, 4):
        folder.publish([entry(f"/page-{day}", "changed", f"2026-11-{24 + day}T18:00:00Z", B, previous=A)])
    for number in (2, 3):  # older than the 45 days the feed keeps, say
        folder.path(f"manifests/{number}.json").unlink()
        folder.path(f"manifests/{number}.json.sig").unlink()
    check = check_feed(folder.root, state, publisher)
    assert (check.status, check.found) == ("ok", 1) and publisher.events[-1].details["manifest"] == 4
    assert check.note.endswith("; manifests 2 and 3 removed before this check could read them")


def test_a_feed_made_again_from_1_is_read_again_without_announcing_anything_twice(folder, state):
    kept = entry("/hazcom", "changed", "2026-11-24T18:00:00Z", B, previous=A)
    folder.publish([kept])
    folder.publish([entry("/two", "new", "2026-11-25T18:00:00Z", C)])
    publisher = ListPublisher()
    assert check_feed(folder.root, state, publisher).found == 2
    shutil.rmtree(folder.folder.path)
    rebuilt = FeedFolder(folder.root)  # a new folder, under a new key
    rebuilt.publish([kept, entry("/three", "new", "2026-11-26T18:00:00Z", A)])
    check = check_feed(folder.root, state, publisher)
    assert (check.status, check.found) == ("ok", 1) and ids(publisher)[-1].endswith("/three|new|" + A
                                                                                     + "|2026-11-26T18:00:00Z")
    assert "the feed starts again below manifest 2, so it was read again from its start" in check.note
    assert state.mark(watch_feed.MARK) == "1"


def sitemap_event(url: str, lastmod: str = "2026-11-24T10:00:00Z") -> Event:
    site = Site(name="osha", canonical_host="www.osha.gov", hosts=frozenset({"www.osha.gov"}), sitemaps=(),
                min_delay=10.0, byte_budget=10**9)
    return sitemaps.page_event(site, sitemaps.Change(url, "added", lastmod, f"{OSHA}/sitemap.xml"), RUN)


def test_a_new_page_the_sitemap_check_announced_first_is_not_announced_again(folder, state):
    publish_new([sitemap_event(f"{OSHA}/hazcom")], ListPublisher(), state)
    folder.publish([entry("/hazcom", "new", "2026-11-24T17:02:11Z", A),
                    entry("/other", "new", "2026-11-24T17:03:00Z", B)])
    publisher = ListPublisher()
    check = check_feed(folder.root, state, publisher)
    assert ids(publisher) == [f"page_change:official|{OSHA}/other|new|{B}|2026-11-24T17:03:00Z"]
    assert "2 new; 1 announced, 1 new page the sitemap check announced already" in check.note


def test_a_new_page_the_feed_announced_first_is_not_added_again_by_the_sitemap_check(folder, state):
    folder.publish([entry("/hazcom", "new", "2026-11-24T17:02:11Z", A)])
    check_feed(folder.root, state, ListPublisher())
    added = sitemaps.Change(f"{OSHA}/hazcom", "added", None, f"{OSHA}/sitemap.xml")
    other = sitemaps.Change(f"{OSHA}/other", "added", None, f"{OSHA}/sitemap.xml")
    dropped = sitemaps.Change(f"{OSHA}/hazcom", "dropped", None, None)  # a different fact: still announced
    assert sitemaps.announced([added, other, dropped], state) == [other, dropped]


def test_a_finding_that_is_not_one_stops_the_check_there(folder, state):
    folder.publish([entry("/fine", "changed", "2026-11-24T18:00:00Z", B, previous=A)])
    folder.publish([entry("/odd", "rejected", "2026-11-25T18:00:00Z", B)])
    publisher = ListPublisher()
    check = check_feed(folder.root, state, publisher)
    assert check.status == "error" and check.found == 1
    assert "manifest 2 holds a finding that is not one (ValueError: an entry of change 'rejected'" in check.note


def test_the_real_daily_runs_feed_is_verified_and_announced_once(feeds, signing, state):  # noqa: F811
    feeds.run()
    feeds.site.set("/files/b.pdf", (200, pdf_bytes(311), PDF))
    feeds.run(days=7)
    publisher = ListPublisher()
    check = check_feed(feeds.root, state, publisher)
    one, two = (json.loads((feeds.root / "exports" / "official" / "manifests" / f"{n}.json").read_bytes())
                for n in (1, 2))
    expected = [watch_feed.entry_key(found) for found in (*one["entries"], *two["entries"])]
    assert check.status == "ok" and check.found == len(expected) > len(one["entries"])
    assert ids(publisher) == [f"page_change:{key}" for key in expected]
    assert ("changed", "osha") in [(event.details["change"], event.source["site"]) for event in publisher.events]
    assert check.note.startswith("manifests 1 and 2 (") and "1 eCFR or Federal Register hints" in check.note
    assert check_feed(feeds.root, state, publisher).found == 0
