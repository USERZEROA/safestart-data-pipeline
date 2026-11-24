"""The signed daily feed (official/feed.py), built by real daily runs against mocked sites: a manifest of every change
kind with the contract's fields, the checked URLs (counted by the run as the feed lists them) and the refused files,
each source's status, the eCFR and Federal Register hints; signatures that the public key verifies; the chain of
previousSha256; the order of the atomic writes and an interrupted publish finished by the next; 45 days of manifests
kept; a run without a signing key, or with a key that is not one; a window split into several manifests; a window held
back while another process still crawls; and `ssp official manifest|keygen|status`."""

from __future__ import annotations

import base64
import hashlib
import itertools
import json
import os
import sys
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from cryptography.hazmat.primitives.serialization import Encoding, NoEncryption, PrivateFormat, PublicFormat

from safestart_pipeline import cli
from safestart_pipeline.crawl.crawler import iso
from safestart_pipeline.crawl.locks import SiteLock
from safestart_pipeline.crawl.store import Store
from safestart_pipeline.official import feed
from safestart_pipeline.official.feed import FeedError, Limits
from safestart_pipeline.official.run import run_official
from safestart_pipeline.official.state import OfficialState
from test_crawler import HOST, PNG, Clock, pdf_bytes
from test_editions import SOFT_ERROR, Answers
from test_official import HTML, PDF, PLANS, SITES, START, FederalRegister, page, rule, site_answers

FOLDER = Path("exports") / "official"


def key_pair() -> tuple[str, str]:
    """A new signing key (base64 of its 32 raw bytes, as OFFICIAL_FEED_SIGNING_KEY holds it) and its public key."""
    key = Ed25519PrivateKey.generate()
    private = key.private_bytes(Encoding.Raw, PrivateFormat.Raw, NoEncryption())
    public = key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    return base64.b64encode(private).decode(), base64.b64encode(public).decode()


def verifies(public: str, data: bytes, signature: bytes) -> bool:
    try:
        Ed25519PublicKey.from_public_bytes(base64.b64decode(public)).verify(base64.b64decode(signature), data)
    except InvalidSignature:
        return False
    return True


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@dataclass
class Feeds:
    root: Path
    site: Answers
    clock: Clock
    client: httpx.Client

    def run(self, *, days: float = 0, sources: tuple[str, ...] = ("msha", "ecfr", "federal_register", "osha"),
            **options) -> dict:
        """A whole daily run `days` after the last one (www.msha.gov refuses, the Federal Register has one rule, the
        example site is OSHA), with its feed."""
        self.clock.now += timedelta(days=days)
        return run_official(self.root, self.client, sources=sources, sites=SITES, plans=PLANS, sleep=self.clock.sleep,
                            now=lambda: self.clock.now, until=self.clock.now + timedelta(hours=12), **options)

    def read(self, name: str) -> bytes:
        return (self.root / FOLDER / name).read_bytes()

    def manifest(self, name: str = "latest.json") -> dict:
        return json.loads(self.read(name))

    def numbers(self) -> list[int]:
        return sorted(int(path.stem) for path in (self.root / FOLDER / "manifests").glob("*.json"))

    def entry(self, document: dict, path: str, change: str) -> dict:
        [found] = [item for item in document["entries"] if item["url"] == HOST + path and item["change"] == change]
        return found


@pytest.fixture
def feeds(tmp_path):
    site, clock = site_answers(), Clock()
    clock.now = START
    api = FederalRegister({29: [rule("2026-24001", "2026-11-23", 29, effective="2027-01-01")]})

    def network(request: httpx.Request) -> httpx.Response:
        if request.url.host == "www.example.test":
            return site.handler(request)
        if request.url.host == "www.federalregister.gov":
            return api.handler(request)
        return httpx.Response(403)  # www.msha.example refuses everything

    Store(tmp_path / "state" / "crawl-osha.sqlite").close()
    client = httpx.Client(transport=httpx.MockTransport(network))
    yield Feeds(tmp_path, site, clock, client)
    client.close()


def publish(feeds: Feeds, **options) -> dict:
    """A publish on its own (`ssp official manifest`), on the runs' clock."""
    return feed.publish(feeds.root, trigger="test", now=lambda: feeds.clock.now, sleep=feeds.clock.sleep, **options)


@pytest.fixture
def signing(monkeypatch) -> str:
    """OFFICIAL_FEED_SIGNING_KEY set to a new key; returns its public key."""
    private, public = key_pair()
    monkeypatch.setenv(feed.KEY_VARIABLE, private)
    return public


def test_a_run_publishes_every_change_kind_with_the_contracts_fields(feeds, signing):
    first = feeds.run()
    assert (first["feed"]["outcome"], first["feed"]["sequences"]) == ("published", [1])
    one = feeds.manifest()
    assert (one["format"], one["sequence"], one["previousSequence"], one["previousSha256"]) == (
        "safestart-official-updates/1", 1, None, None)
    assert list(one) == ["format", "sequence", "previousSequence", "previousSha256", "generatedAt", "window", "sources",
                         "entries", "ecfr", "checked", "rejected", "counts"]
    assert one["window"] == {"start": iso(START), "end": one["generatedAt"]}  # the first official run's start
    assert sorted((item["url"].removeprefix(HOST), item["change"]) for item in one["entries"]) == sorted(
        (path, "new") for path in ("/", "/hazcom", "/publications", "/report.pdf", "/files/a.pdf", "/files/b.pdf",
                                   "/files/c.pdf", "/files/e.pdf"))
    report = feeds.entry(one, "/report.pdf", "new")
    assert report == {"source": "osha", "url": f"{HOST}/report.pdf", "change": "new", "sha256": sha(pdf_bytes(400)),
                      "contentSha256": sha(pdf_bytes(400)), "previousContentSha256": None, "bytes": 400,
                      "contentType": "application/pdf", "detectedType": "pdf", "httpStatus": 200,
                      "observedAt": report["observedAt"]}
    assert one["window"]["start"] <= report["observedAt"] < one["window"]["end"]
    assert feeds.entry(one, "/hazcom", "new")["detectedType"] == "html"
    assert one["checked"] == [] and one["rejected"] == []  # every URL is new
    assert one["ecfr"] == [{"title": 29, "documentNumber": "2026-24001", "effectiveOn": "2027-01-01"}]
    sources = {item["source"]: item for item in one["sources"]}
    assert [item["source"] for item in one["sources"]] == ["osha", "msha", "arlweb", "ecfr", "federal_register"]
    assert (sources["osha"]["status"], sources["osha"]["host"], sources["osha"]["requests"]) == ("ok", "www.osha.gov",
                                                                                                 10)
    assert sources["osha"]["cycle"]["held"] == 8 and sources["osha"]["cycle"]["checkedLast30Days"] == 8
    assert sources["msha"]["status"] == "refused" and "never worked around" in sources["msha"]["note"]
    assert (sources["arlweb"]["status"], sources["arlweb"]["stopReason"]) == ("skipped", "not asked")
    assert sources["ecfr"]["status"] == "skipped" and sources["ecfr"]["cycle"] is None
    assert one["counts"]["all"] == {"new": 8, "changed": 0, "gone": 0, "restored": 0, "moved": 0, "checked": 0,
                                    "rejected": 0, "hints": 1, "otherTypes": 0}
    assert one["counts"]["osha"]["new"] == 8 and one["counts"]["federal_register"]["hints"] == 1

    # A week on: a file changes, one is gone, one serves an error page, one is unchanged (304), the report moves.
    feeds.site.set("/files/a.pdf", (304, b"", {}))
    feeds.site.set("/files/b.pdf", (200, pdf_bytes(311), PDF))
    feeds.site.set("/files/c.pdf", (404, b"", {}))
    feeds.site.set("/files/e.pdf", (200, SOFT_ERROR, PDF))
    feeds.site.set("/report.pdf", (301, b"", {"location": "/reports/2026.pdf"}))
    feeds.site.set("/reports/2026.pdf", (200, pdf_bytes(401), PDF))
    feeds.run(days=7)
    two = feeds.manifest()
    assert (two["sequence"], two["previousSequence"], two["window"]["start"]) == (2, 1, one["window"]["end"])
    changed = feeds.entry(two, "/files/b.pdf", "changed")
    assert (changed["sha256"], changed["contentSha256"], changed["previousContentSha256"], changed["bytes"]) == (
        sha(pdf_bytes(311)), sha(pdf_bytes(311)), sha(pdf_bytes(310)), 311)
    gone = feeds.entry(two, "/files/c.pdf", "gone")
    assert (gone["httpStatus"], gone["goneSince"], gone["sha256"]) == (404, gone["observedAt"], sha(pdf_bytes(320)))
    assert "target" not in gone
    moved = feeds.entry(two, "/report.pdf", "moved")
    assert (moved["httpStatus"], moved["target"], moved["sha256"]) == (301, f"{HOST}/reports/2026.pdf",
                                                                       sha(pdf_bytes(400)))
    assert "goneSince" not in moved
    assert feeds.entry(two, "/reports/2026.pdf", "new")["bytes"] == 401  # the moved file's new address
    assert two["rejected"] == [{"source": "osha", "url": f"{HOST}/files/e.pdf", "reason": "soft_error",
                                "httpStatus": 200, "contentType": "application/pdf", "detectedType": "html"}]
    # Confirmed unchanged: the 304 and the two hubs; not the refused file, whose last check failed.
    assert two["checked"] == [f"{HOST}/files/a.pdf", f"{HOST}/hazcom", f"{HOST}/publications"]
    assert two["ecfr"] == []  # the rule was listed once
    assert two["counts"]["osha"] == {"new": 1, "changed": 1, "gone": 1, "restored": 0, "moved": 1, "checked": 3,
                                     "rejected": 1, "hints": 0, "otherTypes": 0}

    # Another week: the gone file is back with its old bytes, and the error page is the file again.
    feeds.site.set("/files/c.pdf", (200, pdf_bytes(320), PDF))
    feeds.site.set("/files/e.pdf", (200, pdf_bytes(330), PDF))
    feeds.run(days=7)
    three = feeds.manifest()
    restored = feeds.entry(three, "/files/c.pdf", "restored")
    assert (restored["sha256"], restored["httpStatus"], restored["detectedType"]) == (sha(pdf_bytes(320)), 200, "pdf")
    assert f"{HOST}/files/e.pdf" in three["checked"] and three["rejected"] == []


def test_a_run_counts_as_checked_exactly_what_the_feed_lists_as_checked(feeds, signing):
    """The run's "checked and unchanged" (its note, `ssp official status`, the dashboard's Unchanged tile) is what the
    manifest lists as checked: current documents. A picture confirmed, or a URL still gone, is counted apart. (The
    acceptance run of 2026-11-24 found arlweb's 150 against the feed's 147: a picture, a recording, another file.)"""
    feeds.site.set("/publications", (200, page("Publications", "/files/a.pdf", "/files/b.pdf", "/files/c.pdf",
                                               "/files/logo.png"), HTML))
    feeds.site.set("/files/logo.png", (200, PNG, {"content-type": "image/png"}))
    feeds.run()
    feeds.site.set("/files/e.pdf", (410, b"", {}))
    week = feeds.run(days=8)["sources"]["osha"]
    two = feeds.manifest()
    assert ("/files/e.pdf", "gone") in [(item["url"].removeprefix(HOST), item["change"]) for item in two["entries"]]
    # The two hubs and four PDFs; the picture apart.
    assert (week["details"]["checked_unchanged"], week["details"]["checked_other"]) == (6, 1)
    assert two["counts"]["osha"]["checked"] == len(two["checked"]) == 6
    assert f"{HOST}/files/logo.png" not in two["checked"]
    assert "6 checked and unchanged; 1 other URLs checked (still gone or moved, or not documents)" in week["note"]
    later = feeds.run(days=8)["sources"]["osha"]  # the gone file asked again a week on: still gone, no change
    three = feeds.manifest()
    # One hub now (the other reached only the gone file) and the four PDFs; the picture and the gone file apart.
    assert (later["details"]["checked_unchanged"], later["details"]["checked_other"]) == (5, 2)
    assert three["counts"]["osha"]["checked"] == len(three["checked"]) == 5 and three["entries"] == []


def test_every_file_is_signed_with_the_key_whose_public_half_is_published(feeds, signing, caplog):
    feeds.run()
    assert feeds.read("public-key.txt") == signing.encode()  # base64 of 32 raw bytes, nothing else
    assert feeds.read("latest.json") == feeds.read("manifests/1.json")
    for name in ("latest.json", "manifests/1.json"):
        signature = feeds.read(f"{name}.sig")
        assert b"\n" not in signature and len(base64.b64decode(signature, validate=True)) == 64
        assert verifies(signing, feeds.read(name), signature)
        assert feed.verify(signing, feeds.read(name), signature)
    tampered = feeds.read("latest.json").replace(b'"sequence":1', b'"sequence":7')
    assert not verifies(signing, tampered, feeds.read("latest.json.sig"))
    assert not verifies(key_pair()[1], feeds.read("latest.json"), feeds.read("latest.json.sig"))  # another key
    private = os.environ[feed.KEY_VARIABLE].encode()
    assert not [path for path in feeds.root.rglob("*") if path.is_file() and private in path.read_bytes()]
    assert private.decode() not in caplog.text


def test_each_manifest_names_the_one_before_by_its_sequence_and_exact_bytes(feeds, signing):
    for days in (0, 1, 1, 1):
        feeds.run(days=days)
    assert feeds.numbers() == [1, 2, 3, 4]
    data = [feeds.read(f"manifests/{number}.json") for number in (1, 2, 3, 4)]
    for before, after in itertools.pairwise(data):
        earlier, later = json.loads(before), json.loads(after)
        assert (later["previousSequence"], later["previousSha256"]) == (earlier["sequence"], sha(before))
        assert later["window"]["start"] == earlier["window"]["end"]  # the windows meet, so nothing falls between
    assert json.loads(data[-1])["sources"][0]["status"] == "ok"  # a quiet day is still a manifest: a heartbeat


def test_files_go_through_partials_in_order_and_an_interrupted_publish_is_finished(feeds, signing, monkeypatch):
    real_replace, replaced = os.replace, []

    def recording(source, target):
        replaced.append((Path(source).name, Path(target).relative_to(feeds.root / FOLDER).as_posix()))
        real_replace(source, target)

    monkeypatch.setattr(feed.os, "replace", recording)
    feeds.run()
    assert [target for _, target in replaced] == ["manifests/1.json", "manifests/1.json.sig", "public-key.txt",
                                                   "latest.json", "latest.json.sig"]  # latest.json and its sig last
    assert all(source == f"{Path(target).name}.partial" for source, target in replaced)

    def power_cut(source, target):  # the machine stops as latest.json is about to be replaced
        if Path(target).name == "latest.json":
            raise OSError("power cut")
        recording(source, target)

    monkeypatch.setattr(feed.os, "replace", power_cut)
    interrupted = feeds.run(days=1)
    assert interrupted["feed"]["outcome"] == "error" and "power cut" in interrupted["feed"]["note"]
    assert feeds.manifest()["sequence"] == 1 and feeds.numbers() == [1, 2]  # latest.json still names manifest 1
    assert (feeds.root / FOLDER / "latest.json.partial").exists()
    second = feeds.read("manifests/2.json")

    monkeypatch.setattr(feed.os, "replace", recording)
    replaced.clear()
    resumed = feeds.run(days=1)
    # latest.json is brought up to manifest 2 first, then manifest 3 follows it; manifest 2 is never written again.
    assert [target for _, target in replaced] == ["latest.json", "latest.json.sig", "manifests/3.json",
                                                   "manifests/3.json.sig", "latest.json", "latest.json.sig"]
    assert resumed["feed"]["finished"] == ["latest.json", "latest.json.sig"]
    assert resumed["feed"]["partials_removed"] == ["latest.json.partial"]
    assert feeds.read("manifests/2.json") == second
    assert feeds.manifest()["previousSha256"] == sha(second)
    assert not list((feeds.root / FOLDER).rglob("*.partial"))


def test_a_lost_numbered_copy_is_written_again_from_latest_json(feeds, signing):
    feeds.run()
    lost = feeds.read("manifests/1.json")
    for name in ("manifests/1.json", "manifests/1.json.sig"):
        (feeds.root / FOLDER / name).unlink()
    feeds.run(days=1)
    assert feeds.read("manifests/1.json") == lost and feeds.numbers() == [1, 2]
    assert verifies(signing, lost, feeds.read("manifests/1.json.sig"))


def test_a_feed_that_cannot_be_continued_stops_publishing_and_says_why(feeds, signing):
    feeds.run()
    (feeds.root / FOLDER / "manifests" / "2.json").write_bytes(b"<html>not a manifest</html>")
    result = feeds.run(days=1)
    assert result["feed"]["outcome"] == "error" and "manifests/2.json is not JSON" in result["feed"]["note"]
    assert feeds.manifest()["sequence"] == 1
    status = feed.feed_status(feeds.root, feeds.clock.now)
    assert status["last_attempt"]["outcome"] == "error" and status["latest"]["sequence"] == 1


def test_manifests_are_kept_45_days_and_the_newest_always(feeds, signing):
    feeds.run()
    feeds.run(days=20)
    third = feeds.run(days=20)
    assert feeds.numbers() == [1, 2, 3] and third["feed"]["removed"] == []  # the first is 40 days old
    fourth = feeds.run(days=6)
    assert feeds.numbers() == [2, 3, 4] and fourth["feed"]["removed"] == [1]
    assert not (feeds.root / FOLDER / "manifests" / "1.json.sig").exists()
    fifth = feeds.run(days=60)
    assert feeds.numbers() == [5] and fifth["feed"]["removed"] == [2, 3, 4]
    assert feeds.manifest()["previousSha256"] is not None  # the chain goes on though its start is gone


def test_without_a_signing_key_the_run_records_its_findings_and_publishes_nothing(feeds, monkeypatch, caplog):
    monkeypatch.delenv(feed.KEY_VARIABLE, raising=False)
    first = feeds.run()
    assert first["feed"]["outcome"] == "unsigned" and "OFFICIAL_FEED_SIGNING_KEY is not set" in first["feed"]["note"]
    assert first["sources"]["osha"]["status"] == "ok"  # the run itself is complete
    assert not (feeds.root / "exports").exists() and "unsigned feed" in caplog.text
    status = feed.feed_status(feeds.root, feeds.clock.now)
    assert status["latest"] is None and status["last_attempt"]["outcome"] == "unsigned"

    private, public = key_pair()
    monkeypatch.setenv(feed.KEY_VARIABLE, private)
    feeds.run(days=1)
    one = feeds.manifest()
    # The first signed manifest covers what waited: from the first run on.
    assert (one["sequence"], one["window"]["start"], one["counts"]["osha"]["new"]) == (1, iso(START), 8)
    assert verifies(public, feeds.read("latest.json"), feeds.read("latest.json.sig"))


@pytest.mark.parametrize("value", ["not base64 at all!", base64.b64encode(b"thirty-one bytes is not enough").decode()])
def test_a_signing_key_that_is_not_one_is_an_error_that_never_shows_the_key(feeds, monkeypatch, value):
    monkeypatch.setenv(feed.KEY_VARIABLE, value)
    result = feeds.run()
    assert result["feed"]["outcome"] == "error" and feed.KEY_VARIABLE in result["feed"]["note"]
    assert value not in result["feed"]["note"] and not (feeds.root / "exports").exists()
    status = feed.feed_status(feeds.root, feeds.clock.now)
    assert status["last_attempt"]["outcome"] == "error" and value not in status["last_attempt"]["note"]


def test_a_window_too_large_for_one_manifest_is_split_by_time(feeds, signing):
    feeds.run(publish=False)
    result = publish(feeds, limits=Limits(3, 2, 10**6))
    assert result["sequences"] == [1, 2, 3]
    pages = [feeds.manifest(f"manifests/{number}.json") for number in (1, 2, 3)]
    assert [len(page["entries"]) for page in pages] == [3, 3, 2]
    assert pages[0]["window"]["start"] == iso(START) and pages[-1]["window"]["end"] == result["window"]["end"]
    for before, after in itertools.pairwise(pages):
        assert before["window"]["end"] == after["window"]["start"] == after["entries"][0]["observedAt"]
        assert all(item["observedAt"] < before["window"]["end"] for item in before["entries"])
        assert after["previousSha256"] == sha(feeds.read(f"manifests/{before['sequence']}.json"))
    assert sum(page["counts"]["all"]["new"] for page in pages) == 8
    assert feeds.read("latest.json") == feeds.read("manifests/3.json")
    # Each source's outcome is in the manifest whose window holds its end: OSHA's run ended after its last download.
    assert [{item["source"]: item["status"] for item in page["sources"]}["osha"] for page in pages] == [
        "skipped", "skipped", "ok"]

    # A byte limit splits too, and findings of one second that no manifest can hold stop the feed.
    feeds.run(days=1, publish=False)
    small = publish(feeds, limits=Limits(5000, 1, feed.HEADER_BYTES + 200))
    assert len(small["sequences"]) == 2  # the two hubs checked, one in each
    feeds.run(days=1, publish=False)
    with pytest.raises(FeedError, match="alone are more than one manifest holds"):
        publish(feeds, limits=Limits(5000, 0, 10**6))
    assert feed.feed_status(feeds.root, feeds.clock.now)["last_attempt"]["outcome"] == "error"
    assert feeds.numbers() == [1, 2, 3, 4, 5]  # nothing written by the publish that failed


def test_the_window_ends_where_a_crawl_run_another_process_still_has_under_way_began(feeds, signing):
    feeds.run()
    under_way = iso(feeds.clock.now + timedelta(hours=1))
    store = Store(feeds.root / "state" / "crawl-osha.sqlite")
    store.start_run("osha-elsewhere", "osha", under_way)
    store.commit()
    store.close()
    feeds.clock.now += timedelta(hours=5)
    with SiteLock(feeds.root / "state" / "crawl-osha.lock"):  # the other process is crawling
        held = publish(feeds)
    assert held["window"]["end"] == under_way and "a osha crawl run under way since" in held["note"]
    # Once no process holds the lock, an unfinished run is a dead one, and the window runs to the publish again.
    free = publish(feeds)
    assert free["window"] == {"start": under_way, "end": iso(feeds.clock.now)} and free["waiting"] == []


def test_a_crawl_run_whose_process_died_never_holds_the_window_back(feeds, signing):
    """A run killed with its process records no end, and the runs after it show it dead: with the site's lock held by
    another process but no crawl under way (a purge, a watcher's check), the window runs to the publish, and with
    another crawl under way it ends where that crawl began, not where the dead run did."""
    store = Store(feeds.root / "state" / "crawl-osha.sqlite")
    store.start_run("osha-died", "osha", iso(START - timedelta(days=2)))  # its process was killed: no end
    store.commit()
    store.close()
    feeds.run()  # this run's OSHA crawl starts and ends after the dead one
    feeds.clock.now += timedelta(hours=5)
    with SiteLock(feeds.root / "state" / "crawl-osha.lock"):
        quiet = publish(feeds)
    assert quiet["window"]["end"] == iso(feeds.clock.now) and quiet["waiting"] == []
    under_way = iso(feeds.clock.now + timedelta(hours=1))
    store = Store(feeds.root / "state" / "crawl-osha.sqlite")
    store.start_run("osha-elsewhere", "osha", under_way)
    store.commit()
    store.close()
    feeds.clock.now += timedelta(hours=5)
    with SiteLock(feeds.root / "state" / "crawl-osha.lock"):  # another process is crawling
        held = publish(feeds)
    assert held["window"]["end"] == under_way and held["waiting"] == [f"a osha crawl run under way since {under_way}"]


def test_ssp_official_manifest_keygen_and_status(feeds, monkeypatch, capsys, tmp_path):
    monkeypatch.setenv("SAFESTART_DATA_ROOT", str(feeds.root))
    monkeypatch.setattr(cli, "setup_logging", lambda path: None)
    monkeypatch.delenv(feed.KEY_VARIABLE, raising=False)
    key_file = tmp_path / "keys" / "feed.env"
    key_file.parent.mkdir()
    assert cli.main(["official", "keygen", "--out", str(key_file)]) == 0
    printed = capsys.readouterr()
    public = printed.out.strip()
    assert len(base64.b64decode(public, validate=True)) == 32  # the public key, and nothing else, on stdout
    line = next(text for text in key_file.read_text(encoding="utf-8").splitlines() if not text.startswith("#"))
    name, _, private = line.partition("=")
    assert name == feed.KEY_VARIABLE and private not in printed.out + printed.err
    assert feed.public_key(feed.signing_key({name: private})) == public
    if sys.platform != "win32":
        assert key_file.stat().st_mode & 0o777 == 0o600
    with pytest.raises(SystemExit, match="exists already"):
        cli.main(["official", "keygen", "--out", str(key_file)])

    assert cli.main(["official", "manifest"]) == 1  # no key: nothing published
    assert json.loads(capsys.readouterr().out)["outcome"] == "unsigned"
    monkeypatch.setenv(name, private)
    feeds.run(publish=False)
    assert cli.main(["official", "manifest"]) == 0
    assert json.loads(capsys.readouterr().out)["sequences"] == [1]
    assert verifies(public, feeds.read("latest.json"), feeds.read("latest.json.sig"))
    assert cli.main(["official", "status"]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["feed"]["latest"]["sequence"] == 1 and status["feed"]["last_attempt"]["outcome"] == "published"
    assert status["purge"] == {}


def test_a_manifest_holds_only_the_feeds_types_and_counts_the_others(feeds, signing):
    """A picture where a page links to one is a change the feed counts but does not list (SafeStart reads documents)."""
    feeds.site.set("/hazcom", (200, page("Hazard communication", "/files/c.pdf", "/files/e.pdf", "/images/sign.png"),
                               {"content-type": "text/html"}))
    feeds.site.set("/images/sign.png", (200, b"\x89PNG\r\n\x1a\n" + b"\0" * 64, {"content-type": "image/png"}))
    feeds.run()
    one = feeds.manifest()
    assert not [item for item in one["entries"] if item["url"].endswith(".png")]
    assert one["counts"]["osha"]["otherTypes"] == 1 and one["counts"]["all"]["otherTypes"] == 1


def test_a_long_note_is_cut_so_the_lists_always_have_their_room(feeds, signing):
    """A source's note is cut to NOTE_CHARACTERS, so even five notes of escaped characters stay inside HEADER_BYTES and
    a manifest of findings that fill the lists' budget still fits MAX_BYTES."""
    state = OfficialState(feeds.root / "state" / "official.sqlite")
    state.start_run("official-long", started_at=iso(START), until=iso(START + timedelta(hours=1)), trigger="test")
    for source in feed.SOURCES:
        state.start_source("official-long", source, host="example", started_at=iso(START))
        state.end_source("official-long", source, ended_at=iso(START), status="ok", note="\u00e9" * 1999,
                         requests=1, stop_reason="done")
    state.end_run("official-long", ended_at=iso(START), stop_reason="done")
    state.close()
    feeds.clock.now = START + timedelta(minutes=5)
    result = publish(feeds)
    document = feeds.manifest()
    assert {len(source["note"]) for source in document["sources"]} == {feed.NOTE_CHARACTERS}
    header = len(feed.encode(document))  # no findings: all of it is header
    assert header <= feed.HEADER_BYTES and result["manifests"][0]["bytes"] == header
