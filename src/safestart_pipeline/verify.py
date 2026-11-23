"""Completeness reports (`ssp verify`).

For a crawl: does every URL it learned of have a final state, is every downloaded file on disk with its recorded size
(and, with `rehash`, its SHA-256), where do redirects and outside links lead, and, with a live check, does today's
sitemap still match the inventory. For the bulk downloads: is each file present, verified and intact, and, with a live
check, is it still the file the source lists today, and does each eCFR title hold every section the official eCFR
structure lists.

Reading never changes a crawl's state. Live checks obey robots.txt and wait between requests to each host, and they
follow a redirect one hop at a time under the same rules (watch/polite.py does the same for the watchers). A live
request that fails or answers with an error status is recorded in the report and named in the verdict; the rest of the
report is still written, so one unavailable page never costs the rehash of every file.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import time
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import httpx
from lxml import etree

from .acquire.bulk import ECFR_TITLES, MSHA_LISTING, msha_dataset_links
from .config import USER_AGENT
from .crawl.robots import Robots
from .crawl.sitemaps import parse_sitemap
from .crawl.sites import Site
from .crawl.urls import normalize

EXAMPLES = 25
WAITING = ("pending", "retry")
ECFR_API = "https://www.ecfr.gov/api/versioner/v1"
DASHES = str.maketrans({"–": " - ", "—": " - "})
MAX_REDIRECTS = 5
GONE = (404, 410)
# What a live request may end in besides an answer: a transport error, a refusal by robots.txt, an answer that is not
# what was asked for (an error status, JSON or HTML that does not parse).
LIVE_ERRORS = (httpx.HTTPError, PermissionError, ValueError, KeyError, etree.LxmlError)


def read_only(path: Path) -> sqlite3.Connection:
    """A connection that only reads. The stores are SQLite databases in WAL mode: a reader works through their -wal
    and -shm files, creating them when they are missing. Where it cannot (the containers mount the data root
    read-only) and they are missing, no connection has the database open, so every change is in the main file, which
    is then opened as immutable: read as it is, without those files."""
    if not path.exists():
        raise FileNotFoundError(path)
    sidecars = all(path.with_name(f"{path.name}-{suffix}").exists() for suffix in ("wal", "shm"))
    immutable = "&immutable=1" if not sidecars and not os.access(path.parent, os.W_OK) else ""
    db = sqlite3.connect(f"file:{path.as_posix()}?mode=ro{immutable}", uri=True)
    db.row_factory = sqlite3.Row
    return db


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def object_path(data_root: Path, kind: str, digest: str) -> Path:
    return data_root / "raw" / kind / "objects" / digest[:2] / digest[2:4] / digest


class Polite:
    """GET and HEAD for live checks: robots.txt per host, and at least `min_delay` seconds (or the host's Crawl-delay)
    between requests to the same host. A redirect is followed by a request of its own under the same rules, never
    inside httpx, so no hop skips the wait: OSHA asks for 10 seconds, and its sitemap.xml answers 301."""

    def __init__(self, client: httpx.Client, *, min_delay: float = 1.0, sleep: Callable[[float], None] = time.sleep,
                 clock: Callable[[], float] = time.monotonic):
        self.client, self.min_delay, self.sleep, self.clock = client, min_delay, sleep, clock
        self.robots: dict[str, Robots] = {}
        self.last: dict[str, float] = {}
        self.requests = 0

    def _wait(self, host: str) -> None:
        rules = self.robots[host]
        due = self.last.get(host, float("-inf")) + max(self.min_delay, rules.crawl_delay() or 0.0)
        if self.clock() < due:
            self.sleep(due - self.clock())
        self.last[host] = self.clock()
        self.requests += 1

    def _rules(self, url: str) -> Robots:
        host = httpx.URL(url).host
        if host not in self.robots:
            try:
                answer = self.client.get(f"https://{host}/robots.txt", follow_redirects=True)
                self.robots[host] = Robots(answer.status_code, answer.text if answer.is_success else "", USER_AGENT)
            except httpx.HTTPError:
                self.robots[host] = Robots(None, "", USER_AGENT)
            self.last[host] = self.clock()
            self.requests += 1
        return self.robots[host]

    def allowed(self, url: str) -> bool:
        return self._rules(url).allowed(url)

    def request(self, method: str, url: str) -> httpx.Response:
        """The answer at the end of the redirects. PermissionError when robots.txt does not allow a hop."""
        for _ in range(MAX_REDIRECTS + 1):
            if not self.allowed(url):
                raise PermissionError(f"robots.txt disallows {url}")
            self._wait(httpx.URL(url).host)
            response = self.client.request(method, url, follow_redirects=False)
            if not response.is_redirect:
                return response
            url = urljoin(url, response.headers["location"])
        raise httpx.TooManyRedirects(f"more than {MAX_REDIRECTS} redirects, the last to {url}")


def successful(answer: httpx.Response) -> httpx.Response:
    """The answer if its status is 2xx; ValueError otherwise, so an error page is never read as data."""
    if not answer.is_success:
        raise ValueError(f"HTTP {answer.status_code} from {answer.url}")
    return answer


# Crawls ---------------------------------------------------------------------------------------------------------------


def verify_crawl(site: Site, data_root: Path, *, rehash: bool = False, live: Polite | None = None) -> dict:
    db = read_only(data_root / "state" / f"crawl-{site.name}.sqlite")
    try:
        report: dict = {"site": site.name}
        states = {row[0]: row[1] for row in db.execute("select state, count(*) from urls group by state")}
        report["states"] = states
        report["waiting"] = sum(states.get(state, 0) for state in WAITING)
        report["sitemap"] = {
            "urls": db.execute("select count(*) from urls where in_sitemap = 1").fetchone()[0],
            "states": {row[0]: row[1] for row in db.execute(
                "select state, count(*) from urls where in_sitemap = 1 group by state")},
            "final_not_fetched": [dict(row) for row in db.execute(
                "select url, state, skip_reason, http_status, error from urls where in_sitemap = 1"
                " and state not in ('fetched', 'unchanged', 'pending', 'retry') order by state, url limit ?",
                (EXAMPLES,))],
        }
        report["discovered_only"] = db.execute("select count(*) from urls where in_sitemap = 0").fetchone()[0]
        skipped = {}
        for reason, count in db.execute("select coalesce(skip_reason, '?'), count(*) from urls where state = 'skipped'"
                                        " group by 1 order by 2 desc"):
            rows = db.execute("select url, content_length from urls where state = 'skipped'"
                              " and coalesce(skip_reason, '?') = ? order by url limit ?", (reason, EXAMPLES)).fetchall()
            skipped[reason] = {"count": count, "examples": [
                row["url"] + (f"  ({row['content_length'] / 1048576:.1f} MB)"
                              if reason == "size_cap" and row["content_length"] else "") for row in rows]}
        report["skipped"] = skipped
        report["failed"] = {state: {"count": states.get(state, 0), "examples": [dict(row) for row in db.execute(
            "select url, http_status, attempts, error from urls where state = ? order by url limit ?",
            (state, EXAMPLES))]} for state in ("not_found", "error")}
        targets = Counter()
        for row in db.execute("select url, location from urls where state = 'redirect'"):
            target = site.in_scope(normalize(row["location"] or "", row["url"])) if row["location"] else None
            if target is None:
                targets["leaves the site"] += 1
            else:
                known = db.execute("select state from urls where url = ?", (target,)).fetchone()
                targets[f"target {known['state'] if known else 'not recorded'}"] += 1
        report["redirects"] = dict(targets)
        outside = Counter(urlsplit(row[0]).hostname or "?" for row in db.execute(
            "select distinct to_url from links where to_url not in (select url from urls)"))
        report["outside_links"] = {"distinct": sum(outside.values()), "by_host": dict(outside.most_common(EXAMPLES))}
        report["sitemaps_read"] = [dict(row) for row in db.execute(
            "select url, http_status, kind, entries, error from sitemaps order by url")]
        report["objects"] = check_objects(db, data_root, site.name, rehash)
        report["inventory_at"] = (db.execute(
            "select value from meta where key = 'inventory_at'").fetchone() or [None])[0]
        report["last_stop"] = (db.execute("select value from meta where key = 'last_stop'").fetchone() or [None])[0]
        if live is not None and site.sitemaps:
            report["live_sitemap"] = compare_sitemaps(site, db, live)
    finally:
        db.close()
    report["verdict"] = crawl_verdict(report)
    return report


def check_objects(db: sqlite3.Connection, data_root: Path, kind: str, rehash: bool) -> dict:
    """Every fetched URL's stored file: present, of the recorded size, and (with rehash) of the recorded SHA-256."""
    missing, wrong_size, wrong_hash, unique = [], [], [], {}
    rows = db.execute(
        "select url, sha256, content_length from urls where state in ('fetched', 'unchanged')").fetchall()
    for row in rows:
        digest = row["sha256"]
        path = object_path(data_root, kind, digest) if digest else None
        if path is None or not path.exists():
            missing.append(row["url"])
            continue
        if row["content_length"] is not None and path.stat().st_size != row["content_length"]:
            wrong_size.append(row["url"])
        unique[digest] = path
    if rehash:
        wrong_hash = [str(path) for digest, path in unique.items() if sha256_of(path) != digest]
    return {"urls": len(rows), "files": len(unique), "missing": len(missing), "missing_examples": missing[:EXAMPLES],
            "wrong_size": len(wrong_size), "wrong_size_examples": wrong_size[:EXAMPLES], "rehashed": rehash,
            "wrong_hash": len(wrong_hash), "wrong_hash_examples": wrong_hash[:EXAMPLES]}


def compare_sitemaps(site: Site, db: sqlite3.Connection, live: Polite) -> dict:
    """Today's sitemaps against the inventory: URLs added or dropped since, and pages whose lastmod is after our
    fetch."""
    queue, seen, today, errors = list(site.sitemaps), set(), {}, []
    while queue:
        url = queue.pop(0)
        if url in seen:
            continue
        seen.add(url)
        try:
            answer = live.request("GET", url)
            sitemap = parse_sitemap(answer.content)
        except (httpx.HTTPError, PermissionError, ValueError) as problem:
            errors.append(f"{url}: {problem}"[:300])
            continue
        for entry in sitemap.entries:
            target = site.in_scope(normalize(entry.loc, url))
            if target is None:
                continue
            if sitemap.kind == "index":
                queue.append(target)
            else:
                today[target] = entry.lastmod
    known = {row["url"]: row for row in db.execute("select url, in_sitemap, fetched_at from urls")}
    listed = {url for url, row in known.items() if row["in_sitemap"]}
    added = sorted(url for url in today if url not in known)
    dropped = sorted(url for url in listed if url not in today)
    changed = sorted(url for url, lastmod in today.items() if url in known and lastmod and known[url]["fetched_at"]
                     and lastmod[:19] > known[url]["fetched_at"][:19])
    return {"sitemaps": len(seen), "errors": errors, "urls_today": len(today), "added": len(added),
            "added_examples": added[:EXAMPLES], "dropped": len(dropped), "dropped_examples": dropped[:EXAMPLES],
            "changed_after_fetch": len(changed), "changed_examples": changed[:EXAMPLES]}


def crawl_verdict(report: dict) -> dict:
    reasons = []
    if report["waiting"]:
        reasons.append(f"{report['waiting']} URLs still waiting (crawl not finished)")
    objects = report["objects"]
    for key, label in (("missing", "downloaded files missing on disk"), ("wrong_size", "files of the wrong size"),
                       ("wrong_hash", "files whose SHA-256 differs")):
        if objects[key]:
            reasons.append(f"{objects[key]} {label}")
    live = report.get("live_sitemap")
    if live:
        if live["errors"]:
            reasons.append(f"{len(live['errors'])} sitemaps could not be read today")
        if live["added"]:
            reasons.append(f"{live['added']} sitemap URLs added since the inventory")
        if live["changed_after_fetch"]:
            reasons.append(f"{live['changed_after_fetch']} pages changed after they were downloaded")
    return {"complete": not reasons, "reasons": reasons}


# Bulk downloads -------------------------------------------------------------------------------------------------------


def verify_bulk(data_root: Path, *, rehash: bool = True, live: Polite | None = None,
                check_listing: bool = True) -> dict:
    db = read_only(data_root / "state" / "bulk.sqlite")
    try:
        files, problems = [], []
        for row in db.execute("select * from files order by source, name"):
            entry = {key: row[key] for key in ("source", "name", "url", "state", "content_length", "last_modified",
                                               "fetched_at", "verified", "verify_note", "error")}
            path = object_path(data_root, "bulk", row["sha256"]) if row["sha256"] else None
            entry["present"] = bool(path and path.exists())
            entry["size_ok"] = entry["present"] and (row["content_length"] is None
                                                     or path.stat().st_size == row["content_length"])
            entry["hash_ok"] = (sha256_of(path) == row["sha256"]) if rehash and entry["present"] else None
            if (row["state"] not in ("fetched", "unchanged") or not entry["present"] or not entry["size_ok"]
                    or entry["hash_ok"] is False or not row["verified"]):
                problems.append(row["name"])
            files.append(entry)
        report: dict = {"files": files, "problems": problems, "listings": [dict(row) for row in db.execute(
            "select source, listed_at, page_url, files from listings order by listed_at")]}
        if live is not None:
            held = [entry for entry in files if entry["source"] == "msha-ogd"]
            if check_listing:
                report["msha_listing_today"] = compare_listing(held, live)
            report["ecfr_today"] = [compare_ecfr_title(title, data_root, db, live) for title in ECFR_TITLES]
    finally:
        db.close()
    reasons = [f"{len(problems)} files missing, unverified or damaged: {', '.join(problems[:10])}"] if problems else []
    listing = report.get("msha_listing_today")
    if listing:
        if listing.get("error"):
            reasons.append(f"MSHA's open-data page not compared today: {listing['error']}")
        if listing.get("new"):
            reasons.append(f"{len(listing['new'])} files listed today that we do not hold")
        if listing.get("gone"):
            reasons.append(f"{len(listing['gone'])} held files no longer served")
        if listing.get("changed"):
            reasons.append(f"{len(listing['changed'])} files changed size since our download"
                           " (refresh with the acquire flow)")
        if listing.get("errors"):
            reasons.append(f"{len(listing['errors'])} held files not checked today: {'; '.join(listing['errors'][:3])}")
    for title in report.get("ecfr_today", []):
        if title.get("error"):
            reasons.append(f"title {title['title']}: {title['error']}")
        if title.get("missing_sections"):
            reasons.append(f"title {title['title']}: {title['missing_sections']} official sections not in our file")
        if title.get("behind"):
            reasons.append(f"title {title['title']}: our file is amended to {title['our_amended']},"
                           f" eCFR to {title['ecfr_latest_amended_on']}")
    report["verdict"] = {"complete": not reasons, "reasons": reasons}
    return report


def compare_listing(held: list[dict], live: Polite) -> dict:
    """The MSHA open-data page today against the files we hold: new or delisted files, and files republished since. A
    page that cannot be read is an error rather than an empty listing, which would show every held file as delisted;
    a held file that cannot be asked about is named under errors."""
    try:
        listed = msha_dataset_links(successful(live.request("GET", MSHA_LISTING)).content, MSHA_LISTING)
        if not listed:
            raise ValueError("it lists no data sets; its layout may have changed")
    except LIVE_ERRORS as problem:
        return {"error": str(problem)[:300]}
    ours = {entry["url"]: entry for entry in held}
    changed, rewritten, gone, errors = [], [], [], []
    for url, entry in ours.items():
        try:
            answer = live.request("HEAD", url)
            if answer.status_code in GONE:
                gone.append(entry["name"])
                continue
            successful(answer)
        except LIVE_ERRORS as problem:
            errors.append(f"{entry['name']}: {problem}"[:300])
            continue
        size, modified = answer.headers.get("content-length"), answer.headers.get("last-modified")
        record = {"name": entry["name"], "ours": [entry["content_length"], entry["last_modified"]],
                  "today": [size and int(size), modified]}
        if size and entry["content_length"] is not None and int(size) != entry["content_length"]:
            changed.append(record)
        elif modified and modified != entry["last_modified"]:
            rewritten.append(record)  # MSHA rewrites its files daily; the same size usually means the same data
    return {"listed_today": len(listed), "held": len(ours), "new": sorted(url for url in listed if url not in ours),
            "delisted": sorted(entry["name"] for url, entry in ours.items() if url not in listed), "gone": gone,
            "changed": changed, "rewritten_same_size": rewritten, "errors": errors}


def sections_in(path: Path) -> tuple[set[str], str | None]:
    """Section numbers (DIV8 TYPE="SECTION") of a govinfo eCFR title file, and its amendment date (AMDDATE)."""
    sections, amended = set(), None
    for _, element in etree.iterparse(str(path), events=("end",), huge_tree=True, resolve_entities=False):
        if element.tag == "DIV8" and element.get("TYPE") == "SECTION":
            sections.add(clean_section(element.get("N") or ""))
        elif element.tag == "AMDDATE" and amended is None:
            amended = (element.text or "").strip()
        if element.tag in ("DIV5", "DIV6", "DIV8"):
            element.clear()
    return sections, amended


def clean_section(number: str) -> str:
    return " ".join(number.replace("§", "").translate(DASHES).split())


def official_sections(structure: dict) -> dict[str, bool]:
    """Every section of an eCFR structure document, and whether it is reserved."""
    official, stack = {}, [structure]
    while stack:
        node = stack.pop()
        if node.get("type") == "section":
            official[clean_section(str(node.get("identifier") or ""))] = bool(node.get("reserved"))
        stack.extend(node.get("children") or [])
    return official


def compare_ecfr_title(title: int, data_root: Path, db: sqlite3.Connection, live: Polite) -> dict:
    """One title: the sections the official eCFR structure lists today against those in our file, and its currency.
    What a failed request leaves undone is named under error; the rest is compared all the same."""
    url = f"https://www.govinfo.gov/bulkdata/ECFR/title-{title}/ECFR-title{title}.xml"
    row = db.execute("select sha256, last_modified from files where url = ?", (url,)).fetchone()
    if row is None or not row["sha256"]:
        return {"title": title, "error": "not downloaded"}
    ours, amended = sections_in(object_path(data_root, "bulk", row["sha256"]))
    our_date = parse_amddate(amended)
    result: dict = {"title": title, "our_sections": len(ours), "our_amended": our_date or amended,
                    "our_last_modified": row["last_modified"]}
    errors = []
    try:
        titles = successful(live.request("GET", f"{ECFR_API}/titles.json")).json()["titles"]
        info = {item["number"]: item for item in titles}[title]
        structure = f"{ECFR_API}/structure/{info['up_to_date_as_of']}/title-{title}.json"
        official = official_sections(successful(live.request("GET", structure)).json())
    except LIVE_ERRORS as problem:
        errors.append(f"eCFR API: {problem}")
    else:
        result |= {"official_sections": len(official), "official_reserved": sum(official.values()),
                   "missing_sections": len(set(official) - ours),
                   "missing_examples": sorted(set(official) - ours)[:EXAMPLES],
                   "extra_sections": len(ours - set(official)),
                   "extra_examples": sorted(ours - set(official))[:EXAMPLES],
                   "ecfr_latest_amended_on": info.get("latest_amended_on"),
                   "ecfr_up_to_date_as_of": info.get("up_to_date_as_of"),
                   "behind": bool(our_date and info.get("latest_amended_on") and our_date < info["latest_amended_on"])}
    try:
        source_today = successful(live.request("HEAD", url)).headers.get("last-modified")
    except LIVE_ERRORS as problem:
        errors.append(f"govinfo: {problem}")
    else:
        result |= {"source_last_modified_today": source_today,
                   "source_newer": bool(source_today and row["last_modified"] and source_today != row["last_modified"])}
    if errors:
        result["error"] = "; ".join(errors)[:500]
    return result


def parse_amddate(value: str | None) -> str | None:
    """'Nov. 9, 2026' -> '2026-11-09' (govinfo writes abbreviated month names with a period)."""
    if not value:
        return None
    names = ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec")
    months = {name: number for number, name in enumerate(names, 1)}
    parts = value.replace(".", " ").replace(",", " ").split()
    try:
        return f"{int(parts[2]):04d}-{months[parts[0][:3].lower()]:02d}-{int(parts[1]):02d}"
    except (IndexError, KeyError, ValueError):
        return None
