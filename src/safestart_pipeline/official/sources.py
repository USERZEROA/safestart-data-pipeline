"""The daily run's sources that are not crawled: www.msha.gov's probe, the eCFR titles, the Federal Register's rules.

- www.msha.gov refuses the cloud VM, and its pages are not updated automatically (owner, 2026-11-23). Each run asks
  for robots.txt and the first sitemap once, as the watcher does (watch/sitemaps.py, through its polite client), and
  records what came back: "refused" for HTTP 401, 403, 429 or 451, an HTML page in the sitemap's place or a robots.txt
  that disallows it; "unavailable" for no answer or a 5xx; "error" for another status or malformed XML; and "skipped"
  when the sitemap answers, since the pages still are not updated. A refusal is never worked around.
- eCFR: the titles whose govinfo bulk edition changed since the last run, from the bulk store's versions
  (state/bulk.sqlite, written by the batch flow), each with its issue date and the one before (the AMDDATE of the
  title file, near its start; None when the file is not on this host). The first run looks back FIRST_LOOKBACK.
- Federal Register: the final rules that amend title 29 or 30 of the CFR published since the last run (the day of the
  last complete read is read again, and a rule already recorded is not recorded twice), from the API the watcher
  reads (watch/federal_register.py), at most MAX_PAGES pages a title. The first run looks back FIRST_LOOKBACK.
The eCFR and Federal Register findings are hints for SafeStart's own eCFR check, kept in the hints table of
state/official.sqlite. Each source returns its outcome as the daily crawl does (daily.py): status, note, requests,
stop reason and counts.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx

from ..acquire.bulk import ECFR_TITLES, ecfr_files
from ..crawl.crawler import iso
from ..crawl.sitemaps import REFUSED_STATUSES, served_as_html
from ..crawl.sites import Site
from ..verify import parse_amddate, read_only
from ..watch.federal_register import API, MIN_DELAY
from ..watch.polite import PoliteClient
from ..watch.sitemaps import Refused, SitemapClient, read_sitemap
from .state import OfficialState

FIRST_LOOKBACK = timedelta(days=7)
MAX_PAGES = 5  # 100 rules a page; a title rarely sees ten final rules a month
AMDDATE = re.compile(rb"<AMDDATE>\s*([^<]*?)\s*</AMDDATE>")
HEAD_BYTES = 64 * 1024  # AMDDATE sits about 450 bytes into the govinfo title files
RULE_FIELDS = ("document_number", "effective_on", "publication_date", "cfr_references", "type")


def outcome(status: str, note: str, requests: int = 0, stop_reason: str | None = None, **details: object) -> dict:
    return {"status": status, "note": note, "requests": requests, "stop_reason": stop_reason or status,
            "details": details}


# www.msha.gov ---------------------------------------------------------------------------------------------------------


def probe_msha(site: Site, client: httpx.Client, *, sleep: Callable[[float], None],
               clock: Callable[[], float]) -> dict:
    """One robots.txt and one sitemap request (redirects followed one hop at a time), judged as the watcher judges a
    sitemap."""
    polite = SitemapClient(client, min_delay=site.min_delay, sleep=sleep, clock=clock)
    robots = polite.robots(site.canonical_host)
    if robots.unreachable:
        return outcome("unavailable", f"robots.txt answered {robots.status or 'nothing'}, so nothing was asked",
                       polite.requests)
    url = site.sitemaps[0]
    try:
        _, sitemap = read_sitemap(polite, url)
    except Refused as refusal:
        return outcome("refused", f"{url}: refused, {refusal}; recorded, never worked around", polite.requests)
    except PermissionError as problem:
        return outcome("refused", f"{problem}; recorded, never worked around", polite.requests)
    except httpx.HTTPError as problem:
        return outcome("unavailable", f"{url}: {type(problem).__name__}: {problem}"[:500], polite.requests)
    except ValueError as problem:
        unavailable = str(problem).startswith("HTTP 5")
        return outcome("unavailable" if unavailable else "error", f"{url}: {problem}"[:500], polite.requests)
    return outcome("skipped", f"{url} answered with a sitemap {sitemap.kind} of {len(sitemap.entries):,} entries;"
                              " www.msha.gov pages are not updated automatically (owner, 2026-11-23)", polite.requests,
                   stop_reason="answered", entries=len(sitemap.entries))


# eCFR -----------------------------------------------------------------------------------------------------------------


def issue_date(root: Path, sha256: str) -> str | None:
    """The issue date (AMDDATE, as YYYY-MM-DD) of a govinfo eCFR title file held in the bulk store's objects."""
    path = root / "raw" / "bulk" / "objects" / sha256[:2] / sha256[2:4] / sha256
    try:
        with path.open("rb") as handle:
            head = handle.read(HEAD_BYTES)
    except FileNotFoundError:
        return None
    found = AMDDATE.search(head)
    return parse_amddate(found[1].decode("utf-8", "replace")) if found else None


def ecfr_hints(root: Path, state: OfficialState, *, now: datetime, run_id: str) -> dict:
    """The eCFR titles whose govinfo bulk edition changed since the last run, recorded as hints."""
    path = root / "state" / "bulk.sqlite"
    if not path.exists():
        return outcome("skipped", "no bulk store (state/bulk.sqlite) on this host yet")
    since = state.mark("ecfr:versions_seen_until") or iso(now - FIRST_LOOKBACK)
    titles = {item.url: title for item, title in zip(ecfr_files(), ECFR_TITLES, strict=True)}
    db = read_only(path)
    try:
        marks = ", ".join("?" for _ in titles)
        versions = db.execute(f"select url, sha256, first_seen_at from versions where url in ({marks})"
                              " order by url, first_seen_at, sha256", list(titles)).fetchall()
    finally:
        db.close()
    found, newest, before = [], since, {}
    for version in versions:
        title, previous = titles[version["url"]], before.get(version["url"])
        before[version["url"]] = version
        if version["first_seen_at"] <= since:
            continue
        newest = max(newest, version["first_seen_at"])
        hint = {"issue_date": issue_date(root, version["sha256"]), "sha256": version["sha256"],
                "previous_issue_date": issue_date(root, previous["sha256"]) if previous else None}
        if state.add_hint("ecfr", title, observed_at=iso(now), run_id=run_id, **hint):
            found.append({"title": title, **hint})
    state.set_mark("ecfr:versions_seen_until", newest)
    state.commit()
    note = "; ".join(f"title {item['title']}: issue {item['issue_date'] or 'date not read'}"
                     f" (was {item['previous_issue_date'] or 'none held'})" for item in found)
    return outcome("ok", note or f"no new govinfo edition since {since}", stop_reason="done", hints=found)


# Federal Register -----------------------------------------------------------------------------------------------------


def rules_page(title: int, since: str) -> str:
    """The API's first page of final rules affecting a CFR title published on or after `since` (YYYY-MM-DD)."""
    params = [("conditions[type][]", "RULE"), ("conditions[cfr][title]", str(title)),
              ("conditions[publication_date][gte]", since), ("order", "newest"), ("per_page", "100"),
              *(("fields[]", name) for name in RULE_FIELDS)]
    return str(httpx.URL(API, params=params))


def federal_register_hints(client: httpx.Client, state: OfficialState, *, now: datetime, run_id: str,
                           sleep: Callable[[float], None], clock: Callable[[], float]) -> dict:
    """The final rules amending title 29 or 30 published since the last complete read, recorded as hints."""
    today = now.astimezone(UTC).date().isoformat()
    since = state.mark("federal_register:read_through") or (now - FIRST_LOOKBACK).astimezone(UTC).date().isoformat()
    polite = PoliteClient(client, min_delay=MIN_DELAY, sleep=sleep, clock=clock)
    found, read, limited = [], 0, []
    try:
        for title in ECFR_TITLES:
            url: str | None = rules_page(title, since)
            pages = 0
            while url:
                if pages == MAX_PAGES:
                    limited.append(title)
                    break
                answer = polite.get(url)
                pages += 1
                if answer.status in REFUSED_STATUSES:
                    return outcome("refused", f"the API answered HTTP {answer.status}; recorded, never worked around",
                                   polite.requests)
                if answer.status >= 500:
                    return outcome("unavailable", f"the API answered HTTP {answer.status}", polite.requests)
                if answer.status != 200:
                    return outcome("error", f"the API answered HTTP {answer.status} for {answer.url}", polite.requests)
                try:
                    data = json.loads(answer.content)
                except ValueError:
                    if served_as_html(answer.content, None):
                        return outcome("refused", "the API served an HTML page instead of its documents; recorded,"
                                                  " never worked around", polite.requests)
                    raise
                if not isinstance(data, dict):
                    return outcome("error", f"{answer.url} answered with something other than a document list",
                                   polite.requests)
                for document in data.get("results") or []:
                    read += 1
                    number = document.get("document_number")
                    named = {reference.get("title") for reference in document.get("cfr_references") or []}
                    if not number or (named and title not in named):
                        continue
                    hint = {"document_number": number, "effective_on": document.get("effective_on"),
                            "publication_date": document.get("publication_date")}
                    if state.add_hint("federal_register", title, observed_at=iso(now), run_id=run_id, **hint):
                        found.append({"title": title, **hint})
                url = data.get("next_page_url")
    except PermissionError as problem:
        return outcome("refused", f"{problem}; recorded, never worked around", polite.requests)
    except httpx.HTTPError as problem:
        return outcome("unavailable", f"{type(problem).__name__}: {problem}"[:500], polite.requests)
    except ValueError as problem:
        return outcome("error", f"an answer that is not JSON: {problem}"[:500], polite.requests)
    finally:
        state.commit()
    if not limited:  # every page was read: the next run starts from today
        state.set_mark("federal_register:read_through", today)
        state.commit()
    note = (f"{len(found)} final rules amending title 29 or 30 since {since}"
            + (": " + ", ".join(f"{item['document_number']} (title {item['title']}, effective"
                                f" {item['effective_on'] or 'not given'})" for item in found[:10]) if found else "")
            + (f"; stopped after {MAX_PAGES} pages for title {', '.join(map(str, limited))}" if limited else ""))
    return outcome("partial" if limited else "ok", note, polite.requests, stop_reason="done", read=read, hints=found)
