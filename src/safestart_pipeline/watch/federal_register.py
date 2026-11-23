"""New Federal Register documents from MSHA and OSHA (rules, proposed rules, notices), through the official API.

Each check asks the API for the two agencies' newest documents, newest first, a page of 100 at a time, and publishes a
regulation event for each document not published before. A page usually spans months (on 2026-11-23 the newest 100
went back to June), so one request a check is the rule; the next page is read only when a whole page is new, up to a
limit. A first check publishes the newest page, not the archive behind it. robots.txt is read first; the API sets no
crawl delay, and the watcher still waits a second between its requests.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from typing import Any

import httpx

from ..stream.events import Event
from .checks import Check
from .polite import PoliteClient
from .publish import Publisher, publish_new
from .state import WatchState

SOURCE = "federal-register"
API = "https://www.federalregister.gov/api/v1/documents.json"
AGENCIES = {"mine-safety-and-health-administration": "MSHA", "occupational-safety-and-health-administration": "OSHA"}
FIELDS = ("document_number", "title", "type", "html_url", "publication_date", "agencies")
PER_PAGE = 100
MAX_PAGES = 10
MIN_DELAY = 1.0


def first_page() -> str:
    params = [("conditions[agencies][]", slug) for slug in AGENCIES]
    params += [("order", "newest"), ("per_page", str(PER_PAGE)), *(("fields[]", name) for name in FIELDS)]
    return str(httpx.URL(API, params=params))


def regulation_event(document: dict[str, Any]) -> Event | None:
    """A regulation event for one API result; None when it lacks its number or publication date."""
    number, published = document.get("document_number"), document.get("publication_date")
    if not number or not published:
        return None
    ours = [agency for agency in document.get("agencies") or [] if agency.get("slug") in AGENCIES]
    by = "/".join(AGENCIES[agency["slug"]] for agency in ours) or "Federal Register"
    return Event.new(
        kind="regulation", key=number, mode="live", occurred_at=published,
        title=f"{by}: {document.get('title') or number}",
        details={"agency": "; ".join(agency.get("name") or agency["slug"] for agency in ours) or None,
                 "document_number": number, "type": document.get("type"), "html_url": document.get("html_url"),
                 "publication_date": published},
        source={"api": "federalregister"})


def check_federal_register(state: WatchState, publisher: Publisher, client: httpx.Client, *,
                           sleep: Callable[[float], None] = time.sleep,
                           clock: Callable[[], float] = time.monotonic) -> Check:
    polite = PoliteClient(client, min_delay=MIN_DELAY, sleep=sleep, clock=clock)
    first_check = not state.any_emitted("regulation")
    url: str | None = first_page()
    events: list[Event] = []
    pages = malformed = 0
    limited = False
    try:
        while url:
            if pages == MAX_PAGES:
                limited = True
                break
            answer = polite.get(url)
            if answer.status != 200:
                return Check(SOURCE, "error", note=f"HTTP {answer.status} from {answer.url}")
            data = json.loads(answer.content)
            if not isinstance(data, dict):
                return Check(SOURCE, "error", note=f"{answer.url} answered with something other than a document list")
            page = [regulation_event(document) for document in data.get("results") or []]
            malformed += page.count(None)
            page_events = [event for event in page if event is not None]
            events += page_events
            pages += 1
            if first_check or not page_events or state.emitted([event.event_id for event in page_events]):
                break
            url = data.get("next_page_url")
    except PermissionError as problem:
        return Check(SOURCE, "skipped", note=str(problem))
    except (httpx.HTTPError, ValueError) as problem:
        return Check(SOURCE, "error", note=f"{type(problem).__name__}: {problem}"[:300])
    found = publish_new(events, publisher, state)
    note = f"{len(events)} documents read in {pages} page{'s' if pages > 1 else ''}"
    if malformed:
        note += f"; {malformed} without a number or date left out"
    if limited:
        note += f"; stopped after {MAX_PAGES} pages"
    return Check(SOURCE, "ok" if found else "unchanged", found, note)
