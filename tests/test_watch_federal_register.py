from __future__ import annotations

import httpx
import pytest

from safestart_pipeline.watch.federal_register import AGENCIES, check_federal_register
from safestart_pipeline.watch.publish import ListPublisher
from safestart_pipeline.watch.state import WatchState

# The shape of the real robots.txt and API answer (checked once on 2026-11-23); the documents are made up.
ROBOTS = "User-Agent: *\nDisallow: /documents/search\nDisallow: /my/\n"
OSHA = {"raw_name": "Occupational Safety and Health Administration", "name": "Occupational Safety and Health Administration",
        "id": 386, "parent_id": 271, "slug": "occupational-safety-and-health-administration"}
MSHA = {"raw_name": "Mine Safety and Health Administration", "name": "Mine Safety and Health Administration", "id": 330,
        "parent_id": 271, "slug": "mine-safety-and-health-administration"}
LABOR = {"raw_name": "DEPARTMENT OF LABOR", "name": "Labor Department", "id": 271, "parent_id": None,
         "slug": "labor-department"}


def document(number: int, *agencies: dict, kind: str = "Notice") -> dict:
    return {"document_number": f"2026-{number:05d}", "title": f"Synthetic document {number}", "type": kind,
            "html_url": f"https://www.federalregister.gov/documents/2026/11/18/2026-{number:05d}/synthetic",
            "publication_date": f"2026-11-{number % 28 + 1:02d}", "agencies": [LABOR, *(agencies or (OSHA,))]}


class FakeApi:
    def __init__(self, pages: list[list[dict]], robots: str = ROBOTS, status: int = 200):
        self.pages, self.robots, self.status = pages, robots, status
        self.requests: list[httpx.URL] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request.url)
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text=self.robots)
        if self.status != 200:
            return httpx.Response(self.status, text="busy")
        page = int(request.url.params.get("page", "1"))
        more = page < len(self.pages)
        following = f"https://www.federalregister.gov/api/v1/documents?format=json&order=newest&page={page + 1}"
        return httpx.Response(200, json={"count": 4998, "total_pages": len(self.pages),
                                         "next_page_url": following if more else None, "results": self.pages[page - 1]})

    def api_requests(self) -> list[httpx.URL]:
        return [url for url in self.requests if url.path != "/robots.txt"]


@pytest.fixture
def state(tmp_path):
    state = WatchState(tmp_path / "watch.sqlite")
    yield state
    state.close()


def check(state: WatchState, api: FakeApi, publisher: ListPublisher, waits: list[float] | None = None):
    client = httpx.Client(transport=httpx.MockTransport(api.handler))
    clock = [0.0]

    def sleep(seconds: float) -> None:
        (waits if waits is not None else []).append(seconds)
        clock[0] += seconds

    try:
        return check_federal_register(state, publisher, client, sleep=sleep, clock=lambda: clock[0])
    finally:
        client.close()


def test_a_first_check_publishes_the_newest_page_of_msha_and_osha_documents(state):
    api = FakeApi([[document(3), document(2, MSHA, kind="Rule"), document(1, MSHA, OSHA)], [document(0)]])
    publisher = ListPublisher()
    result = check(state, api, publisher)
    assert (result.status, result.found, result.note) == ("ok", 3, "3 documents read in 1 page")
    (request,) = api.api_requests()  # the archive behind the first page is not read
    assert request.path == "/api/v1/documents.json"
    assert request.params.get_list("conditions[agencies][]") == list(AGENCIES)
    assert (request.params["order"], request.params["per_page"]) == ("newest", "100")
    assert request.params.get_list("fields[]") == ["document_number", "title", "type", "html_url", "publication_date",
                                                   "agencies"]
    events = {event.event_id: event for event in publisher.events}
    rule = events["regulation:2026-00002"]
    assert rule.title == "MSHA: Synthetic document 2" and rule.occurred_at == "2026-11-03T00:00:00Z"
    assert rule.details == {"agency": "Mine Safety and Health Administration", "document_number": "2026-00002",
                            "type": "Rule", "html_url": document(2)["html_url"], "publication_date": "2026-11-03"}
    assert (rule.mode, rule.topic, rule.source, rule.mine_id) == ("live", "safety.sources", {"api": "federalregister"}, None)
    both = events["regulation:2026-00001"]
    assert both.title.startswith("MSHA/OSHA: ")
    assert both.details["agency"] == "Mine Safety and Health Administration; Occupational Safety and Health Administration"


def test_later_checks_publish_only_documents_not_seen_before(state):
    check(state, FakeApi([[document(3), document(2), document(1)]]), ListPublisher())
    api, publisher = FakeApi([[document(4), document(3), document(2)], [document(1)]]), ListPublisher()
    result = check(state, api, publisher)
    assert [event.event_id for event in publisher.events] == ["regulation:2026-00004"]
    assert (result.status, result.found) == ("ok", 1) and len(api.api_requests()) == 1

    quiet = check(state, FakeApi([[document(4), document(3)]]), ListPublisher())
    assert (quiet.status, quiet.found) == ("unchanged", 0)


def test_a_page_that_is_all_new_leads_to_the_next_one(state):
    check(state, FakeApi([[document(3), document(2)]]), ListPublisher())
    api, publisher, waits = FakeApi([[document(7), document(6), document(5)], [document(4), document(3)]]), ListPublisher(), []
    result = check(state, api, publisher, waits)
    assert [event.event_id for event in publisher.events] == [f"regulation:2026-0000{n}" for n in (7, 6, 5, 4)]
    assert result.note == "5 documents read in 2 pages" and len(api.api_requests()) == 2
    assert waits and min(waits) >= 1.0  # at least a second between requests, though the API asks for no delay


def test_robots_txt_and_failures_are_respected_and_reported(state):
    refused = FakeApi([[document(1)]], robots="User-agent: *\nDisallow: /api/\n")
    result = check(state, refused, ListPublisher())
    assert result.status == "skipped" and "robots.txt disallows" in result.note and refused.api_requests() == []

    busy = check(state, FakeApi([[document(1)]], status=503), ListPublisher())
    assert (busy.status, busy.found) == ("error", 0) and "HTTP 503" in busy.note

    broken = {"title": "No number"}
    publisher = ListPublisher()
    partial = check(state, FakeApi([[document(1), broken]]), publisher)
    assert partial.found == 1 and "1 without a number or date left out" in partial.note
