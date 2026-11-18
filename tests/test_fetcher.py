import httpx

from safestart_pipeline.crawl.fetcher import ObjectStore, fetch, parse_retry_after

URL = "https://www.example.test/file"


def client_for(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False)


def test_identical_bodies_are_stored_once(tmp_path):
    objects = ObjectStore(tmp_path / "objects")
    with client_for(lambda request: httpx.Response(200, content=b"same bytes")) as client:
        first = fetch(client, URL, objects)
        second = fetch(client, URL + "2", objects)
    assert first.sha256 == second.sha256 and first.path == second.path
    assert first.path.read_bytes() == b"same bytes" and first.size == len(b"same bytes")
    assert not any(objects.tmp.iterdir())


def test_declared_size_over_the_cap_is_not_downloaded(tmp_path):
    objects = ObjectStore(tmp_path / "objects")
    with client_for(lambda request: httpx.Response(200, content=b"x" * 500)) as client:
        result = fetch(client, URL, objects, max_bytes=100)
    assert result.too_large and result.size == 500 and result.path is None


def test_streamed_body_over_the_cap_is_discarded(tmp_path):
    objects = ObjectStore(tmp_path / "objects")
    with client_for(lambda request: httpx.Response(200, content=iter([b"x" * 60, b"y" * 60]))) as client:
        result = fetch(client, URL, objects, max_bytes=100)
    assert result.too_large and result.path is None
    assert not any(objects.tmp.iterdir())


def test_errors_keep_a_bounded_sample_and_network_failures_have_no_status(tmp_path):
    objects = ObjectStore(tmp_path / "objects")
    with client_for(lambda request: httpx.Response(503, content=b"z" * 200_000, headers={"retry-after": "120"})) as client:
        busy = fetch(client, URL, objects)
    assert busy.status == 503 and busy.retry_after == 120.0 and len(busy.sample) == 64 * 1024

    def refuse(request):
        raise httpx.ConnectError("refused", request=request)

    with client_for(refuse) as client:
        down = fetch(client, URL, objects)
    assert down.status is None and down.error.startswith("ConnectError")


def test_conditional_headers_are_sent(tmp_path):
    seen = {}

    def handler(request):
        seen.update(request.headers)
        return httpx.Response(304)

    with client_for(handler) as client:
        result = fetch(client, URL, ObjectStore(tmp_path / "objects"), etag='"v1"', last_modified="Mon, 16 Nov 2026 10:00:00 GMT")
    assert result.status == 304
    assert seen["if-none-match"] == '"v1"' and seen["if-modified-since"] == "Mon, 16 Nov 2026 10:00:00 GMT"


def test_retry_after_forms():
    assert parse_retry_after("30") == 30.0
    assert parse_retry_after(None) is None and parse_retry_after("soon") is None
    midnight = 1_794_960_000.0  # 2026-11-18T00:00:00Z
    assert parse_retry_after("Wed, 18 Nov 2026 00:01:00 GMT", now=lambda: midnight) == 60.0
    assert parse_retry_after("Wed, 18 Nov 2026 00:01:00 GMT", now=lambda: midnight + 3600) == 0.0
