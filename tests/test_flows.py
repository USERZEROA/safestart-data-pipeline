import pytest
from prefect import flow
from prefect.testing.utilities import prefect_test_harness

from safestart_pipeline.flows import pipeline


@pytest.fixture(scope="module", autouse=True)
def prefect_api():
    """A temporary Prefect API, so flow runs never reach a real workspace."""
    with prefect_test_harness():
        yield


@pytest.fixture
def lake_calls(monkeypatch, tmp_path):
    calls = []

    def fake_build_lake(root, only=None):
        calls.append((root, only))
        return {"mines": {"rows": 3, "built": True, "versions_built": 1}, "accidents": {"rows": 2, "versions_built": 0}}

    monkeypatch.setattr("safestart_pipeline.lake.msha_ogd.build_lake", fake_build_lake)
    monkeypatch.setenv("SAFESTART_DATA_ROOT", str(tmp_path))
    return calls


def test_batch_without_downloads_rebuilds_the_lake_from_the_data_root(lake_calls, tmp_path):
    summary = pipeline.batch(download=False)
    assert lake_calls == [(tmp_path, None)]
    assert summary == {"lake": {"mines": {"rows": 3, "built": True, "versions_built": 1},
                                "accidents": {"rows": 2, "versions_built": 0}}}


def test_a_failed_download_still_rebuilds_the_lake_then_fails_the_run(lake_calls, monkeypatch):
    @flow(name="acquire-bulk-files")
    def failing_acquire():
        raise RuntimeError("2 file(s) failed after retries")

    monkeypatch.setattr(pipeline, "acquire_bulk", failing_acquire)
    with pytest.raises(RuntimeError, match="acquire-bulk-files"):
        pipeline.batch(download=True)
    assert len(lake_calls) == 1


def test_downloads_come_before_the_lake(lake_calls, monkeypatch):
    order = []

    @flow(name="acquire-bulk-files")
    def acquire():
        order.append("acquire")
        return {"msha-ogd": 40}

    monkeypatch.setattr(pipeline, "acquire_bulk", acquire)
    original = pipeline.build_lake_task.fn

    def lake_after(only=None):
        order.append("lake")
        return original(only)

    monkeypatch.setattr(pipeline.build_lake_task, "fn", lake_after)
    summary = pipeline.batch(download=True)
    assert order == ["acquire", "lake"]
    assert summary["acquire"] == {"msha-ogd": 40}
