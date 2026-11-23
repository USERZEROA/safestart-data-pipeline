"""The sample data root (`python -m safestart_pipeline.sample`) is one the whole pipeline runs on, with the outcomes CI
checks in the running stack: the lake is built from the sample's bulk store and found current again by the batch, the
batch flow without downloads builds the warehouse and the exports, the replay and the version watcher send the events
the stack job expects in PostgreSQL, and a folder that is not empty is never written."""

from __future__ import annotations

import json
import re
import shutil
import sqlite3
from collections import Counter
from datetime import date
from pathlib import Path

import pytest

from safestart_pipeline import sample
from safestart_pipeline.lake.msha_ogd import build_lake
from safestart_pipeline.stream.analytics import item
from safestart_pipeline.stream.records import KINDS
from safestart_pipeline.stream.replay import build_event, current_parquet, read_rows
from safestart_pipeline.warehouse.build import build
from safestart_pipeline.warehouse.exports import export
from safestart_pipeline.watch.publish import ListPublisher
from safestart_pipeline.watch.state import WatchState
from safestart_pipeline.watch.versions import check_versions

yaml = pytest.importorskip("yaml")

ROOT = Path(__file__).resolve().parents[1]
REPLAYED = ("inspections", "violations", "orders_107a", "accidents", "mines")


@pytest.fixture(scope="module")
def written(tmp_path_factory) -> tuple[Path, dict]:
    root = tmp_path_factory.mktemp("sample") / "data"
    return root, sample.write_sample(root)


def manifest(root: Path, dataset: str) -> dict:
    return json.loads((root / "lake" / "msha_ogd" / dataset / "manifest.json").read_text(encoding="utf-8"))


def stack_job() -> dict:
    workflow = yaml.safe_load((ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8"))
    return workflow["jobs"]["stack"]


def expected(name: str) -> list[str]:
    """The lines of one of the stack job's EXPECTED_* settings, which it compares with the running stack."""
    values = [step["env"][name] for step in stack_job()["steps"] if name in (step.get("env") or {})]
    assert len(values) == 1, name
    return values[0].splitlines()


def test_the_lake_holds_two_weeks_built_from_the_bulk_store_and_the_batch_finds_it_current(written):
    root, summary = written
    for dataset in REPLAYED:
        held = manifest(root, dataset)
        assert len(held["versions"]) == 2 and held["current"] == held["versions"][-1]["parquet"], dataset
    assert len(manifest(root, "mines_prod_quarterly")["versions"]) == 1
    assert summary["news_from"] == sample.NEWS_FROM.isoformat()
    with sqlite3.connect(root / "state" / "bulk.sqlite") as db:
        current = dict(db.execute("select name, sha256 from files where name like '%.zip'").fetchall())
    assert {name: result["parquet"] for name, result in summary["lake"].items()} == {
        dataset: current[zip_name][:16] + ".parquet" for dataset, (zip_name, _) in sample.DATASETS.items()
        if dataset in sample.RELEASES}
    # What the batch flow without downloads finds: every version already built.
    again = build_lake(root)
    assert all(again[name]["built"] is False and again[name]["versions_built"] == 0 for name in sample.RELEASES)
    assert not list(root.rglob("*-wal")) and not list(root.rglob("*-shm"))  # read on read-only mounts as immutable


def test_the_same_sample_is_written_every_time(written, tmp_path):
    assert sample.write_sample(tmp_path / "again")["lake"] == written[1]["lake"]


def test_the_replay_and_the_version_watcher_send_the_events_the_stack_job_expects(written, tmp_path):
    root, _ = written
    lake = root / "lake" / "msha_ogd"
    versions = {kind.name: current_parquet(lake, kind.dataset).stem for kind in KINDS}
    until = re.search(r"safestart_pipeline\.stream\.replay .*--until (\S+)",
                      " ".join(step.get("run", "") for step in stack_job()["steps"]))
    assert until and date.fromisoformat(until[1]) == sample.NEWS_FROM
    replayed = [build_event(row, versions) for row in read_rows(lake, date(2024, 1, 1), sample.NEWS_FROM)]
    state, publisher = WatchState(tmp_path / "watch.sqlite"), ListPublisher()
    try:
        check = check_versions(root, state, publisher)
    finally:
        state.close()
    assert [f"{check.status} {check.found}"] == expected("EXPECTED_CHECK")
    events = replayed + publisher.events
    assert len({event.event_id for event in events}) == len(events)  # the new week's records are not replayed
    by_mode = Counter((event.mode, event.kind) for event in events)
    assert sorted(f"{mode} {kind} {count}" for (mode, kind), count in by_mode.items()) == expected("EXPECTED_EVENTS")
    counts = Counter()
    for event in events:
        counts[item(event).kind, "events"] += 1
        counts[item(event).kind, "serious"] += item(event).serious
    kinds = sorted({kind for kind, _ in counts})
    assert [f"{kind} {counts[kind, 'events']} {counts[kind, 'serious']}" for kind in kinds] == expected(
        "EXPECTED_COUNTS")


def test_the_warehouse_and_the_exports_build_from_the_sample(written, tmp_path):
    root, _ = written
    summary = build(root, tmp_path / "safestart.duckdb")
    tables = dict(line.split() for line in expected("EXPECTED_TABLES"))
    assert {table: str(summary["rows"][table]) for table in tables} == tables
    # The sample cites only sections its title 30 holds; the MSHA and OSHA catalogs are left out on purpose.
    assert {name for name, check in summary["checks"].items() if check["rows"]} == {"crawl_catalog_missing"}
    exported = export(tmp_path / "safestart.duckdb", tmp_path / "exports")
    assert sorted(f"{name} {file['rows']}" for name, file in exported["files"].items()) == expected("EXPECTED_EXPORTS")


def test_the_batch_flow_without_downloads_builds_the_warehouse_and_the_exports(written, tmp_path, monkeypatch):
    from prefect.testing.utilities import prefect_test_harness

    from safestart_pipeline.flows.pipeline import batch

    root = tmp_path / "data"
    shutil.copytree(written[0], root)
    monkeypatch.setenv("SAFESTART_DATA_ROOT", str(root))
    monkeypatch.delenv("SAFESTART_WAREHOUSE_PATH", raising=False)
    with prefect_test_harness():
        summary = batch(download=False)
    assert all(result["built"] is False for name, result in summary["lake"].items() if name in sample.RELEASES)
    assert summary["warehouse"]["warehouse"] == str(root / "warehouse" / "safestart.duckdb")
    assert sorted(summary["exports"]["files"]) == ["cited_sections.parquet", "mine_profiles.parquet"]
    assert (root / "exports" / "safestart" / "manifest.json").is_file()


def test_main_writes_into_a_new_folder_and_never_into_one_that_holds_anything(tmp_path, capsys):
    assert sample.main([str(tmp_path / "new")]) == 0
    assert json.loads(capsys.readouterr().out)["news_from"] == sample.NEWS_FROM.isoformat()
    used = tmp_path / "used"
    used.mkdir()
    (used / "keep.txt").write_text("not a sample", encoding="utf-8")
    with pytest.raises(SystemExit) as refused:
        sample.main([str(used)])
    assert refused.value.code == 2 and [path.name for path in used.iterdir()] == ["keep.txt"]
