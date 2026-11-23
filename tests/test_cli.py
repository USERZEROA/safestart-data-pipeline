"""`ssp pipeline`, `ssp warehouse` and `ssp ml` (what each hands to the flows or to the module it stands for), the
verify summary, and the packages the command line imports before a command needs them."""

import subprocess
import sys

import pytest

from safestart_pipeline import cli
from safestart_pipeline.flows.locks import warehouse_lock


@pytest.fixture
def root(monkeypatch, tmp_path):
    monkeypatch.setenv("SAFESTART_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("SAFESTART_WAREHOUSE_PATH", str(tmp_path / "warehouse" / "safestart.duckdb"))
    return tmp_path


def lock_held(target) -> bool:
    try:
        with warehouse_lock(target):
            return False
    except RuntimeError:
        return True


def test_pipeline_runs_the_batch_flow_once_and_the_model_flow_after_it_on_request(monkeypatch, root):
    calls, exit_codes = [], iter([0, 1])
    monkeypatch.setattr(cli, "setup_logging", lambda path: calls.append(path))
    monkeypatch.setattr("safestart_pipeline.flows.__main__.run_once",
                        lambda names, *, download: calls.append((names, download)) or next(exit_codes))
    assert cli.main(["pipeline"]) == 0
    assert cli.main(["pipeline", "--no-download", "--model"]) == 1  # the exit code of the runs
    log = root / "logs" / "pipeline.log"
    assert calls == [log, (["batch"], True), log, (["batch", "model"], False)]


@pytest.mark.parametrize(("argv", "locked"), [
    (["build"], True),
    (["export", "--out", "exports"], True),
    (["query", "top_cited_sections"], False),
    (["--help"], False),
])
def test_warehouse_commands_reach_the_warehouse_module_and_its_writers_hold_the_lock(monkeypatch, root, argv, locked):
    calls = []
    target = root / "warehouse" / "safestart.duckdb"
    monkeypatch.setattr("safestart_pipeline.warehouse.__main__.main",
                        lambda args: calls.append((args, lock_held(target))) or 0)
    assert cli.main(["warehouse", *argv]) == 0
    assert calls == [(argv, locked)]
    assert not lock_held(target)


def test_a_warehouse_build_while_another_writer_holds_the_lock_fails_at_once(monkeypatch, root):
    monkeypatch.setattr("safestart_pipeline.warehouse.__main__.main", lambda args: pytest.fail("built anyway"))
    with warehouse_lock(root / "warehouse" / "safestart.duckdb"):
        with pytest.raises(RuntimeError, match="another run is building or exporting"):
            cli.main(["warehouse", "build"])


def test_ml_commands_reach_the_ml_module(monkeypatch, root):
    calls = []
    monkeypatch.setattr("safestart_pipeline.ml.__main__.main", lambda args: calls.append(args) or 0)
    assert cli.main(["ml", "train", "--out", "models"]) == 0
    assert cli.main(["ml", "analyze"]) == 0
    assert calls == [["train", "--out", "models"], ["analyze"]]


def test_the_other_commands_still_refuse_arguments_they_do_not_know(capsys, root):
    with pytest.raises(SystemExit) as stopped:
        cli.main(["pipeline", "--fast"])
    assert stopped.value.code == 2 and "unrecognized arguments: --fast" in capsys.readouterr().err


def test_a_verify_summary_keeps_the_counts_and_leaves_out_the_examples(tmp_path):
    path = tmp_path / "verify.json"
    crawl = {"site": "msha", "verdict": "complete", "states": {"fetched": 2}, "waiting": 0,
             "sitemap": {"urls": 2, "states": {"fetched": 2}, "lastmod": {}},
             "skipped": {"robots": {"count": 1, "examples": ["/a"]}}, "failed": {"error": {"count": 0, "examples": []}},
             "objects": {"missing": 0, "missing_examples": []},
             "live_sitemap": {"urls_today": 2, "added": 0, "dropped": 0, "changed_after_fetch": 1, "added_urls": []}}
    assert cli.summary(crawl, path) == {
        "site": "msha", "verdict": "complete", "states": {"fetched": 2}, "waiting": 0,
        "sitemap": {"urls": 2, "states": {"fetched": 2}}, "skipped": {"robots": 1}, "failed": {"error": 0},
        "objects": {"missing": 0},
        "live_sitemap": {"urls_today": 2, "added": 0, "dropped": 0, "changed_after_fetch": 1}, "report": str(path)}
    bulk = {"verdict": "complete", "files": [{}, {}], "problems": [], "live_check": "arlweb crawl running",
            "msha_listing_today": {"files": ["Mines.zip", "Accidents.zip"], "page_sha256": "ab"},
            "ecfr_today": [{"title": 30, "official_sections": 5, "our_sections": 5, "sections": []}]}
    assert cli.summary(bulk, path) == {
        "verdict": "complete", "files": 2, "problems": [], "msha_listing_today": {"files": 2, "page_sha256": "ab"},
        "ecfr_today": [{"title": 30, "official_sections": 5, "our_sections": 5, "missing_sections": None,
                        "extra_sections": None, "our_amended": None, "ecfr_latest_amended_on": None,
                        "source_newer": None}],
        "live_check": "arlweb crawl running", "report": str(path)}


def test_the_command_line_loads_no_analysis_or_orchestration_package_until_a_command_needs_one():
    code = ("import sys, safestart_pipeline.cli\n"
            "print(sorted({'duckdb', 'polars', 'prefect', 'sklearn'} & set(sys.modules)))")
    loaded = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True).stdout
    assert loaded.strip() == "[]"
