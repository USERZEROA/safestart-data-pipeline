"""`ssp pipeline`, `ssp warehouse` and `ssp ml` (what each hands to the flows or to the module it stands for),
`ssp lake`, the verify summary, and the packages the command line imports before a command needs them."""

import json
import logging
import subprocess
import sys

import pytest

from safestart_pipeline import cli
from safestart_pipeline.crawl.locks import SiteLock
from test_lake_msha import DEFINITION, hold, mines_zip


@pytest.fixture
def root(monkeypatch, tmp_path):
    monkeypatch.setenv("SAFESTART_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("SAFESTART_WAREHOUSE_PATH", str(tmp_path / "warehouse" / "safestart.duckdb"))
    return tmp_path


def test_pipeline_runs_the_batch_flow_once_and_the_model_flow_after_it_on_request(monkeypatch, root):
    calls, exit_codes = [], iter([0, 1])
    monkeypatch.setattr(cli, "setup_logging", lambda path: calls.append(path))
    monkeypatch.setattr("safestart_pipeline.flows.__main__.run_once",
                        lambda names, *, download: calls.append((names, download)) or next(exit_codes))
    assert cli.main(["pipeline"]) == 0
    assert cli.main(["pipeline", "--no-download", "--model"]) == 1  # the exit code of the runs
    log = root / "logs" / "pipeline.log"
    assert calls == [log, (["batch"], True), log, (["batch", "model"], False)]


@pytest.mark.parametrize("argv", [["build"], ["export", "--out", "exports"], ["query", "top_cited_sections"],
                                  ["--help"]])
def test_warehouse_commands_reach_the_warehouse_module_unchanged(monkeypatch, root, argv):
    calls = []
    monkeypatch.setattr("safestart_pipeline.warehouse.__main__.main", lambda args: calls.append(args) or 0)
    assert cli.main(["warehouse", *argv]) == 0
    assert calls == [argv]


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


def test_lake_refuses_a_dataset_it_does_not_know_before_building_anything(capsys, root):
    with pytest.raises(SystemExit) as stopped:
        cli.main(["lake", "--only", "mine", "accidents"])
    error = capsys.readouterr().err
    assert stopped.value.code == 2 and "unknown dataset mine;" in error and "mines, address_of_record" in error
    assert not (root / "lake").exists()


def test_lake_prints_every_datasets_result_and_fails_when_one_could_not_be_built(capsys, caplog, root, tmp_path):
    hold(root, {"Mines.zip": mines_zip(tmp_path / "Mines.zip").read_bytes(),
                "Mines_Definition_File.txt": DEFINITION.encode("cp1252"), "Accidents.zip": b"not a zip file",
                "Accidents_Definition_File.txt": DEFINITION.encode("cp1252")})
    caplog.set_level(logging.INFO, logger="safestart_pipeline.lake.msha_ogd")
    assert cli.main(["lake", "--only", "mines", "accidents"]) == 1
    printed = json.loads(capsys.readouterr().out)["msha_ogd"]
    assert printed["mines"]["rows"] == 3 and printed["accidents"]["built"] is False and printed["accidents"]["error"]
    assert {record.getMessage().split(":")[0] for record in caplog.records
            if record.name == "safestart_pipeline.lake.msha_ogd"} == {"mines", "accidents"}


def test_lake_fails_at_once_while_another_writer_holds_the_data_root(root):
    with SiteLock(root / "state" / "pipeline.lock"), pytest.raises(RuntimeError, match="one writer of the data root"):
        cli.main(["lake"])


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
