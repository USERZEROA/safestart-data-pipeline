"""The Google Cloud scripts, run against a stand-in gcloud that records its arguments: the server gets the machine,
disk, address, identity and firewall the deployment promises, the upload copies the parts of the data root the server
needs without deleting, and neither side copies a database's journal or work in progress."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
GCP = ROOT / "deploy" / "gcp"
EXCLUDE = "--exclude=.*objects.tmp.*,.*[.]partial$,.*[.]sqlite-[a-z]+$,.*[.]duckdb[.-].*"
SERVER_FOLDERS = ("raw/bulk", "state", "lake", "warehouse", "exports")
STUB = """\
import json, os, sys

args = [arg for arg in sys.argv[1:] if arg != "--quiet" and not arg.startswith("--project=")]
with open(os.environ["GCLOUD_LOG"], "a", encoding="utf-8") as log:
    log.write(json.dumps(args) + "\\n")
if args[:3] == ["config", "get-value", "project"]:
    print("stub-project")
elif "addresses" in args and "--format=value(address)" in args:
    print("203.0.113.7")
elif "describe" in args and not set(args) & set(os.environ.get("GCLOUD_EXISTING", "buckets").split(",")):
    sys.exit(1)
"""


def find_bash() -> str:
    """Git Bash on Windows (a bare `bash` there may be WSL's), otherwise the system's bash."""
    if sys.platform == "win32":
        git_bash = Path(os.environ.get("ProgramFiles", r"C:\Program Files")) / "Git" / "bin" / "bash.exe"
        if not git_bash.is_file():
            pytest.skip("Git Bash is needed to run the shell scripts on Windows")
        return str(git_bash)
    return shutil.which("bash") or pytest.skip("bash is not installed")


@pytest.fixture
def gcloud(tmp_path):
    """A stand-in gcloud first on PATH. Yields the environment to run a script in and a reader of the recorded calls."""
    folder = tmp_path / "bin"
    folder.mkdir()
    stub = folder / "gcloud_stub.py"
    stub.write_text(STUB, encoding="utf-8")
    (folder / "gcloud").write_text(f'#!/bin/sh\nexec "{Path(sys.executable).as_posix()}" "{stub.as_posix()}" "$@"\n',
                                   encoding="utf-8", newline="\n")
    (folder / "gcloud").chmod(0o755)
    (folder / "gcloud.cmd").write_text(f'@"{sys.executable}" "{stub}" %*\r\n', encoding="utf-8")
    log = tmp_path / "gcloud.log"
    env = {**os.environ, "PATH": f"{folder}{os.pathsep}{os.environ['PATH']}", "GCLOUD_LOG": str(log),
           "MSYS_NO_PATHCONV": "1"}

    def calls() -> list[list[str]]:
        return [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()] if log.exists() else []

    yield env, calls


def run(command: list[str], env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(command, env=env, capture_output=True, text=True, timeout=120, check=False)


def run_script(name: str, *args: str, env: dict[str, str]) -> subprocess.CompletedProcess:
    return run([find_bash(), (GCP / name).as_posix(), *args], env)


def call(calls: list[list[str]], *words: str) -> list[str]:
    found = [args for args in calls if args[:len(words)] == list(words)]
    assert len(found) == 1, (words, found)
    return found[0]


def test_create_vm_makes_an_e2_medium_that_only_the_web_can_reach(gcloud):
    env, calls = gcloud
    result = run_script("create-vm.sh", env={**env, "BUCKET": "gs://safestart-data/"})
    assert result.returncode == 0, result.stdout + result.stderr
    made = calls()
    assert call(made, "compute", "networks", "create", "safestart-pipeline-net")[-1] == "--subnet-mode=custom"
    firewall = [args for args in made if args[:3] == ["compute", "firewall-rules", "create"]]
    assert len(firewall) == 1
    assert {"--rules=tcp:80,tcp:443", "--source-ranges=0.0.0.0/0", "--direction=INGRESS"} <= set(firewall[0])
    assert call(made, "compute", "addresses", "create", "safestart-pipeline-ip")[-1] == "--region=us-central1"
    account = "safestart-pipeline-vm@stub-project.iam.gserviceaccount.com"
    assert call(made, "storage", "buckets", "add-iam-policy-binding", "gs://safestart-data")[4:] == [
        f"--member=serviceAccount:{account}", "--role=roles/storage.objectViewer"]
    server = call(made, "compute", "instances", "create", "safestart-pipeline")
    assert {"--machine-type=e2-medium", "--boot-disk-size=50GB", "--boot-disk-type=pd-balanced",
            "--address=203.0.113.7", "--tags=safestart-pipeline-web", f"--service-account={account}",
            "--scopes=storage-ro", "--zone=us-central1-a", "--image-family=ubuntu-2404-lts-amd64"} <= set(server)
    metadata = next(arg for arg in server if arg.startswith("--metadata="))
    assert "safestart-bucket=safestart-data" in metadata and "enable-oslogin=TRUE" in metadata
    assert any(arg.endswith("deploy/gcp/startup.sh") for arg in server if arg.startswith("--metadata-from-file="))
    assert "https://203.0.113.7" in result.stdout


def test_create_vm_without_a_bucket_grants_its_account_nothing(gcloud):
    env, calls = gcloud
    result = run_script("create-vm.sh", env=env)
    assert result.returncode == 0, result.stdout + result.stderr
    server = call(calls(), "compute", "instances", "create", "safestart-pipeline")
    assert "--service-account=safestart-pipeline-vm@stub-project.iam.gserviceaccount.com" in server
    assert not [args for args in calls() if "add-iam-policy-binding" in args]
    assert not any("safestart-bucket" in arg for arg in server)


def test_a_rerun_updates_the_existing_server_instead_of_creating_one(gcloud):
    env, calls = gcloud
    env = {**env, "GCLOUD_EXISTING": "networks,subnets,firewall-rules,addresses,service-accounts,instances"}
    result = run_script("create-vm.sh", env=env)
    assert result.returncode == 0, result.stdout + result.stderr
    assert not [args for args in calls() if "create" in args]
    assert call(calls(), "compute", "instances", "add-metadata", "safestart-pipeline")


@pytest.fixture
def data_root(tmp_path):
    """A data root with the bulk downloads, the crawls' archive, the state and the lake, but no warehouse or exports."""
    root = tmp_path / "SafeStartData"
    for folder in ("raw/bulk/objects", "raw/osha/objects", "state", "lake/msha_ogd"):
        (root / folder).mkdir(parents=True)
    return root


def test_upload_copies_what_the_server_needs_and_never_deletes(gcloud, data_root):
    env, calls = gcloud
    result = run_script("upload-to-gcs.sh", "gs://safestart-data/", "--data-root", data_root.as_posix(), "--dry-run",
                        env=env)
    assert result.returncode == 0, result.stdout + result.stderr
    assert calls() == [["storage", "rsync", "--recursive", "--dry-run", EXCLUDE, f"{data_root.as_posix()}/{folder}",
                        f"gs://safestart-data/{folder}"] for folder in SERVER_FOLDERS[:3]]  # not the crawls' raw/osha
    assert "Skipped warehouse/" in result.stdout and "Skipped exports/" in result.stdout


def test_upload_copies_the_whole_raw_archive_only_on_request(gcloud, data_root):
    env, calls = gcloud
    result = run_script("upload-to-gcs.sh", "safestart-data", "--data-root", data_root.as_posix(), "--only", "raw",
                        env=env)
    assert result.returncode == 0, result.stdout + result.stderr
    assert calls() == [["storage", "rsync", "--recursive", EXCLUDE, f"{data_root.as_posix()}/raw",
                        "gs://safestart-data/raw"]]
    refused = run_script("upload-to-gcs.sh", "safestart-data", "--data-root", data_root.as_posix(), "--only",
                         "raw/osha", env=env)
    assert refused.returncode == 2 and len(calls()) == 1


def test_upload_refuses_a_bad_bucket_name(gcloud, data_root):
    env, calls = gcloud
    result = run_script("upload-to-gcs.sh", "Not A Bucket", env=env)
    assert result.returncode == 2 and calls() == []


def test_the_windows_upload_passes_the_same_arguments_through_gcloud(gcloud, data_root):
    shell = shutil.which("pwsh") or shutil.which("powershell")
    if not shell:
        pytest.skip("PowerShell is not installed")
    env, calls = gcloud
    command = [shell, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File",
               str(GCP / "upload-to-gcs.ps1"), "-Bucket", "gs://safestart-data", "-DataRoot", str(data_root)]
    result = run([*command, "-DryRun"], env)
    assert result.returncode == 0, result.stdout + result.stderr
    made = calls()
    assert [call[:4] for call in made] == [["storage", "rsync", "--recursive", EXCLUDE]] * 3
    assert [(Path(call[4]), call[5], call[6]) for call in made] == [
        (data_root / folder, f"gs://safestart-data/{folder}", "--dry-run") for folder in SERVER_FOLDERS[:3]]
    assert "Skipped warehouse/" in result.stdout and "Skipped exports/" in result.stdout
    result = run([*command, "-Only", "lake,warehouse"], env)
    assert result.returncode == 0, result.stdout + result.stderr
    assert calls()[3:] == [["storage", "rsync", "--recursive", EXCLUDE, str(data_root / "lake"),
                            "gs://safestart-data/lake"]]


@pytest.mark.parametrize(("path", "copied"), [
    ("state/bulk.sqlite", True), ("state/bulk.sqlite-wal", False), ("state/crawl-osha.sqlite-shm", False),
    ("state/bulk.sqlite-journal", False), ("warehouse/safestart.duckdb", True),
    ("warehouse/safestart.duckdb.wal", False), ("warehouse/safestart.duckdb.building", False),
    ("warehouse/.safestart.duckdb-x1y2/mine-0.parquet", False),
    ("lake/msha_ogd/violations/7943d78c6aba1c23.parquet", True),
    ("lake/msha_ogd/mines/.c4ba9edb.parquet.partial", False),
    ("raw/bulk/objects/tmp/download", False), ("raw/bulk/objects/0a/1b/0a1b2c", True),
])
def test_files_in_use_are_never_copied(path, copied):
    """gcloud matches each pattern (a Python regular expression) against a file's path below the folder it copies,
    written with backslashes on Windows."""
    patterns = EXCLUDE.removeprefix("--exclude=").split(",")
    below = path.partition("/")[2]
    for name in (path, below, below.replace("/", "\\")):
        assert (not any(re.match(pattern, name) for pattern in patterns)) == copied, name


def test_the_server_copies_what_it_lacks_of_the_folders_it_needs():
    script = (GCP / "startup.sh").read_text(encoding="utf-8")
    assert f"for part in {' '.join(SERVER_FOLDERS)}; do" in script
    rsync = re.search(r"gcloud storage rsync (.+?)\\\n\s+--exclude='([^']+)'", script)
    assert rsync and rsync[1].split() == ["--recursive", "--no-clobber"]  # never replaces nor deletes on the server
    assert f"--exclude={rsync[2]}" == EXCLUDE  # the same files left out both ways
    assert "chown -R 10001:10001 \"$data\"" in script  # the image's user, as whom the orchestrator writes


def test_the_startup_script_parses():
    result = run([find_bash(), "-n", (GCP / "startup.sh").as_posix()])
    assert result.returncode == 0, result.stderr
