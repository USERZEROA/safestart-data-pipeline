"""The deployment files keep their promises: the stack fits an e2-medium and its swap file, only the gateway is public,
every pipeline service runs unprivileged and only the orchestrator writes the data root, the settings Compose gives a
service are the ones its code reads, and the services run the documented entry points."""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path, PurePosixPath

import pytest

from safestart_pipeline.watch import __main__ as watch
from safestart_pipeline.watch.state import default_path

yaml = pytest.importorskip("yaml")

ROOT = Path(__file__).resolve().parents[1]
ENTRY_POINTS = {
    "init": ["python", "-m", "safestart_pipeline.stream.init"],
    "replay": ["python", "-m", "safestart_pipeline.stream.replay"],
    "storage-consumer": ["python", "-m", "safestart_pipeline.stream.storage"],
    "analytics-consumer": ["python", "-m", "safestart_pipeline.stream.analytics"],
    "watcher": ["python", "-m", "safestart_pipeline.watch", "loop"],
    "dashboard": ["streamlit", "run", "/app/src/safestart_pipeline/dashboard/app.py"],
    "orchestrator": ["python", "-m", "safestart_pipeline.flows", "serve"],
}
STREAM_SERVICES = set(ENTRY_POINTS) - {"init", "orchestrator"}
MIB = {"k": 1 / 1024, "m": 1, "g": 1024}
# MiB. An e2-medium's memory, the swap file deploy/gcp/startup.sh adds, and what the system and Docker keep.
RAM, SWAP, SYSTEM = 4096, 2048, 1024
ONE_OFF = {"init", "replay"}  # they exit once their work is done
# Peaks measured in the image on the real lake (peak resident memory, MiB): training the model with two threads, 1,350,
# with Prefect and its local server beside it, 400; the watcher comparing two versions of the 3.1 million violations
# with DuckDB held to 256 MB, 343.
ORCHESTRATOR_PEAK = 1350 + 400
WATCHER_PEAK = 343


def services(name: str) -> dict:
    return yaml.safe_load((ROOT / name).read_text(encoding="utf-8"))["services"]


def mebibytes(limit: str) -> float:
    number, unit = re.fullmatch(r"(\d+)([kmg])", limit).groups()
    return int(number) * MIB[unit]


def test_the_stack_fits_an_e2_medium_with_its_swap_file():
    """Limits are ceilings. The services that keep running fit in memory beside the system, so they never swap. The
    orchestrator's runs (the daily batch, the weekly model) are the one large use: with the orchestrator and the
    replay, which runs once after each start, at their limits too, the host may swap but cannot run out of memory.
    The initializer exits before the others start."""
    stack = {**services("docker-compose.yml"), **services("docker-compose.public.yml")}
    limits = {name: mebibytes(service["mem_limit"]) for name, service in stack.items()}
    keep_running = sum(limit for name, limit in limits.items() if name not in ONE_OFF | {"orchestrator"})
    assert keep_running <= RAM - SYSTEM
    assert keep_running + limits["orchestrator"] + limits["replay"] <= RAM + SWAP - SYSTEM
    assert "fallocate -l 2G /swapfile" in (ROOT / "deploy" / "gcp" / "startup.sh").read_text(encoding="utf-8")
    assert limits["orchestrator"] >= ORCHESTRATOR_PEAK and limits["watcher"] >= WATCHER_PEAK
    heap = re.fullmatch(r"-Xms(\d+)m -Xmx(\d+)m", stack["kafka"]["environment"]["KAFKA_HEAP_OPTS"])
    assert heap.groups() == ("256", "384") and limits["kafka"] >= 384 + 256  # room for the JVM and its health check


def test_the_dashboards_duckdb_keeps_to_half_of_its_container():
    data = pytest.importorskip("safestart_pipeline.dashboard.data")
    number, unit = re.fullmatch(r"(\d+)(MiB|GiB)", data.WAREHOUSE_SETTINGS["memory_limit"]).groups()
    duckdb = int(number) * (1024 if unit == "GiB" else 1)
    assert duckdb <= mebibytes(services("docker-compose.yml")["dashboard"]["mem_limit"]) / 2


def test_only_the_gateway_listens_beyond_this_host():
    for name, service in services("docker-compose.yml").items():
        assert all(port.startswith("127.0.0.1:") for port in service.get("ports", [])), name
    assert services("docker-compose.public.yml")["gateway"]["ports"] == ["80:80", "443:443"]


def test_pipeline_services_share_one_unprivileged_image_and_only_the_orchestrator_writes_the_data_root():
    stack = services("docker-compose.yml")
    for name in ENTRY_POINTS:
        service = stack[name]
        assert (service["image"], service["build"], service["init"]) == ("safestart-pipeline:local", ".", True)
        assert service["cap_drop"] == ["ALL"] and service["security_opt"] == ["no-new-privileges:true"]
        assert service["environment"]["SAFESTART_DATA_ROOT"] == "/data"
        data = [mount for mount in service["volumes"] if isinstance(mount, dict) and mount["target"] == "/data"]
        assert data == [{"type": "bind", "source": "${DATA_ROOT:-./data}", "target": "/data",
                         "read_only": name != "orchestrator"}], name
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert re.findall(r"^USER (\S+)", dockerfile, re.MULTILINE) == ["safestart"]
    assert "useradd --uid 10001" in dockerfile  # the owner startup.sh gives the server's data root
    assert dockerfile.index("--no-install-project") < dockerfile.index("COPY src")  # dependencies cached apart


def test_the_watcher_keeps_its_state_and_its_log_off_the_read_only_data_root(monkeypatch, tmp_path):
    watcher = services("docker-compose.yml")["watcher"]
    assert "watch-state:/state" in watcher["volumes"]
    env = watcher["environment"]
    assert PurePosixPath(env["SAFESTART_WATCH_STATE"]).parent == PurePosixPath("/state")
    monkeypatch.setenv("SAFESTART_DATA_ROOT", str(tmp_path))
    for name in ("SAFESTART_WATCH_STATE", "SAFESTART_WATCH_LOG"):  # the names the watcher's code reads
        monkeypatch.setenv(name, env[name])
    assert default_path() == Path(env["SAFESTART_WATCH_STATE"])
    assert watch.log_file(tmp_path) is None  # standard error only, which Docker keeps and rotates


def test_the_dashboard_shows_a_viewers_toolbar():
    assert services("docker-compose.yml")["dashboard"]["environment"]["STREAMLIT_CLIENT_TOOLBAR_MODE"] == "viewer"


def test_the_orchestrator_takes_prefect_from_env_and_needs_no_stream_service():
    orchestrator = services("docker-compose.yml")["orchestrator"]
    assert orchestrator["profiles"] == ["orchestrator"]  # started only when asked for: it writes the data root
    assert "COMPOSE_PROFILES=\n" in (ROOT / ".env.example").read_text(encoding="utf-8")  # off by default
    env = orchestrator["environment"]
    assert env["PREFECT_API_URL"] is None and env["PREFECT_API_KEY"] is None  # from .env when set there, else unset
    assert (env["POLARS_MAX_THREADS"], env["OMP_NUM_THREADS"]) == ("2", "2")
    assert "depends_on" not in orchestrator and "SAFESTART_POSTGRES_URL" not in env  # nor the database password
    example = (ROOT / ".env.example").read_text(encoding="utf-8")
    assert "# PREFECT_API_URL=\n" in example and "# PREFECT_API_KEY=\n" in example  # optional, so left out


def test_secrets_and_data_never_enter_the_image():
    rules = [line for line in (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
             if line and not line.startswith("#")]
    assert rules[0] == "*"  # an allowlist
    allowed = {rule[1:].rstrip("/") for rule in rules if rule.startswith("!")}
    assert allowed == {"pyproject.toml", "uv.lock", "README.md", "LICENSE", "src", "scripts/smoke_test.py"}


def test_services_wait_for_what_they_need_and_run_the_documented_entry_points():
    stack = services("docker-compose.yml")
    assert {name: stack[name]["command"] for name in ENTRY_POINTS} == ENTRY_POINTS
    assert stack["init"]["depends_on"] == {"kafka": {"condition": "service_healthy"},
                                           "postgres": {"condition": "service_healthy"}}
    for name in STREAM_SERVICES:
        assert stack[name]["depends_on"] == {"init": {"condition": "service_completed_successfully"}}
    assert stack["init"]["restart"] == "no" and stack["replay"]["restart"] == "on-failure"
    assert stack["orchestrator"]["restart"] == "unless-stopped"


def runnable(module: str) -> bool:
    """`python -m module` works: a module, or a package with a __main__."""
    spec = importlib.util.find_spec(module)
    return spec is not None and (spec.submodule_search_locations is None or runnable(f"{module}.__main__"))


def test_every_entry_point_exists():
    missing = [command[2] for command in ENTRY_POINTS.values() if command[0] == "python" and not runnable(command[2])]
    if not (ROOT / "src" / "safestart_pipeline" / "dashboard" / "app.py").is_file():
        missing.append("safestart_pipeline/dashboard/app.py")
    assert missing == []
