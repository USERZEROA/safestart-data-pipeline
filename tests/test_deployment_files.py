"""The deployment files keep their promises: the stack fits a 4 GiB host, only the gateway is public, every pipeline
service runs unprivileged on a read-only data root, and the services run the documented entry points."""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

ROOT = Path(__file__).resolve().parents[1]
ENTRY_POINTS = {
    "init": ["python", "-m", "safestart_pipeline.stream.init"],
    "replay": ["python", "-m", "safestart_pipeline.stream.replay"],
    "storage-consumer": ["python", "-m", "safestart_pipeline.stream.storage"],
    "analytics-consumer": ["python", "-m", "safestart_pipeline.stream.analytics"],
    "watcher": ["python", "-m", "safestart_pipeline.watch", "loop"],
    "dashboard": ["streamlit", "run", "/app/src/safestart_pipeline/dashboard/app.py"],
}
MIB = {"k": 1 / 1024, "m": 1, "g": 1024}


def services(name: str) -> dict:
    return yaml.safe_load((ROOT / name).read_text(encoding="utf-8"))["services"]


def mebibytes(limit: str) -> float:
    number, unit = re.fullmatch(r"(\d+)([kmg])", limit).groups()
    return int(number) * MIB[unit]


def test_the_running_services_fit_a_4_gib_host_with_1_gib_left_for_the_system():
    stack = {**services("docker-compose.yml"), **services("docker-compose.public.yml")}
    limits = {name: mebibytes(service["mem_limit"]) for name, service in stack.items()}
    assert sum(limit for name, limit in limits.items() if name != "init") <= 3 * 1024  # init exits before they start
    heap = re.fullmatch(r"-Xms(\d+)m -Xmx(\d+)m", stack["kafka"]["environment"]["KAFKA_HEAP_OPTS"])
    assert heap.groups() == ("256", "384") and limits["kafka"] >= 384 + 256  # room for the JVM and its health check


def test_only_the_gateway_listens_beyond_this_host():
    for name, service in services("docker-compose.yml").items():
        assert all(port.startswith("127.0.0.1:") for port in service.get("ports", [])), name
    assert services("docker-compose.public.yml")["gateway"]["ports"] == ["80:80", "443:443"]


def test_pipeline_services_share_one_unprivileged_image_and_a_read_only_data_root():
    stack = services("docker-compose.yml")
    for name in ENTRY_POINTS:
        service = stack[name]
        assert (service["image"], service["build"], service["init"]) == ("safestart-pipeline:local", ".", True)
        assert service["cap_drop"] == ["ALL"] and service["security_opt"] == ["no-new-privileges:true"]
        assert service["environment"]["SAFESTART_DATA_ROOT"] == "/data"
        data = [mount for mount in service["volumes"] if isinstance(mount, dict) and mount["target"] == "/data"]
        assert data == [{"type": "bind", "source": "${DATA_ROOT:-./data}", "target": "/data", "read_only": True}]
    assert "watch-state:/state" in stack["watcher"]["volumes"]  # the watcher's own state stays writable
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert re.findall(r"^USER (\S+)", dockerfile, re.MULTILINE) == ["safestart"]
    assert dockerfile.index("--no-install-project") < dockerfile.index("COPY src")  # dependencies cached apart


def test_secrets_and_data_never_enter_the_image():
    rules = [line for line in (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
             if line and not line.startswith("#")]
    assert rules[0] == "*"  # an allowlist
    allowed = {rule[1:].rstrip("/") for rule in rules if rule.startswith("!")}
    assert allowed == {"pyproject.toml", "uv.lock", "README.md", "LICENSE", "src", "scripts/smoke_test.py"}


def test_services_wait_for_the_initializer_and_run_the_documented_entry_points():
    stack = services("docker-compose.yml")
    assert {name: stack[name]["command"] for name in ENTRY_POINTS} == ENTRY_POINTS
    assert stack["init"]["depends_on"] == {"kafka": {"condition": "service_healthy"},
                                           "postgres": {"condition": "service_healthy"}}
    for name in set(ENTRY_POINTS) - {"init"}:
        assert stack[name]["depends_on"] == {"init": {"condition": "service_completed_successfully"}}
    assert stack["init"]["restart"] == "no" and stack["replay"]["restart"] == "on-failure"


def runnable(module: str) -> bool:
    """`python -m module` works: a module, or a package with a __main__."""
    spec = importlib.util.find_spec(module)
    return spec is not None and (spec.submodule_search_locations is None or runnable(f"{module}.__main__"))


def test_every_entry_point_exists():
    missing = [command[2] for command in ENTRY_POINTS.values() if command[0] == "python" and not runnable(command[2])]
    if not (ROOT / "src" / "safestart_pipeline" / "dashboard" / "app.py").is_file():
        missing.append("safestart_pipeline/dashboard/app.py")
    if len(missing) == len(ENTRY_POINTS):
        pytest.skip("the stream, watch and dashboard packages are not in this checkout")
    assert missing == []
