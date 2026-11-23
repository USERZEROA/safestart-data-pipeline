from __future__ import annotations

import importlib.util
import os
import shutil
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def load_script(name: str):
    spec = importlib.util.spec_from_file_location(f"scripts_{name}", ROOT / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


start = load_script("start")


@pytest.fixture
def checkout(tmp_path):
    shutil.copy(ROOT / ".env.example", tmp_path / ".env.example")
    return tmp_path


def no_volume(name: str) -> bool:
    return False


@pytest.mark.parametrize("given, host", [
    ("pipeline.example.org", "pipeline.example.org"),
    (" Pipeline.Example.ORG. ", "pipeline.example.org"),
    ("34.120.7.15", "34.120.7.15"),
])
def test_a_public_domain_is_a_hostname_or_a_public_ipv4_address(given, host):
    assert start.public_host(given) == host


@pytest.mark.parametrize("given", [
    "https://pipeline.example.org", "pipeline.example.org:443", "pipeline.example.org/dashboard", "localhost",
    "-bad.example.org", "example.123", "two words.org", "", "10.0.0.5", "127.0.0.1", "203.0.113.7", "2600:1900::1",
])
def test_anything_else_is_refused(given):
    with pytest.raises(ValueError):
        start.public_host(given)


def test_the_first_run_writes_a_private_env_with_a_random_password(checkout):
    env = start.prepare_env(checkout, volume_exists=no_volume)
    text = (checkout / ".env").read_text(encoding="utf-8")
    assert len(env["POSTGRES_PASSWORD"]) == 48 and f"POSTGRES_PASSWORD={env['POSTGRES_PASSWORD']}\n" in text
    assert env["DATA_ROOT"] == "./data" and env["PUBLIC_DOMAIN"] == "" and env["KAFKA_HOST_PORT"] == "19092"
    assert text.startswith("# Settings for docker-compose.yml.")  # the example's explanations stay
    if os.name == "posix":
        assert (checkout / ".env").stat().st_mode & 0o777 == 0o600


def test_later_runs_keep_the_password_and_save_new_options(checkout):
    first = start.prepare_env(checkout, volume_exists=no_volume)
    second = start.prepare_env(checkout, data_root="F:/Safe Start Data", public_domain="34.120.7.15",
                               volume_exists=lambda name: pytest.fail("an existing .env needs no volume check"))
    assert second["POSTGRES_PASSWORD"] == first["POSTGRES_PASSWORD"]
    text = (checkout / ".env").read_text(encoding="utf-8")
    assert "DATA_ROOT='F:/Safe Start Data'\n" in text and "PUBLIC_DOMAIN=34.120.7.15\n" in text
    assert start.read_env(text)["DATA_ROOT"] == "F:/Safe Start Data"
    assert start.prepare_env(checkout) == second  # nothing given, nothing changed


def test_a_new_password_is_refused_while_the_old_database_exists(checkout):
    with pytest.raises(SystemExit, match="down --volumes"):
        start.prepare_env(checkout, volume_exists=lambda name: name == start.POSTGRES_VOLUME)
    assert not (checkout / ".env").exists()


def test_an_env_without_its_password_is_refused(checkout):
    (checkout / ".env").write_text("POSTGRES_PASSWORD=\nDATA_ROOT=./data\n", encoding="utf-8")
    with pytest.raises(SystemExit, match="POSTGRES_PASSWORD"):
        start.prepare_env(checkout)


def test_the_default_data_root_is_created_and_any_other_must_exist(checkout, capsys):
    assert start.data_folder(checkout, "./data") == checkout / "data" and (checkout / "data").is_dir()
    assert "no lake/" in capsys.readouterr().out
    with pytest.raises(SystemExit, match="does not exist"):
        start.data_folder(checkout, str(checkout / "missing"))


def test_on_linux_the_data_root_goes_to_the_orchestrators_user(checkout, monkeypatch, capsys):
    monkeypatch.setattr(start.sys, "platform", "win32")  # Docker Desktop shares a folder whatever its owner
    start.data_folder(checkout, "./data")
    assert "chown" not in capsys.readouterr().out
    monkeypatch.setattr(start.sys, "platform", "linux")
    folder = start.data_folder(checkout, "./data")
    assert folder.stat().st_uid != start.SERVICE_UID  # this test's own folder
    assert f"`sudo chown -R 10001:10001 {folder}`" in capsys.readouterr().out


def test_the_gateway_overlay_is_added_only_for_a_public_domain():
    assert start.compose_command({"PUBLIC_DOMAIN": ""}) == ["docker", "compose", "-f", "docker-compose.yml"]
    assert start.compose_command({"PUBLIC_DOMAIN": "34.120.7.15"})[-2:] == ["-f", "docker-compose.public.yml"]


def test_start_brings_the_stack_up_and_waits_for_the_dashboard(checkout, monkeypatch, capsys):
    commands = []
    monkeypatch.setattr(start.shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(start.subprocess, "run", lambda command, **options: commands.append((command, options)))
    monkeypatch.setattr(start, "healthy", lambda url: url == "http://127.0.0.1:8600/_stcore/health")
    assert start.start(checkout, {"PUBLIC_DOMAIN": "34.120.7.15", "DASHBOARD_HOST_PORT": "8600"}, timeout=5) == 0
    assert commands == [(["docker", "compose", "-f", "docker-compose.yml", "-f", "docker-compose.public.yml", "up",
                          "--detach", "--build", "--remove-orphans"], {"cwd": checkout, "check": True})]
    assert "Dashboard: https://34.120.7.15" in capsys.readouterr().out
    monkeypatch.setattr(start, "healthy", lambda url: False)
    assert start.start(checkout, {}, timeout=-1) == 1
    assert "not healthy" in capsys.readouterr().err


def test_no_start_prepares_env_and_data_root_only(checkout, monkeypatch):
    monkeypatch.setattr(start, "ROOT", checkout)
    monkeypatch.setattr(start, "docker_volume_exists", no_volume)
    monkeypatch.setattr(start, "start", lambda *args: pytest.fail("--no-start must not start the stack"))
    assert start.main(["--no-start", "--public-domain", "Pipeline.Example.org"]) == 0
    assert start.read_env((checkout / ".env").read_text(encoding="utf-8"))["PUBLIC_DOMAIN"] == "pipeline.example.org"
    assert (checkout / "data").is_dir()
    with pytest.raises(SystemExit):
        start.main(["--no-start", "--data-root", str(checkout / "missing")])
    assert start.read_env((checkout / ".env").read_text(encoding="utf-8"))["DATA_ROOT"] == "./data"  # not saved
