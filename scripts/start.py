#!/usr/bin/env python3
"""Start the pipeline's Docker Compose stack with one command, on Windows, macOS or Linux.

The first run writes `.env` from `.env.example` with a random PostgreSQL password. PostgreSQL keeps the password its
database was created with, so later runs never replace it, and a first run refuses to invent a new one while the
database of an earlier `.env` still exists. `--data-root` names the host folder of the data root (read-only in every
service but the orchestrator, whose batch flows write it) and `--public-domain` adds the HTTPS gateway of
docker-compose.public.yml for a domain name or the server's public IPv4 address; both are kept in `.env` for the next
run. `--no-start` stops once `.env` is ready (CI). Only the standard library is used, so the launcher runs before any
dependency is installed.
"""

from __future__ import annotations

import argparse
import ipaddress
import os
import re
import secrets
import shutil
import subprocess
import sys
import time
import urllib.request
from collections.abc import Callable
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
POSTGRES_VOLUME = "safestart_postgres-data"  # <the `name` in docker-compose.yml>_<volume>
SERVICE_UID = 10001  # the image's user (Dockerfile), as whom the orchestrator writes the data root
LABEL = re.compile(r"(?!-)[a-z0-9-]{1,63}(?<!-)")


def public_host(value: str) -> str:
    """A domain name or a public IPv4 address, as Caddy and Let's Encrypt take it: the host alone, no scheme or port."""
    host = value.strip().lower().rstrip(".")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        labels = host.split(".")
        if len(host) > 253 or len(labels) < 2 or labels[-1].isdigit() or not all(map(LABEL.fullmatch, labels)):
            raise ValueError(f"{value!r} is not a domain name or an IPv4 address (give the host alone, without "
                             "https:// or a port)") from None
        return host
    if address.version != 4 or not address.is_global:
        raise ValueError(f"{value!r} is not a public IPv4 address; a certificate needs an address the internet reaches")
    return str(address)


def unquote(value: str) -> str:
    value = value.strip()
    return value[1:-1] if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"" else value


def read_env(text: str) -> dict[str, str]:
    """The KEY=VALUE settings of a Compose .env file; comments and blank lines are skipped."""
    values = {}
    for line in text.splitlines():
        key, separator, value = line.partition("=")
        if separator and key.strip() and not key.lstrip().startswith("#"):
            values[key.strip()] = unquote(value)
    return values


def env_value(value: str) -> str:
    """Single quotes keep a value with spaces or a '#' literal for Compose."""
    return f"'{value}'" if any(character in value for character in " \t#\"'") else value


def with_values(text: str, updates: dict[str, str]) -> str:
    """The .env text with these settings: a key's line is rewritten in place, a missing key is appended."""
    lines, done = [], set()
    for line in text.splitlines():
        key = line.partition("=")[0].strip()
        if "=" in line and key in updates and not line.lstrip().startswith("#"):
            lines.append(f"{key}={env_value(updates[key])}")
            done.add(key)
        else:
            lines.append(line)
    lines += [f"{key}={env_value(value)}" for key, value in updates.items() if key not in done]
    return "\n".join(lines) + "\n"


def write_private(path: Path, text: str) -> None:
    """Replace the file in one step with one that only its owner may read (it holds the database password)."""
    temp = path.with_name(path.name + ".tmp")
    handle = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(handle, "w", encoding="utf-8", newline="\n") as file:
        file.write(text)
    os.replace(temp, path)


def docker_volume_exists(name: str) -> bool:
    if not shutil.which("docker"):
        return False
    return subprocess.run(["docker", "volume", "inspect", name], capture_output=True, check=False).returncode == 0


def prepare_env(root: Path, *, data_root: str | None = None, public_domain: str | None = None,
                volume_exists: Callable[[str], bool] | None = None) -> dict[str, str]:
    """The settings in root/.env, created on the first run and updated with the options given."""
    path = root / ".env"
    if path.exists():
        text = path.read_text(encoding="utf-8")
        values = read_env(text)
        if not values.get("POSTGRES_PASSWORD"):
            raise SystemExit(f"{path} has no POSTGRES_PASSWORD. Put back the password the database was created with.")
        updates = {key: value for key, value in (("DATA_ROOT", data_root), ("PUBLIC_DOMAIN", public_domain))
                   if value is not None and values.get(key) != value}
        if updates:
            write_private(path, with_values(text, updates))
        return {**values, **updates}
    if (volume_exists or docker_volume_exists)(POSTGRES_VOLUME):
        raise SystemExit(f"The PostgreSQL volume {POSTGRES_VOLUME} of an earlier start still exists and keeps the "
                         "password it was created with. Restore that .env, or delete the old stream store with "
                         "`docker compose down --volumes` and run this again.")
    template = (root / ".env.example").read_text(encoding="utf-8")
    created = {"POSTGRES_PASSWORD": secrets.token_hex(24), "DATA_ROOT": data_root or "./data",
               "PUBLIC_DOMAIN": public_domain or ""}
    write_private(path, with_values(template, created))
    print(f"Created {path} with a random PostgreSQL password.")
    return {**read_env(template), **created}


def data_folder(root: Path, value: str) -> Path:
    """The data root on the host. The default ./data is created empty; any other folder must exist already. On Linux
    the orchestrator writes it as the image's user, so a folder of another owner gets a note on how to hand it over
    (Docker Desktop, on Windows and macOS, shares a folder whatever its owner)."""
    folder = Path(value) if Path(value).is_absolute() else root / value
    if not folder.is_dir():
        if value != "./data":
            raise SystemExit(f"The data root {folder} does not exist. Pass --data-root with the folder holding lake/.")
        folder.mkdir(parents=True)
    for part, user in (("lake", "the replay"), ("warehouse", "the dashboard")):
        if not (folder / part).is_dir():
            print(f"Note: {folder} has no {part}/ yet, which {user} reads; the orchestrator's daily batch builds it.")
    if sys.platform == "linux" and folder.stat().st_uid != SERVICE_UID:
        print(f"Note: the orchestrator writes {folder} as user {SERVICE_UID}; hand the folder over with "
              f"`sudo chown -R {SERVICE_UID}:{SERVICE_UID} {folder}`.")
    return folder


def compose_command(env: dict[str, str]) -> list[str]:
    files = ["-f", "docker-compose.yml"]
    if env.get("PUBLIC_DOMAIN"):
        files += ["-f", "docker-compose.public.yml"]
    return ["docker", "compose", *files]


def healthy(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            return response.status == 200
    except OSError:
        return False


def wait_healthy(url: str, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while not healthy(url):
        if time.monotonic() > deadline:
            return False
        time.sleep(3)
    return True


def start(root: Path, env: dict[str, str], timeout: float) -> int:
    if not shutil.which("docker"):
        raise SystemExit("Docker is required: Docker Desktop on Windows or macOS, Docker Engine with the Compose "
                         "plugin on Linux.")
    command = compose_command(env)
    subprocess.run([*command, "up", "--detach", "--build", "--remove-orphans"], cwd=root, check=True)
    port = env.get("DASHBOARD_HOST_PORT") or "8501"
    print("Waiting for the dashboard ...")
    if not wait_healthy(f"http://127.0.0.1:{port}/_stcore/health", timeout):
        print(f"The dashboard is not healthy after {timeout:.0f} s; see `{' '.join(command)} ps` and "
              f"`{' '.join(command)} logs dashboard`.", file=sys.stderr)
        return 1
    domain = env.get("PUBLIC_DOMAIN")
    print(f"Dashboard: {f'https://{domain}' if domain else f'http://localhost:{port}'}")
    print(f"Kafka for local tools: 127.0.0.1:{env.get('KAFKA_HOST_PORT') or '19092'}")
    print(f"PostgreSQL: postgresql://safestart@127.0.0.1:{env.get('POSTGRES_HOST_PORT') or '15432'}/safestart "
          "(the password is in .env)")
    print("Check the stream end to end: uv run --extra stream python scripts/smoke_test.py")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Start the SafeStart pipeline's Docker Compose stack.")
    parser.add_argument("--public-domain", metavar="HOST",
                        help="serve the dashboard over HTTPS for this domain name or public IPv4 address "
                             "(kept in .env)")
    parser.add_argument("--data-root", metavar="PATH",
                        help="host folder with lake/ and warehouse/, mounted read-only (kept in .env; default ./data)")
    parser.add_argument("--no-start", action="store_true", help="only prepare .env and the data root")
    parser.add_argument("--timeout", type=float, default=300, help="seconds to wait for a healthy dashboard")
    args = parser.parse_args(argv)
    try:
        domain = public_host(args.public_domain) if args.public_domain is not None else None
    except ValueError as problem:
        parser.error(str(problem))
    data_root = Path(args.data_root).expanduser().absolute() if args.data_root else None
    if data_root and not data_root.is_dir():
        parser.error(f"--data-root {data_root} is not a folder")
    env = prepare_env(ROOT, data_root=data_root.as_posix() if data_root else None, public_domain=domain)
    data_folder(ROOT, env.get("DATA_ROOT") or "./data")
    return 0 if args.no_start else start(ROOT, env, args.timeout)


if __name__ == "__main__":
    raise SystemExit(main())
