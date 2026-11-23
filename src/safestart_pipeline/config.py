"""Settings shared by every pipeline stage."""

from __future__ import annotations

import os
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
LOCAL_CONFIG = REPO_ROOT / "pipeline.local.toml"

# The crawler names itself with a product token only, without a website or contact address. The "compatible" form is
# the common crawler convention; one source answers a bare product token with 403.
USER_AGENT = "Mozilla/5.0 (compatible; SafeStartInventory/0.1)"


def _local_settings() -> dict:
    for candidate in (Path.cwd() / LOCAL_CONFIG.name, LOCAL_CONFIG):
        if candidate.exists():
            return tomllib.loads(candidate.read_text(encoding="utf-8"))
    return {}


def setting(name: str, default: str) -> str:
    """A deployment setting: the environment variable SAFESTART_<NAME> wins, then `<name>` in pipeline.local.toml, then
    the default. Secrets (passwords, API keys) come only from the environment, never from files in the repository."""
    return os.environ.get(f"SAFESTART_{name.upper()}") or str(_local_settings().get(name.lower(), default))


def postgres_url() -> str:
    """The streaming store. The default is the local Compose database (host port 15432), addressed as 127.0.0.1: the
    host ports listen on IPv4 only, and on Windows "localhost" tries IPv6 (::1) first, which stalls every connection."""
    return setting("postgres_url", "postgresql://safestart:safestart@127.0.0.1:15432/safestart")


def kafka_bootstrap() -> str:
    """Kafka brokers. The default is the local Compose broker (host port 19092), at 127.0.0.1 for the same reason."""
    return setting("kafka_bootstrap", "127.0.0.1:19092")


def warehouse_path() -> Path:
    """The DuckDB warehouse built from the lake (rebuilt into a new file and swapped in atomically)."""
    return Path(setting("warehouse_path", str(data_root() / "warehouse" / "safestart.duckdb")))


def data_root() -> Path:
    """Where raw files, state, the lake and the warehouse live.

    SAFESTART_DATA_ROOT wins, then `data_root` in a pipeline.local.toml in the working directory or the repository
    (an installed copy, such as the crawler's own environment, finds it through the working directory), then ./data.
    """
    value = os.environ.get("SAFESTART_DATA_ROOT")
    if not value:
        for candidate in (Path.cwd() / LOCAL_CONFIG.name, LOCAL_CONFIG):
            if candidate.exists():
                value = tomllib.loads(candidate.read_text(encoding="utf-8")).get("data_root")
                break
    root = Path(value) if value else Path.cwd() / "data"
    root.mkdir(parents=True, exist_ok=True)
    return root
