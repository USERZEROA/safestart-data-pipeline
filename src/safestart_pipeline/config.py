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


def data_root() -> Path:
    """Where raw files, state, the lake and the warehouse live.

    SAFESTART_DATA_ROOT wins, then `data_root` in pipeline.local.toml, then ./data inside the repository.
    """
    value = os.environ.get("SAFESTART_DATA_ROOT")
    if not value and LOCAL_CONFIG.exists():
        value = tomllib.loads(LOCAL_CONFIG.read_text(encoding="utf-8")).get("data_root")
    root = Path(value) if value else REPO_ROOT / "data"
    root.mkdir(parents=True, exist_ok=True)
    return root
