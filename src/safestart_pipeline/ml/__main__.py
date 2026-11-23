"""`python -m safestart_pipeline.ml train|analyze`: the accident-risk model and the lake analysis for the docs.

Both read the lake's current versions (`<data root>/lake/msha_ogd`, or --lake) and write to `<data root>/models/
accident_risk` and `<data root>/analysis` unless --out names another folder, so a run can read a shared data root it
must not change and write elsewhere.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

from ..config import data_root
from .analysis import write_tables
from .features import DATASETS, build_panel, current_parquet, mine_details
from .train import train

log = logging.getLogger("safestart_pipeline.ml")
DEFAULT_OUT = {"train": "models/accident_risk", "analyze": "analysis"}  # under the data root
HEADLINE = ("roc_auc", "pr_auc", "precision_at_5pct", "recall_at_5pct")


def run_training(lake: Path, out: Path) -> dict:
    started = time.perf_counter()
    panel = build_panel(lake).collect(engine="streaming")
    log.info("panel: %d mine-quarters in %.1f s", panel.height, time.perf_counter() - started)
    run = {
        "lake": {dataset: current_parquet(lake, dataset).name for dataset in DATASETS},
        "panel_rows": panel.height,
        "panel_seconds": round(time.perf_counter() - started, 1),
        "cpu_count": os.cpu_count(),
    }
    metrics = train(panel, out, details=mine_details(lake).collect(), run=run)
    return {
        "out": str(out),
        "selected": metrics["selected"],
        "test": {name: {key: result["test"][key] for key in HEADLINE} for name, result in metrics["models"].items()},
        "scores": metrics["scores"],
        "run": metrics["run"] | {"total_seconds": round(time.perf_counter() - started, 1)},
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m safestart_pipeline.ml",
                                     description="SafeStart's mine accident-risk model and lake analysis")
    commands = parser.add_subparsers(dest="command", required=True)
    for name, help_text in (
        ("train", "build the mine-quarter features, train and test both models, score the open quarter"),
        ("analyze", "write the lake analysis tables for the docs (CSV and Markdown)"),
    ):
        command = commands.add_parser(name, help=help_text)
        command.add_argument("--lake", type=Path, help="the MSHA lake folder (default: <data root>/lake/msha_ogd)")
        command.add_argument("--out", type=Path, help=f"output folder (default: <data root>/{DEFAULT_OUT[name]})")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s", stream=sys.stderr)
    lake = args.lake or data_root() / "lake" / "msha_ogd"
    out = args.out or data_root() / DEFAULT_OUT[args.command]
    result = run_training(lake, out) if args.command == "train" else {"out": str(out), "rows": write_tables(lake, out)}
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
