"""Prefect flow for the model side: train the mine accident-risk model and write the lake analysis tables, each step a
task with retries and its own log. Both read the lake's current versions and write where `python -m
safestart_pipeline.ml train|analyze` writes by default: <data root>/models/accident_risk and <data root>/analysis.

The two steps are independent, so a failed training still writes the analysis; the run is then marked failed at the end.
Run once with `python -m safestart_pipeline.flows run model` or `ssp pipeline --model`.
"""

from __future__ import annotations

from pathlib import Path

from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE

from ..config import data_root
from . import report_progress


def lake() -> Path:
    return data_root() / "lake" / "msha_ogd"


@task(name="train-accident-risk", retries=1, retry_delay_seconds=300, cache_policy=NO_CACHE)
def train_task() -> dict:
    from ..ml.__main__ import DEFAULT_OUT, run_training

    result = run_training(lake(), data_root() / DEFAULT_OUT["train"])
    get_run_logger().info("model in %s: selected %s, test %s", result["out"], result["selected"],
                          result["test"].get(result["selected"]))
    return result


@task(name="analyze-lake", retries=1, retry_delay_seconds=300, cache_policy=NO_CACHE)
def analyze_task() -> dict[str, int]:
    from ..ml.__main__ import DEFAULT_OUT
    from ..ml.analysis import write_tables

    out = data_root() / DEFAULT_OUT["analyze"]
    rows = write_tables(lake(), out)
    get_run_logger().info("analysis tables in %s: %s", out, rows)
    return rows


@flow(name="safestart-model")
def model() -> dict[str, object]:
    """The accident-risk model and the lake analysis tables, from the lake's current versions."""
    report_progress()
    summary: dict[str, object] = {}
    problems: list[str] = []
    for step in (train_task, analyze_task):
        state = step(return_state=True)
        if state.is_failed():
            problems.append(f"{step.name}: {state.message}")
        else:
            summary[step.name] = state.result()
    if problems:
        raise RuntimeError("; ".join(problems))
    return summary
