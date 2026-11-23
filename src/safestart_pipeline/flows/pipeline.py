"""Prefect flow for the batch side of the pipeline: bring the bulk downloads up to date, then rebuild what depends on
them, each step a task with retries and its own log.

A failed download leaves the copy already held current, so the later steps still run on what is held; the run is then
marked failed at the end, so the failure is visible without leaving the lake behind the downloads that did succeed.

Run once with `uv run python -m safestart_pipeline.flows.pipeline` (`--no-download` rebuilds from what is held).
"""

from __future__ import annotations

import argparse

from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE

from ..config import data_root
from .acquire import acquire_bulk


@task(name="build-lake", retries=1, retry_delay_seconds=60, cache_policy=NO_CACHE)
def build_lake_task(only: list[str] | None = None) -> dict[str, dict]:
    from ..lake.msha_ogd import build_lake  # the analysis extra

    results = build_lake(data_root(), only)
    rebuilt = sorted(name for name, result in results.items() if result.get("versions_built"))
    get_run_logger().info("lake: %d datasets current; rebuilt %s", len(results), ", ".join(rebuilt) or "none")
    return results


@flow(name="safestart-batch")
def batch(download: bool = True) -> dict[str, object]:
    logger = get_run_logger()
    summary: dict[str, object] = {}
    problems: list[str] = []
    if download:
        state = acquire_bulk(return_state=True)
        if state.is_failed():
            problems.append(f"acquire-bulk-files: {state.message}")
            logger.warning("downloads failed, continuing with the copies held: %s", state.message)
        else:
            summary["acquire"] = state.result()
    summary["lake"] = build_lake_task()
    if problems:
        raise RuntimeError("; ".join(problems))
    return summary


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m safestart_pipeline.flows.pipeline",
                                     description="Run the batch pipeline once.")
    parser.add_argument("--no-download", action="store_true", help="rebuild from the downloads already held")
    args = parser.parse_args(argv)
    batch(download=not args.no_download)


if __name__ == "__main__":
    main()
