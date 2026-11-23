"""Prefect flow for the batch side of the pipeline: bring the bulk downloads up to date, rebuild the lake, build the
warehouse from it and write the SafeStart exports, each step a task with retries and its own log.

A failed step lets the later ones run on what is held wherever that is sound, and the run is marked failed at the end,
so the failure is visible without leaving the later stages behind:
- a failed download leaves the copy already held current;
- a LakeBuildError (a dataset's newest version could not be built) leaves that dataset's previous version current
  while the others move on, so the warehouse is built from what the lake holds as current; a retry would meet the same
  version, so there is none. Any other lake failure stops the run;
- a failed warehouse build keeps the previous warehouse, and the exports, which would only repeat the previous ones,
  are skipped.

The run holds the data root's writer lock from start to end (flows/__init__.py), so a second batch or model run fails
at once instead of writing the same files; the warehouse build and the exports also hold the warehouse's lock
(warehouse/lock.py) against `ssp warehouse` and the like, and a step that finds it held fails and is retried later.

Run once with `python -m safestart_pipeline.flows run batch [--no-download]` or `ssp pipeline`.
"""

from __future__ import annotations

from prefect import State, Task, flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from prefect.client.schemas.objects import TaskRun

from ..config import data_root, warehouse_path
from . import report_progress, writer_lock
from .acquire import acquire_bulk


def retry_unless_unbuildable(_task: Task, _run: TaskRun, state: State) -> bool:
    from ..lake.msha_ogd import LakeBuildError

    return not isinstance(state.result(raise_on_failure=False), LakeBuildError)


@task(name="build-lake", retries=1, retry_delay_seconds=60, retry_condition_fn=retry_unless_unbuildable,
      cache_policy=NO_CACHE)
def build_lake_task() -> dict[str, dict]:
    from ..lake.msha_ogd import build_lake

    results = build_lake(data_root())
    current = sum("parquet" in result for result in results.values())
    rebuilt = sorted(name for name, result in results.items() if result.get("versions_built"))
    get_run_logger().info("lake: %d of %d datasets have a current version; rebuilt %s", current, len(results),
                          ", ".join(rebuilt) or "none")
    return results


@task(name="build-warehouse", retries=1, retry_delay_seconds=600, cache_policy=NO_CACHE)
def build_warehouse_task() -> dict:
    from ..warehouse.build import build

    target = warehouse_path()
    result = build(data_root(), target)
    get_run_logger().info("warehouse %s built in %s s; rows %s", target, result.get("seconds"), result.get("rows"))
    return result


@task(name="export-safestart", retries=1, retry_delay_seconds=60, cache_policy=NO_CACHE)
def export_task() -> dict:
    from ..warehouse.exports import export

    out = data_root() / "exports" / "safestart"
    manifest = export(warehouse_path(), out)
    rows = {name: file.get("rows") for name, file in manifest.get("files", {}).items()}
    get_run_logger().info("SafeStart exports in %s: %s", out, rows)
    return manifest


@flow(name="safestart-batch")
def batch(download: bool = True) -> dict[str, object]:
    """Downloads, lake, warehouse and SafeStart exports, each built from the step before."""
    from ..lake.msha_ogd import LakeBuildError

    report_progress()
    logger = get_run_logger()
    summary: dict[str, object] = {}
    problems: list[str] = []
    with writer_lock(data_root()):
        if download:
            acquired = acquire_bulk(return_state=True)
            if acquired.is_failed():
                problems.append(f"acquire-bulk-files: {acquired.message}")
                logger.warning("downloads failed, continuing with the copies held: %s", acquired.message)
            else:
                summary["acquire"] = acquired.result()
        try:
            summary["lake"] = build_lake_task()
        except LakeBuildError as problem:
            problems.append(f"build-lake: {problem}")
            logger.warning("lake: %s; the warehouse is built from the versions the lake holds as current", problem)
        built = build_warehouse_task(return_state=True)
        if built.is_failed():
            problems.append(f"build-warehouse: {built.message}")
            logger.warning("exports skipped: the warehouse and exports already held stay as they are")
        else:
            summary["warehouse"] = built.result()
            exported = export_task(return_state=True)
            if exported.is_failed():
                problems.append(f"export-safestart: {exported.message}")
            else:
                summary["exports"] = exported.result()
    if problems:
        raise RuntimeError("; ".join(problems))
    return summary
