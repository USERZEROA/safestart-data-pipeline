"""Prefect flow: list each bulk source and download its files, one task run per file, with retries and run logs.

It is the first step of the daily batch flow (pipeline.py); `uv run python -m safestart_pipeline.flows.acquire` runs it
on its own.
"""

from __future__ import annotations

from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE

from ..acquire.bulk import BulkAcquirer, BulkFile
from ..config import data_root
from ..crawl.crawler import make_client

SOURCES = ("msha-ogd", "ecfr-govinfo")


@task(name="download-bulk-file", task_run_name="{item.name}", retries=2, retry_delay_seconds=300, cache_policy=NO_CACHE)
def download_file(acquirer: BulkAcquirer, item: BulkFile) -> dict[str, object]:
    outcome = acquirer.download(item)
    get_run_logger().info("%s: %s", item.name, outcome)
    if outcome["state"] == "error" or outcome.get("verified") is False:
        raise RuntimeError(f"{item.name} did not download cleanly: {outcome}")
    return outcome


@flow(name="acquire-bulk-files")
def acquire_bulk(sources: tuple[str, ...] = SOURCES) -> dict[str, object]:
    logger = get_run_logger()
    failed: list[str] = []
    with make_client() as client:
        acquirer = BulkAcquirer(data_root(), client)
        try:
            for source in sources:
                files = acquirer.list_msha() if source == "msha-ogd" else acquirer.list_ecfr()
                logger.info("%s lists %d files", source, len(files))
                for item in files:
                    state = download_file(acquirer, item, return_state=True)
                    if state.is_failed():
                        failed.append(item.name)
            summary = acquirer.store.summary()
        finally:
            acquirer.store.close()
    logger.info("summary: %s", summary)
    if failed:
        raise RuntimeError(f"{len(failed)} file(s) failed after retries: {failed}")
    return summary


if __name__ == "__main__":
    acquire_bulk()
