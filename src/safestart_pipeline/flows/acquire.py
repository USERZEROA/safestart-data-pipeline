"""Prefect flow: list each bulk source and download its files, one task run per file, with retries and run logs.

MSHA's open data are on its legacy host, which the arlweb crawl visits. While that crawl runs (it holds its lock), the
flow leaves that host alone and keeps the copies held, with a note in its summary, as `ssp verify bulk --live` does;
the lock only reaches a crawl on the same operating system. A source whose listing fails does not stop the others, and
the run fails at the end if anything failed.

It is the first step of the daily batch flow (pipeline.py); `uv run python -m safestart_pipeline.flows.acquire` runs it
on its own.
"""

from __future__ import annotations

from contextlib import ExitStack

from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE

from ..acquire.bulk import BulkAcquirer, BulkFile
from ..config import data_root
from ..crawl.crawler import make_client
from ..crawl.locks import SiteLock

SOURCES = ("msha-ogd", "ecfr-govinfo")
# The crawl that visits a source's host: while it holds its lock, the source is left alone.
CRAWLED_BY = {"msha-ogd": "arlweb"}


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
    root = data_root()
    failed: list[str] = []
    not_asked: dict[str, str] = {}
    with make_client() as client:
        acquirer = BulkAcquirer(root, client)
        try:
            for source in sources:
                with ExitStack() as stack:
                    if site := CRAWLED_BY.get(source):
                        try:
                            stack.enter_context(SiteLock(root / "state" / f"crawl-{site}.lock"))
                        except RuntimeError:
                            not_asked[source] = f"the {site} crawl is running, so its host was not asked"
                            logger.warning("%s: %s; the copies held stay current", source, not_asked[source])
                            continue
                    try:
                        files = acquirer.list_msha() if source == "msha-ogd" else acquirer.list_ecfr()
                    except RuntimeError as problem:
                        failed.append(f"{source} listing: {problem}")
                        logger.error("%s: %s", source, problem)
                        continue
                    logger.info("%s lists %d files", source, len(files))
                    for item in files:
                        state = download_file(acquirer, item, return_state=True)
                        if state.is_failed():
                            failed.append(item.name)
            summary = acquirer.store.summary() | ({"not_asked": not_asked} if not_asked else {})
        finally:
            acquirer.store.close()
    logger.info("summary: %s", summary)
    if failed:
        raise RuntimeError(f"{len(failed)} failed (a file after its retries), the copies held stay current: "
                           + "; ".join(failed))
    return summary


if __name__ == "__main__":
    acquire_bulk()
