"""Prefect flows: thin orchestration over the plain functions in the other packages.

- pipeline.py: the daily batch flow (downloads, lake, warehouse, SafeStart exports); acquire.py: its download step.
- model.py: the weekly model flow (accident-risk model, lake analysis tables).
- official.py: the daily flow of the official files (official/): what the sites changed since the last run.
- __main__.py: `python -m safestart_pipeline.flows serve | run`, with or without a Prefect API.
- schedule.py: when the flows run.

Each step imports its packages when it runs, so a process that only serves the flows holds Prefect without Polars,
DuckDB and scikit-learn (measured on Windows: 91 MB after importing the flows, 234 MB with those packages as well).

One writer of the data root at a time. The batch and model flows hold the data root's writer lock for their whole run,
whoever starts them (the schedule, Prefect, `ssp pipeline`, `flows run`), and so does `ssp lake`: two batches at once
would write the same temporary files, Parquet partials and manifests, and a second downloader empties the temporary
folder of the bulk store under the first. A second writer fails at once instead of waiting. The official flow writes
none of those files: it holds each crawl site's own lock (state/crawl-<site>.lock) while it asks that site and writes
its crawl store and objects, and records its runs in state/official.sqlite.
"""

import logging
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path

from ..crawl.locks import SiteLock

PACKAGE = "safestart_pipeline"


def report_progress() -> None:
    """Let the stages' INFO progress lines through (the lake's datasets, the warehouse's tables, the model's fits).
    A flow run served by a Prefect API runs in a process of its own, whose loggers start at WARNING; `flows serve` names
    this package in PREFECT_LOGGING_EXTRA_LOGGERS, so Prefect also sends these lines to the run's log."""
    logging.getLogger(PACKAGE).setLevel(logging.INFO)


@contextmanager
def writer_lock(data_root: Path) -> Iterator[None]:
    """Hold <data root>/state/pipeline.lock, an OS file lock released if the process dies. File locks do not reach
    across Docker Desktop's file sharing, so it cannot keep a process on a workstation and one in a container there
    apart (docs/DEPLOYMENT.md)."""
    path = data_root / "state" / "pipeline.lock"
    with ExitStack() as stack:
        try:
            stack.enter_context(SiteLock(path))
        except RuntimeError as problem:
            message = f"another batch, model or lake run holds {path}; one writer of the data root at a time"
            raise RuntimeError(message) from problem
        yield
