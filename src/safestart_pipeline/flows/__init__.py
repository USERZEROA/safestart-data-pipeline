"""Prefect flows: thin orchestration over the plain functions in the other packages.

- pipeline.py: the daily batch flow (downloads, lake, warehouse, SafeStart exports); acquire.py: its download step.
- model.py: the weekly model flow (accident-risk model, lake analysis tables).
- __main__.py: `python -m safestart_pipeline.flows serve | run`, with or without a Prefect API.
- schedule.py: when the flows run.

Each step imports its packages when it runs, so a process that only serves the flows holds Prefect without Polars,
DuckDB and scikit-learn (measured on Windows: 91 MB after importing the flows, 234 MB with those packages as well).
"""

import logging

PACKAGE = "safestart_pipeline"


def report_progress() -> None:
    """Let the stages' INFO progress lines through (the lake's datasets, the warehouse's tables, the model's fits).
    A flow run served by a Prefect API runs in a process of its own, whose loggers start at WARNING; `flows serve` names
    this package in PREFECT_LOGGING_EXTRA_LOGGERS, so Prefect also sends these lines to the run's log."""
    logging.getLogger(PACKAGE).setLevel(logging.INFO)
