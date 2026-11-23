"""Prefect flows: thin orchestration over the plain functions in the other packages.

- pipeline.py: the daily batch flow (downloads, lake, warehouse, SafeStart exports); acquire.py: its download step.
- model.py: the weekly model flow (accident-risk model, lake analysis tables).
- __main__.py: `python -m safestart_pipeline.flows serve | run`, with or without a Prefect API.
- schedule.py: when the flows run; locks.py: one writer of the warehouse at a time.

Each step imports its packages when it runs, so a process that only serves the flows holds Prefect without Polars,
DuckDB and scikit-learn (measured on Windows: 91 MB after importing the flows, 234 MB with those packages as well).
"""
