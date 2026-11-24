"""Prefect flow for the daily update of the official files (official/): each source a task with its own log line of
counts, one after another, until the run's deadline; then the signed feed's next manifest, and the purge of the
originals of URLs retired more than 30 days ago.

It is served every day at 16:30 UTC (flows/__main__.py), after the 14:00 batch and the Sunday 15:00 model, and stops by
04:30 UTC the next day (official/run.py: the first 04:30 after its scheduled start, or 15 minutes before the batch or
the model if one comes first, for a run started by hand), so the batch and the model never wait behind it when the
flows run one at a time. It holds each site's crawl lock while it asks or purges that site and state/official.lock
while it writes the feed, never the data root's pipeline.lock: it writes only the crawl stores, their objects,
state/official.sqlite and exports/official/, which the batch and model flows do not write.

A source that fails is retried once five minutes later and the others still run; the run then fails at the end, so
Prefect shows it and can notify. The feed is published after every run that was not interrupted (a heartbeat when it
found nothing), and the purge follows it; a feed or a purge that fails fails the run too. Without its signing key
(OFFICIAL_FEED_SIGNING_KEY) the feed writes nothing and logs a warning, which `ssp official status` and the dashboard
repeat, and the purge deletes nothing (a dry run): only the server, whose .env holds the key, deletes retired
originals, never a workstation that runs the orchestrator on its own archive. The run does not fail for it. A crash
loses at most the answer being recorded: the next run, or a retry, goes on from the inventory (official/daily.py), and
the next feed finishes an interrupted one (official/feed.py). Run once
with `python -m safestart_pipeline.flows run official`, or without Prefect with `ssp official run`.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from prefect.runtime import flow_run

from ..config import data_root
from ..crawl.crawler import iso, make_client, utc_now
from ..official import feed, purge
from ..official.run import OfficialRun, deadline, ordered, parse_until
from . import report_progress


@task(name="official-source", task_run_name="{source}", retries=1, retry_delay_seconds=300, cache_policy=NO_CACHE)
def source_task(run: OfficialRun, source: str) -> dict:
    result = run.source(source)
    details = result.get("details", {})
    counts = {key: details[key] for key in ("asked", "changes", "checked_unchanged", "hints") if key in details}
    get_run_logger().info("%s: %s after %d requests (%s); %s; counts %s", source, result["status"], result["requests"],
                          result["stop_reason"], result["note"], counts)
    return result


@task(name="official-feed", cache_policy=NO_CACHE)
def feed_task(root: Path, run_id: str) -> dict:
    result = feed.publish(root, trigger="flow", run_id=run_id)
    logger = get_run_logger()
    if result["outcome"] == "unsigned":
        logger.warning("feed not published: %s", result["note"])
    else:
        logger.info("feed: %s", result["note"])
    return result


@task(name="official-purge", cache_policy=NO_CACHE)
def purge_task(root: Path) -> dict:
    delete = purge.deletes_here()
    logger = get_run_logger()
    if not delete:
        logger.warning("purge: a dry run, nothing deleted: %s is not set, and only the server that signs the feed"
                       " deletes retired originals", feed.KEY_VARIABLE)
    result = purge.purge(root, delete=delete)
    logger.info("purge: %s", "; ".join(f"{site}: {found['note']}" for site, found in result.items()))
    return result


def scheduled_start() -> datetime:
    """When this flow run was due: its scheduled start under a Prefect API, else now."""
    start = flow_run.scheduled_start_time
    return start.astimezone(UTC) if isinstance(start, datetime) else utc_now()


@flow(name="safestart-official")
def official(sources: list[str] | None = None, until: str | None = None, budget: int | None = None) -> dict:
    """The day's run of the official files: www.msha.gov's probe, the eCFR editions, the Federal Register's rules,
    arlweb and OSHA, until `until` (ISO 8601 or HH:MM UTC; by default the deadline of official/run.py), each crawl site
    within `budget` requests (by default its own); then the feed and the purge."""
    report_progress()
    logger = get_run_logger()
    names = ordered(sources)
    stop_at = parse_until(until, utc_now()) if until else deadline(scheduled_start())
    results: dict[str, dict] = {}
    problems: list[str] = []
    root = data_root()
    with make_client() as client:
        run = OfficialRun(root, client, until=stop_at, budget=budget, trigger="flow")
        logger.info("official run %s: %s until %s", run.run_id, ", ".join(names), iso(stop_at))
        ended = "interrupted"
        try:
            for name in names:
                state = source_task(run, name, return_state=True)
                if state.is_failed():
                    problems.append(f"{name}: {state.message}")
                    results[name] = {"status": "error", "note": state.message}
                else:
                    results[name] = state.result()
            ended = "errors" if problems else "done"
        finally:
            run.close(ended)
    logger.info("official run %s: %s", run.run_id, {name: result["status"] for name, result in results.items()})
    summary: dict[str, object] = {"run_id": run.run_id, "until": iso(stop_at), "sources": results}
    for name, step, arguments in (("feed", feed_task, (root, run.run_id)), ("purge", purge_task, (root,))):
        state = step(*arguments, return_state=True)
        if state.is_failed():
            problems.append(f"{name}: {state.message}")
            summary[name] = {"outcome": "error", "note": state.message}
        else:
            summary[name] = state.result()
    if problems:
        raise RuntimeError("; ".join(problems))
    return summary
