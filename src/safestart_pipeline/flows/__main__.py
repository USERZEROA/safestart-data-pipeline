"""`python -m safestart_pipeline.flows serve | run [batch|model] [--no-download]`: the flows on their schedules, or one
of them now.

The batch flow runs every day at 14:00 UTC, so new files are taken the day they appear: MSHA replaces all 20 open-data
files at once, on no fixed weekday (the bulk store holds two such replacements, Last-Modified Tuesday 2026-11-17 13:54
UTC and Monday 2026-11-23 12:50 UTC). The model flow runs every Sunday at 15:00 UTC, after that day's batch.

`serve` registers both flows as deployments of the Prefect API that PREFECT_API_URL names (with PREFECT_API_KEY for
Prefect Cloud) and executes the runs that API schedules, one at a time. Without PREFECT_API_URL in the environment it
keeps the same schedule in this process (schedule.py): it sleeps until the next run is due, runs the flow, logs the
outcome and repeats, so the container works without Prefect Cloud. Either way a failed run is logged and the schedule
goes on; locally, a run that raises before it starts is tried again 15 minutes later, up to three times. `run` runs
one flow now and prints its outcome.

Run as `python -m`, this module is named "__main__", outside the package's logger that `report_progress` lets through
at INFO, so it logs under a name of its own in the package.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import dataclass

from prefect import Flow, serve
from prefect.schedules import Cron
from prefect.types.entrypoint import EntrypointType

from . import PACKAGE, report_progress
from .model import model
from .pipeline import batch
from .schedule import SUNDAY, Schedule, run_on_schedule

log = logging.getLogger(f"{PACKAGE}.flows")


@dataclass(frozen=True)
class Job:
    flow: Flow
    deployment: str  # the Prefect deployment's name, shown as "<flow name>/<deployment>"
    schedule: Schedule


JOBS = {
    "batch": Job(batch, "daily", Schedule(hour=14)),
    "model": Job(model, "weekly", Schedule(hour=15, weekday=SUNDAY)),
}


def serve_deployments() -> None:
    """Serve every flow as a deployment of the Prefect API, running one flow run at a time."""
    serve(*(job.flow.to_deployment(job.deployment, schedules=[Cron(job.schedule.cron, timezone="UTC")],
                                   entrypoint_type=EntrypointType.MODULE_PATH) for job in JOBS.values()),
          limit=1)


def run_flow(name: str, *, download: bool = True) -> dict[str, object]:
    """Run one flow in this process and return its outcome. A failed run is logged (Prefect logs its traceback) and
    returned rather than raised, so whatever comes next still runs."""
    state = JOBS[name].flow(return_state=True, **({"download": download} if name == "batch" else {}))
    if not state.is_completed():
        log.error("%s run %s: %s", name, state.type.value.lower(), state.message)
        return {"state": state.type.value.lower(), "error": state.message}
    log.info("%s run completed", name)
    return {"state": "completed", "result": state.result()}


def run_once(names: list[str], *, download: bool = True) -> int:
    """Run the named flows now, in order, print their outcomes and return 1 unless all of them completed."""
    outcomes = {name: run_flow(name, download=download) for name in names}
    print(json.dumps(outcomes, indent=2, default=str))
    return int(any(outcome["state"] != "completed" for outcome in outcomes.values()))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m safestart_pipeline.flows",
                                     description="SafeStart's batch and model flows")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("serve", help="run the flows on their schedules: as Prefect deployments when PREFECT_API_URL "
                                      "is set, otherwise in this process")
    once = commands.add_parser("run", help="run one flow now")
    once.add_argument("flow", nargs="?", choices=list(JOBS), default="batch")
    once.add_argument("--no-download", action="store_true", help="batch: rebuild from the downloads already held")
    args = parser.parse_args(argv)
    if args.command == "run" and args.no_download and args.flow != "batch":
        parser.error("--no-download applies to the batch flow")
    report_progress()  # the stages' progress joins the console log
    if args.command == "run":
        return run_once([args.flow], download=not args.no_download)
    if os.environ.get("PREFECT_API_URL"):
        # Served runs execute in processes of their own, which inherit this setting: their package lines reach the API.
        os.environ.setdefault("PREFECT_LOGGING_EXTRA_LOGGERS", PACKAGE)
        serve_deployments()
        return 0
    try:
        run_on_schedule({name: job.schedule for name, job in JOBS.items()}, run_flow)
    except KeyboardInterrupt:
        log.info("stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
