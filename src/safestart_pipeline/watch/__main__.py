"""`python -m safestart_pipeline.watch once|loop`: the live watchers on their schedules (schedule.py).

Dataset versions every 6 hours (MSHA publishes weekly, and the lake takes each new version when the acquire flow runs),
MSHA's sitemaps every 6 hours, OSHA's once a day (it asks for 10 seconds between requests), and the Federal Register
every hour. A check skipped because its site's crawl was running is due again sooner, as the check says (`retry_in`,
an hour for the sitemaps), so it is not put off a whole interval at a time while the crawl or the daily run of the
official files writes. `once` runs the checks that are due (all of them the first time) and exits; `loop` keeps running
them as they come due. `--only` picks checks and `--force` runs them now, though OSHA's sitemaps are still read at most
once a day.
Events go to Kafka; every check is logged, and stored in stream.source_checks when PostgreSQL is reachable.

The log goes to standard error and to `<data root>/logs/watch.log`, or to the file SAFESTART_WATCH_LOG (`watch_log` in
pipeline.local.toml) names; "-" keeps standard error alone. The containers set "-": their data root is read-only, and
Docker keeps and rotates each service's output. The watcher's state is set apart the same way (state.py).
"""

from __future__ import annotations

import argparse
import logging
import time
from collections.abc import Callable, Iterable
from dataclasses import replace
from datetime import UTC, datetime
from functools import partial
from pathlib import Path

import httpx

from ..cli import setup_logging
from ..config import data_root, setting
from ..crawl.crawler import make_client
from ..crawl.sites import SITES
from .checks import Check, record_check
from .federal_register import check_federal_register
from .publish import Publisher, kafka_publisher
from .schedule import SCHEDULE
from .sitemaps import check_sitemaps
from .state import WatchState, default_path
from .versions import check_versions

MAX_SLEEP_SECONDS = 900.0  # wake at least this often, so a changed clock or a stop request is noticed
log = logging.getLogger("watch")


def utc_now() -> datetime:
    return datetime.now(UTC)


def log_file(root: Path) -> Path | None:
    """The watcher's log file, or None to log to standard error alone (see the module docstring)."""
    value = setting("watch_log", str(root / "logs" / "watch.log"))
    return None if value == "-" else Path(value)


def start_logging(path: Path | None) -> None:
    if path is not None:
        setup_logging(path)
        return
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)


def run_check(name: str, *, root: Path, state: WatchState, publisher: Publisher, client: httpx.Client) -> Check:
    if name == "versions":
        return check_versions(root, state, publisher)
    if name == "federal-register":
        return check_federal_register(state, publisher, client)
    return check_sitemaps(SITES[name.removeprefix("sitemaps-")], root, state, publisher, client)


def next_time(state: WatchState, name: str) -> datetime | None:
    """When a check is next due: an interval after its last run, or sooner when that run asked to be retried; None for
    a check that never ran."""
    last = state.time(f"{name}:checked_at")
    if last is None:
        return None
    retry = state.time(f"{name}:retry_at")
    return min(last + SCHEDULE[name], retry) if retry else last + SCHEDULE[name]


def due(state: WatchState, names: Iterable[str], now: datetime) -> list[str]:
    return [name for name in names if (at := next_time(state, name)) is None or now >= at]


def seconds_to_next(state: WatchState, names: Iterable[str], now: datetime) -> float:
    waits = [(at - now).total_seconds() if (at := next_time(state, name)) else 0.0 for name in names]
    return max(min(waits, default=MAX_SLEEP_SECONDS), 0.0)


def run_checks(names: Iterable[str], state: WatchState, run: Callable[[str], Check], *,
               now: Callable[[], datetime] = utc_now, record: Callable[[Check], bool] = record_check) -> list[Check]:
    """Run each check, stamped with its start; a check that fails is recorded as an error and the others still run. A
    check that asks to be retried (`retry_in`) is due again that much after its start."""
    checks = []
    for name in names:
        started = now()
        try:
            check = run(name)
        except Exception as problem:  # one failing source must not stop the other watchers
            log.exception("%s failed", name)
            check = Check(name, "error", note=f"{type(problem).__name__}: {problem}"[:500])
        check = replace(check, checked_at=started)
        state.set_time(f"{name}:checked_at", started)
        if check.retry_in is not None:
            state.set_time(f"{name}:retry_at", started + check.retry_in)
        else:
            state.clear_time(f"{name}:retry_at")
        state.commit()
        record(check)
        checks.append(check)
    return checks


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m safestart_pipeline.watch", description="SafeStart live watchers")
    parser.add_argument("mode", choices=("once", "loop"), help="run the due checks once, or keep running them")
    parser.add_argument("--only", nargs="+", choices=list(SCHEDULE), default=list(SCHEDULE), metavar="CHECK",
                        help=f"run only these checks ({', '.join(SCHEDULE)})")
    parser.add_argument("--force", action="store_true", help="run the chosen checks now, whatever their schedule")
    args = parser.parse_args(argv)
    root = data_root()
    start_logging(log_file(root))
    try:
        publisher = kafka_publisher()
    except RuntimeError as problem:
        log.error("%s", problem)
        return 2
    state = WatchState(default_path())
    try:
        with make_client() as client:
            run = partial(run_check, root=root, state=state, publisher=publisher, client=client)
            run_checks(args.only if args.force else due(state, args.only, utc_now()), state, run)
            while args.mode == "loop":
                time.sleep(min(max(seconds_to_next(state, args.only, utc_now()), 1.0), MAX_SLEEP_SECONDS))
                run_checks(due(state, args.only, utc_now()), state, run)
    except KeyboardInterrupt:
        log.info("stopped")
    finally:
        state.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
