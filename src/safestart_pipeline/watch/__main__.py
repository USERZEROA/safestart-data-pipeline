"""`python -m safestart_pipeline.watch once|loop`: the live watchers on their schedules.

Dataset versions every 6 hours (MSHA publishes weekly, and the lake takes each new version when the acquire flow runs),
MSHA's sitemaps every 6 hours, OSHA's once a day (it asks for 10 seconds between requests), and the Federal Register
every hour. `once` runs the checks that are due (all of them the first time) and exits; `loop` keeps running them as they
come due. `--only` picks checks and `--force` runs them now, though OSHA's sitemaps are still read at most once a day.
Events go to Kafka; every check is logged, and stored in stream.source_checks when PostgreSQL is reachable.
"""

from __future__ import annotations

import argparse
import logging
import time
from collections.abc import Callable, Iterable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path

import httpx

from ..cli import setup_logging
from ..config import data_root
from ..crawl.crawler import make_client
from ..crawl.sites import SITES
from .checks import Check, record_check
from .federal_register import check_federal_register
from .publish import Publisher, kafka_publisher
from .sitemaps import check_sitemaps
from .state import WatchState, default_path
from .versions import check_versions

SCHEDULE = {
    "versions": timedelta(hours=6),
    "sitemaps-msha": timedelta(hours=6),
    "sitemaps-osha": timedelta(days=1),
    "federal-register": timedelta(hours=1),
}
MAX_SLEEP_SECONDS = 900.0  # wake at least this often, so a changed clock or a stop request is noticed
log = logging.getLogger("watch")


def utc_now() -> datetime:
    return datetime.now(UTC)


def run_check(name: str, *, root: Path, state: WatchState, publisher: Publisher, client: httpx.Client) -> Check:
    if name == "versions":
        return check_versions(root, state, publisher)
    if name == "federal-register":
        return check_federal_register(state, publisher, client)
    return check_sitemaps(SITES[name.removeprefix("sitemaps-")], root, state, publisher, client)


def due(state: WatchState, names: Iterable[str], now: datetime) -> list[str]:
    return [name for name in names if (last := state.time(f"{name}:checked_at")) is None or now - last >= SCHEDULE[name]]


def seconds_to_next(state: WatchState, names: Iterable[str], now: datetime) -> float:
    waits = [(last + SCHEDULE[name] - now).total_seconds() if (last := state.time(f"{name}:checked_at")) else 0.0
             for name in names]
    return max(min(waits, default=MAX_SLEEP_SECONDS), 0.0)


def run_checks(names: Iterable[str], state: WatchState, run: Callable[[str], Check], *,
               now: Callable[[], datetime] = utc_now, record: Callable[[Check], bool] = record_check) -> list[Check]:
    """Run each check, stamped with its start; a check that fails is recorded as an error and the others still run."""
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
    setup_logging(root / "logs" / "watch.log")
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
