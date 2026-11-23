"""When the scheduled flows run, stated once for both ways of running them: a Prefect deployment takes a schedule's cron
string, and the local scheduler, for a host without a Prefect API, takes its next run after a given moment. Times are
UTC.

The local scheduler runs one flow at a time, as `prefect.serve` does with a limit of one, so the warehouse build and the
model training never compete for the host's memory. A run that ends after another flow's time leaves that flow due, so
it starts next rather than a week later; a run that outlasts its own flow's next time skips that time rather than
starting again at once.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import NoReturn

SUNDAY = 6  # as datetime.weekday() counts
MAX_SLEEP_SECONDS = 900.0  # wake at least this often, so a changed clock (a host resumed from sleep) is noticed
log = logging.getLogger(__name__)


def utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class Schedule:
    """Every day at hour:minute UTC, or only on `weekday` (Monday 0 to Sunday 6)."""

    hour: int
    minute: int = 0
    weekday: int | None = None

    @property
    def cron(self) -> str:
        day = "*" if self.weekday is None else str((self.weekday + 1) % 7)  # cron counts the days from Sunday, 0
        return f"{self.minute} {self.hour} * * {day}"

    def next_after(self, moment: datetime) -> datetime:
        """The first run strictly after `moment`, a timezone-aware time."""
        moment = moment.astimezone(UTC)
        run = moment.replace(hour=self.hour, minute=self.minute, second=0, microsecond=0)
        if self.weekday is not None:
            run += timedelta(days=(self.weekday - run.weekday()) % 7)
        if run <= moment:
            run += timedelta(days=1 if self.weekday is None else 7)
        return run


def run_on_schedule(schedules: Mapping[str, Schedule], run: Callable[[str], object], *,
                    now: Callable[[], datetime] = utc_now, sleep: Callable[[float], object] = time.sleep) -> NoReturn:
    """Run each named job at its times, one at a time, until interrupted: sleep until the earliest is due, run it, set
    its next time after the run ends, repeat."""
    due = {name: schedule.next_after(now()) for name, schedule in schedules.items()}
    for name, at in due.items():
        log.info("%s: first run at %s UTC", name, f"{at:%Y-%m-%d %H:%M}")
    while True:
        name = min(due, key=due.__getitem__)
        wait = (due[name] - now()).total_seconds()
        if wait > 0:
            sleep(min(wait, MAX_SLEEP_SECONDS))
            continue
        run(name)
        due[name] = schedules[name].next_after(now())
        log.info("%s: next run at %s UTC", name, f"{due[name]:%Y-%m-%d %H:%M}")
