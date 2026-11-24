"""One crawl process per site: an OS file lock that is released automatically if the process dies.

`lock_state` says whether another process holds a lock now; `wait_for_lock` holds one, trying again for a while, for
work that must not run beside a crawl of the site but may wait for a short holder such as a watcher's check.
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from contextlib import ExitStack
from datetime import datetime
from pathlib import Path
from typing import Self


class SiteLock:
    def __init__(self, path: Path):
        self.path = path
        self.handle = None

    def __enter__(self) -> Self:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = open(self.path, "a+b")  # held for the lifetime of the crawl
        try:
            self._lock()
        except OSError as problem:
            self.handle.close()
            self.handle = None
            raise RuntimeError(f"another process holds {self.path.name}; one crawl per site at a time") from problem
        return self

    def __exit__(self, *_: object) -> None:
        if self.handle is None:
            return
        try:
            self._unlock()
        finally:
            self.handle.close()
            self.handle = None

    def _lock(self) -> None:
        if sys.platform == "win32":
            import msvcrt

            self.handle.seek(0)
            msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _unlock(self) -> None:
        if sys.platform == "win32":
            import msvcrt

            self.handle.seek(0)
            msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)


def lock_state(path: Path) -> str:
    """"free" or "held" (another process holds the lock now); "unknown" where it cannot be tried. No lock file is made:
    a holder makes its own."""
    if not path.exists():
        return "free"
    try:
        with SiteLock(path):
            return "free"
    except RuntimeError:
        return "held"
    except OSError:
        return "unknown"


def wait_for_lock(stack: ExitStack, path: Path, *, give_up: datetime, now: Callable[[], datetime],
                  sleep: Callable[[float], None], retry_seconds: float = 30.0) -> bool:
    """Hold the lock at `path` in `stack`, trying again every `retry_seconds` until `give_up`; False when another
    process held it all that time."""
    while True:
        try:
            stack.enter_context(SiteLock(path))
            return True
        except RuntimeError:
            if now() >= give_up:
                return False
            sleep(min(retry_seconds, max((give_up - now()).total_seconds(), 0.0)))
