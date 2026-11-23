"""One crawl process per site: an OS file lock that is released automatically if the process dies."""

from __future__ import annotations

import sys
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
