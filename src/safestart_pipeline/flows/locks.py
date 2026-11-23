"""One writer of the warehouse at a time. A build starts by discarding any half-written build of the same warehouse and
ends by swapping its own file into place, and an export rewrites the files any other export writes, so two writers at
once would corrupt each other's work. The batch flow and `ssp warehouse build|export` therefore hold this lock, a file
beside the warehouse, while they write; a second writer fails at once instead of waiting.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path

from ..crawl.locks import SiteLock


@contextmanager
def warehouse_lock(target: Path) -> Iterator[None]:
    """Hold the lock of the warehouse at `target`; the operating system releases it if the process dies."""
    with ExitStack() as stack:
        try:
            stack.enter_context(SiteLock(target.with_name(f"{target.name}.lock")))
        except RuntimeError as problem:
            raise RuntimeError(f"another run is building or exporting {target}; one at a time") from problem
        yield
