"""Exit helper for command-line tools that read Delta tables with delta-rs.

delta-rs keeps a native (Rust/tokio) runtime alive in background threads.
Occasionally that runtime aborts while Python tears down at interpreter exit
("terminate called without an active exception", exit code 139), after the
tool has already done its work and printed its result. CI then fails on a
gate that actually passed. The tools below finish by flushing their output and
leaving immediately with their own exit code, so teardown cannot change it.
"""

from __future__ import annotations

import os
import sys


def exit_now(code: int | None) -> None:
    """Flush stdout/stderr, then exit with `code` without interpreter teardown."""
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(int(code or 0))
