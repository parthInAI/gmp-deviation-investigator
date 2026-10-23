"""lakehouse.cli.exit_now keeps the tool's exit code and flushes its output."""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def run(code):
    prog = f"from lakehouse.cli import exit_now; print('GATE PASSED', end=''); exit_now({code})"
    return subprocess.run([sys.executable, "-c", prog], cwd=ROOT, capture_output=True, text=True, timeout=30)


def test_exit_code_and_output_survive():
    for code, expected in [(0, 0), (1, 1), (None, 0)]:
        r = run(code)
        assert r.returncode == expected
        assert r.stdout == "GATE PASSED"  # printed without a newline, still flushed
