"""Shared helpers for the checks in this folder (see run_checks.sh)."""

import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent  # the repository


def start_server(port, capture=False):
    """Start solution_server.py on `port` and wait until it accepts
    connections. With `capture`, its output can be read from .stdout."""
    output = subprocess.PIPE if capture else subprocess.DEVNULL
    server = subprocess.Popen(
        [sys.executable, "solution_server.py", "--port", str(port)],
        cwd=ROOT, stdout=output, stderr=subprocess.STDOUT, text=True)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if server.poll() is not None:
            raise RuntimeError("solution_server.py exited during start-up")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return server
        except OSError:
            time.sleep(0.1)
    server.terminate()
    raise RuntimeError(f"solution_server.py did not listen on port {port}")
