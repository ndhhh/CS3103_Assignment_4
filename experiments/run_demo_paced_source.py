#!/usr/bin/env python3
"""Run run_demo.py with the caption source at a different pace.

run_demo.py starts caption_source.py at its default pace (0.20 s between
revisions, 0.35 s after each final). For robustness experiments this wrapper
also passes --revision-delay and --segment-gap to the source, and sizes the
number of segments to the run the way run_demo.py does for the default pace.
Nothing in the harness is modified. Run it from the checkout whose server is
tested:

    python experiments/run_demo_paced_source.py --revision-delay 0.1 \\
        --segment-gap 0.15 --clients 100 --duration 40 --runs 3

All other arguments go to run_demo.py unchanged.
"""

from __future__ import annotations

import argparse
import math
import os
import subprocess
import sys

sys.path.insert(0, os.getcwd())
import run_demo  # noqa: E402  the harness of the checkout being tested


def main() -> None:
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--revision-delay", type=float, required=True)
    p.add_argument("--segment-gap", type=float, required=True)
    p.add_argument("--duration", type=float, default=20)
    p.add_argument("--source-start-delay", type=float, default=1.0)
    p.add_argument("--source-segments", type=int)
    pace, rest = p.parse_known_args()

    # run_demo.default_source_segments for this pace: the public generator
    # sends three revisions per segment, and the run keeps a short tail.
    per_segment = 2 * pace.revision_delay + pace.segment_gap
    active = max(3.0, pace.duration - pace.source_start_delay - 2.0)
    segments = pace.source_segments or max(4, math.floor(active / per_segment))

    source_args = ["--revision-delay", str(pace.revision_delay),
                   "--segment-gap", str(pace.segment_gap)]
    original_run = subprocess.run

    def run(cmd, *args, **kwargs):
        if any(str(part).endswith("caption_source.py") for part in cmd):
            cmd = [*cmd, *source_args]
        return original_run(cmd, *args, **kwargs)

    run_demo.subprocess.run = run  # run_demo starts only the source with subprocess.run
    sys.argv = [run_demo.__file__, *rest, "--duration", str(pace.duration),
                "--source-start-delay", str(pace.source_start_delay),
                "--source-segments", str(segments)]
    print(f"caption source: {pace.revision_delay} s between revisions, "
          f"{pace.segment_gap} s after each final, {segments} segments", flush=True)
    run_demo.main()


if __name__ == "__main__":
    main()
