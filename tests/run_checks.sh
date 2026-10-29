#!/usr/bin/env bash
# Correctness checks for solution_server.py. Run from anywhere:
#     bash tests/run_checks.sh
# Each check that needs a server starts its own on a private port
# (9301-9305); cork_segments.py reads TCP_INFO, so the checks need Linux.
# Takes about two minutes and exits non-zero if any check fails.
#
# Overrides: PYTHON=/path/to/python
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="${PYTHON:-$HERE/venv/bin/python}"
[ -x "$PY" ] || PY=python3
cd "$HERE"

failed=0
for check in state_rules control_loops source_path catchup_history cork_segments return_path_stall; do
  if out=$("$PY" "tests/$check.py" 2>&1); then
    echo "PASS  $check"
  else
    echo "FAIL  $check"
    echo "$out" | tail -20 | sed 's/^/      /'
    failed=1
  fi
done
exit "$failed"
