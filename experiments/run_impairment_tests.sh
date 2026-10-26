#!/usr/bin/env bash
# T5 network impairment and constrained-bandwidth experiments (Ubuntu,
# tc/netem on the loopback interface).
#
# Run as your normal user, from anywhere:
#     bash experiments/run_impairment_tests.sh [output_dir]
#
# It asks for your sudo password once: only the network_profiles scripts need
# it (they run `sudo tc` themselves). Loopback is always reset when the script
# exits, including on Ctrl-C. While it runs, ALL loopback traffic on this
# machine is delayed or rate-limited.
#
# Every scenario is run with the default server and again with lag probes
# disabled (LIVERECALL_BACKPRESSURE=0) for the before/after comparison.
# Overrides: PYTHON=/path/to/python RUNS=3 DURATION=40 PORT=9220
set -euo pipefail

if [ "$(id -u)" -eq 0 ]; then
  echo "Run this as your normal user, not with sudo; it calls sudo itself." >&2
  exit 1
fi

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"
OUT="${1:-results/impairment}"
RUNS="${RUNS:-3}"
DURATION="${DURATION:-40}"
PORT="${PORT:-9220}"
if [ -n "${PYTHON:-}" ]; then
  PY="$PYTHON"
elif [ -x venv/bin/python ]; then
  PY=venv/bin/python
else
  PY=python3
fi
mkdir -p "$OUT"

# The profile scripts are not executable in the repo, so run them with bash.
apply_profile() { bash "network_profiles/$1.sh" lo; }
reset_lo() { bash network_profiles/reset.sh lo >/dev/null 2>&1 || true; }

sudo -v
# Keep the sudo credential fresh during the ~25 minute run.
( while sleep 60; do sudo -n -v 2>/dev/null || exit 0; done ) &
KEEPALIVE=$!
trap 'exit 130' INT TERM
trap 'kill "$KEEPALIVE" 2>/dev/null || true; reset_lo; echo "loopback reset"' EXIT

run_scenario() {  # run_scenario <label> <run_demo.py args...>
  local label=$1
  shift
  local stem="$OUT/$label"
  echo "$(date +%T)  $label (default server)"
  "$PY" run_demo.py --port "$PORT" --runs "$RUNS" --duration "$DURATION" \
    --output-stem "$stem" "$@" > "$stem.log" 2>&1
  echo "$(date +%T)  $label (lag probes disabled)"
  LIVERECALL_BACKPRESSURE=0 "$PY" run_demo.py --port "$PORT" --runs "$RUNS" --duration "$DURATION" \
    --output-stem "${stem}_noprobes" "$@" > "${stem}_noprobes.log" 2>&1
}

for profile in delay_100ms delay_loss loss_3pct; do
  apply_profile "$profile"
  run_scenario "t5_$profile" --clients 100
done

apply_profile bandwidth_2mbit
run_scenario bw2mbit_100 --clients 100
run_scenario bw2mbit_300 --clients 300

echo "$(date +%T)  finished; logs and CSVs are in $REPO/$OUT"
