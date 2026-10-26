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
# machine is delayed or rate-limited, so don't run other tests meanwhile.
#
# Every scenario is run with the default server and again with lag probes
# disabled (LIVERECALL_BACKPRESSURE=0) for the before/after comparison.
# Results go to results/impairment-<date-time>/ unless output_dir is given.
#
# Overrides:
#     ONLY="bw2mbit_100 bw2mbit_300"   run only these scenarios
#     TARGET=../other-checkout          test the server in another checkout of
#                                       the repo (e.g. a git worktree of a
#                                       teammate's branch); results still go
#                                       to this checkout's results/
#     COMPARE=0                         skip the lag-probes-disabled runs
#                                       (the default when TARGET is set)
#     RUNS=3 DURATION=40 PORT=9220 PYTHON=/path/to/python
set -euo pipefail

if [ "$(id -u)" -eq 0 ]; then
  echo "Run this as your normal user, not with sudo; it calls sudo itself." >&2
  exit 1
fi

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="$(cd "${TARGET:-$HERE}" && pwd)"
if [ "$TARGET" = "$HERE" ]; then
  COMPARE="${COMPARE:-1}"
  suffix=""
else
  COMPARE="${COMPARE:-0}"
  suffix="-$(basename "$TARGET")"
fi
OUT="${1:-$HERE/results/impairment-$(date +%Y%m%d-%H%M)$suffix}"
mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"
ONLY="${ONLY:-}"
RUNS="${RUNS:-3}"
DURATION="${DURATION:-40}"
PORT="${PORT:-9220}"
if [ -n "${PYTHON:-}" ]; then
  PY="$PYTHON"
elif [ -x "$HERE/venv/bin/python" ]; then
  PY="$HERE/venv/bin/python"
else
  PY=python3
fi

# label            network profile   run_demo.py arguments
SCENARIOS=(
  "t5_delay_100ms   delay_100ms       --clients 100"
  "t5_delay_loss    delay_loss        --clients 100"
  "t5_loss_3pct     loss_3pct         --clients 100"
  "bw2mbit_100      bandwidth_2mbit   --clients 100"
  "bw2mbit_300      bandwidth_2mbit   --clients 300"
  "bw2mbit_500      bandwidth_2mbit   --clients 500"  # more than 2 Mbit/s of captions
)

selected() { [ -z "$ONLY" ] || [[ " $ONLY " == *" $1 "* ]]; }

# The profile scripts are not executable in the repo, so run them with bash.
apply_profile() { bash "$HERE/network_profiles/$1.sh" lo; }
reset_lo() { bash "$HERE/network_profiles/reset.sh" lo >/dev/null 2>&1 || true; }

run_scenario() {  # run_scenario <label> <run_demo.py args...>
  local label=$1
  shift
  local stem="$OUT/$label"
  echo "$(date +%T)  $label (default server)"
  "$PY" run_demo.py --port "$PORT" --runs "$RUNS" --duration "$DURATION" \
    --output-stem "$stem" "$@" > "$stem.log" 2>&1
  [ "$COMPARE" = 1 ] || return 0
  echo "$(date +%T)  $label (lag probes disabled)"
  LIVERECALL_BACKPRESSURE=0 "$PY" run_demo.py --port "$PORT" --runs "$RUNS" --duration "$DURATION" \
    --output-stem "${stem}_noprobes" "$@" > "${stem}_noprobes.log" 2>&1
}

cd "$TARGET"  # run_demo.py starts the solution_server.py next to it
echo "testing the server in $TARGET ($(git log -1 --format='%h %s' 2>/dev/null || echo 'not a git checkout'))"
sudo -v
# Keep the sudo credential fresh during the run (~26 minutes, ~13 with COMPARE=0).
( while sleep 60; do sudo -n -v 2>/dev/null || exit 0; done ) >/dev/null 2>&1 &
KEEPALIVE=$!
stop_keepalive() { pkill -P "$KEEPALIVE" 2>/dev/null || true; kill "$KEEPALIVE" 2>/dev/null || true; }
trap 'exit 130' INT TERM
trap 'stop_keepalive; reset_lo; echo "loopback reset"' EXIT

for entry in "${SCENARIOS[@]}"; do
  read -r label profile args <<< "$entry"
  selected "$label" || continue
  apply_profile "$profile"
  # shellcheck disable=SC2086  # args is a plain flag list
  run_scenario "$label" $args
done

echo "$(date +%T)  finished; logs and CSVs are in $OUT"
