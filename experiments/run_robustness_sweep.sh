#!/usr/bin/env bash
# Robustness sweep: workloads beyond the public tests, to find where the
# server's thresholds stop working (they were tuned on the 2 Mbit/s profile
# and the T5 profiles). Run as your normal user, from anywhere:
#     bash experiments/run_robustness_sweep.sh [output_dir]
#
# Scenarios with a network condition use tc on the loopback interface, which
# needs sudo (asked once); the others run on plain loopback without sudo.
# Loopback is reset when the script exits, including on Ctrl-C. The supplied
# network_profiles/ and harness are used unchanged: extra network conditions
# are tc arguments in the table below, and a faster caption source goes
# through experiments/run_demo_paced_source.py.
#
# Overrides:
#     GROUP=network | clean | all        which scenarios (default all)
#     ONLY="reorder bw2mbit_700"          only these scenarios
#     TARGET=../other-checkout            test the server in another checkout
#     RUNS=3 PORT=9220 PYTHON=/path/to/python
# Results go to results/sweep-<date-time>-<commit of solution_server.py>/
# unless output_dir is given. Compare result folders with
#     python experiments/compare_runs.py results/sweep-A results/sweep-B
set -euo pipefail

if [ "$(id -u)" -eq 0 ]; then
  echo "Run this as your normal user, not with sudo; it calls sudo itself." >&2
  exit 1
fi

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="$(cd "${TARGET:-$HERE}" && pwd)"
# Named after the last commit that changed the server, not the checkout's HEAD.
COMMIT="$(git -C "$TARGET" log -1 --format=%h -- solution_server.py 2>/dev/null || true)"
COMMIT="${COMMIT:-nogit}"
OUT="${1:-$HERE/results/sweep-$(date +%Y%m%d-%H%M)-$COMMIT}"
GROUP="${GROUP:-all}"
ONLY="${ONLY:-}"
RUNS="${RUNS:-3}"
PORT="${PORT:-9220}"
if [ -n "${PYTHON:-}" ]; then
  PY="$PYTHON"
elif [ -x "$HERE/venv/bin/python" ]; then
  PY="$HERE/venv/bin/python"
else
  PY=python3
fi

# label|network|duration|run_demo.py arguments|extra
#   network: none, profile:<supplied network_profiles script>, or tc:<qdisc arguments>
#   extra:   -, source=<revision delay>,<segment gap>, or until=<seconds the
#            network condition lasts before it is removed mid-run>
SCENARIOS=(
  # Network conditions not covered by the public tests
  "reorder|profile:reorder|40|--clients 100|-"
  "jitter_loss|tc:netem delay 100ms 50ms distribution normal loss 3%|40|--clients 100|-"
  "bw2mbit_400|profile:bandwidth_2mbit|40|--clients 400|-"
  "bw2mbit_700|profile:bandwidth_2mbit|40|--clients 700|-"
  "bw1mbit_300|tc:tbf rate 1mbit burst 32kbit latency 400ms|40|--clients 300|-"
  "bw2mbit_shallow_500|tc:tbf rate 2mbit burst 32kbit latency 100ms|40|--clients 500|-"
  "bw2mbit_deep_500|tc:tbf rate 2mbit burst 32kbit latency 1000ms|40|--clients 500|-"
  # Combinations and changes during a run
  "slow_bw2mbit_300|profile:bandwidth_2mbit|60|--clients 300 --slow-percent 10 --slow-ms 500|-"
  "slow_delay100|profile:delay_100ms|60|--clients 100 --slow-percent 10 --slow-ms 500|-"
  "fastsrc_bw2mbit_300|profile:bandwidth_2mbit|40|--clients 300|source=0.1,0.15"
  "bw2mbit_removed_500|profile:bandwidth_2mbit|40|--clients 500|until=20"
  # Plain loopback (no sudo)
  "scale_1000|none|40|--clients 1000|-"
  "mass_reconnect_300|none|60|--clients 300 --disconnect-percent 50 --disconnect-at 15 --disconnect-seconds 5|-"
  "slow_2s|none|60|--clients 100 --slow-percent 10 --slow-ms 2000|-"
  "slow_30pct|none|60|--clients 100 --slow-percent 30 --slow-ms 500|-"
  "fastsrc_100|none|40|--clients 100|source=0.1,0.15"
)

selected() {  # selected <label> <network>
  [ -z "$ONLY" ] || [[ " $ONLY " == *" $1 "* ]] || return 1
  case "$GROUP" in
    network) [ "$2" != none ] ;;
    clean) [ "$2" = none ] ;;
    *) true ;;
  esac
}

NETWORK_USED=0
apply_network() {  # apply_network <network>
  case "$1" in
    none)
      reset_lo ;;
    profile:*)
      NETWORK_USED=1
      bash "$HERE/network_profiles/${1#profile:}.sh" lo ;;
    tc:*)
      NETWORK_USED=1
      sudo tc qdisc del dev lo root 2>/dev/null || true
      # shellcheck disable=SC2086  # plain tc argument list
      sudo tc qdisc add dev lo root ${1#tc:}
      echo "Applied ${1#tc:} on lo" ;;
  esac
}
reset_lo() {
  [ "$NETWORK_USED" = 1 ] || return 0
  bash "$HERE/network_profiles/reset.sh" lo >/dev/null 2>&1 || true
}

run_scenario() {  # run_scenario <label> <network> <duration> <args> <extra>
  local label=$1 network=$2 duration=$3 args=$4 extra=$5
  local stem="$OUT/$label" demo=(run_demo.py)
  if [[ $extra == source=* ]]; then
    local pace=${extra#source=}
    demo=("$HERE/experiments/run_demo_paced_source.py"
          --revision-delay "${pace%,*}" --segment-gap "${pace#*,}")
  fi
  local note=""
  [ "$extra" = - ] || note=", $extra"
  echo "$(date +%T)  $label ($network$note)"
  if [[ $extra == until=* ]]; then
    # The condition lasts only the first part of each run, so runs go one at
    # a time; +2 s covers server start-up and the source's start delay.
    : > "$stem.log"
    local run
    for ((run = 1; run <= RUNS; run++)); do
      apply_network "$network" >> "$stem.log" 2>&1
      # shellcheck disable=SC2086  # args is a plain flag list
      "$PY" "${demo[@]}" --port "$PORT" --runs 1 --duration "$duration" \
        --output-stem "$(printf '%s_run%02d' "$stem" "$run")" $args >> "$stem.log" 2>&1 &
      local pid=$!
      sleep "$(( ${extra#until=} + 2 ))"
      reset_lo
      echo "network condition removed" >> "$stem.log"
      wait "$pid" || echo "    run $run FAILED, see $stem.log"
    done
  else
    apply_network "$network"
    # shellcheck disable=SC2086  # args is a plain flag list
    "$PY" "${demo[@]}" --port "$PORT" --runs "$RUNS" --duration "$duration" \
      --output-stem "$stem" $args > "$stem.log" 2>&1 || echo "    FAILED, see $stem.log"
  fi
}

need_sudo=0
for entry in "${SCENARIOS[@]}"; do
  IFS='|' read -r label network _ <<< "$entry"
  if selected "$label" "$network" && [ "$network" != none ]; then need_sudo=1; fi
done

while pgrep -f '[l]oad_clients\.py' >/dev/null; do
  echo "another experiment is running; waiting for it to finish..."
  sleep 10
done
if [ "$need_sudo" = 0 ] && ! tc qdisc show dev lo | grep -q noqueue; then
  echo "loopback is shaped ($(tc qdisc show dev lo)); reset it first" >&2
  exit 1
fi

mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"
cd "$TARGET"  # run_demo.py starts the solution_server.py next to it
echo "testing the server in $TARGET ($(git log -1 --format='%h %s' 2>/dev/null || echo 'not a git checkout'))"
if [ "$need_sudo" = 1 ]; then
  sudo -v
  ( while sleep 60; do sudo -n -v 2>/dev/null || exit 0; done ) >/dev/null 2>&1 &
  KEEPALIVE=$!
  stop_keepalive() { pkill -P "$KEEPALIVE" 2>/dev/null || true; kill "$KEEPALIVE" 2>/dev/null || true; }
else
  stop_keepalive() { :; }
fi
trap 'exit 130' INT TERM
trap 'stop_keepalive; reset_lo; [ "$NETWORK_USED" = 1 ] && echo "loopback reset"; true' EXIT

for entry in "${SCENARIOS[@]}"; do
  IFS='|' read -r label network duration args extra <<< "$entry"
  selected "$label" "$network" || continue
  run_scenario "$label" "$network" "$duration" "$args" "$extra"
done

echo "$(date +%T)  finished; results in $OUT"
