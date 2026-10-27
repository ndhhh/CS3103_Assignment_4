#!/usr/bin/env bash
# TCP feedback traces under several network conditions, for design
# measurements (see experiments/trace_tcp_feedback.py). The server runs in
# open loop (no pings, no network gate), so the traces show what TCP reports
# before any of our control acts on it. Run as your normal user:
#     bash experiments/run_trace_suite.sh [output_dir]
# Conditions with tc need sudo (asked once); loopback is reset on exit.
#
# Overrides: GROUP=network | clean | all, ONLY="delay_loss bw2mbit_500",
#            DURATION=30 PORT=9263 PYTHON=/path/to/python
# Results: results/trace-<date-time>/<condition>/, summary printed at the end.
set -euo pipefail

if [ "$(id -u)" -eq 0 ]; then
  echo "Run this as your normal user, not with sudo; it calls sudo itself." >&2
  exit 1
fi

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-$HERE/results/trace-$(date +%Y%m%d-%H%M)}"
GROUP="${GROUP:-all}"
ONLY="${ONLY:-}"
DURATION="${DURATION:-30}"
PORT="${PORT:-9263}"
PY="${PYTHON:-$HERE/venv/bin/python}"
[ -x "$PY" ] || PY=python3

# label|network|clients|slow percent   (network: none, profile:<name> or tc:<qdisc args>)
CONDITIONS=(
  "loopback_100|none|100|10"
  "loopback_500|none|500|0"
  "delay_100ms|profile:delay_100ms|100|10"
  "delay_loss|profile:delay_loss|100|10"
  "jitter_loss|tc:netem delay 100ms 50ms distribution normal loss 3%|100|10"
  "loss_3pct|profile:loss_3pct|100|10"
  "reorder|profile:reorder|100|10"
  "bw2mbit_300|profile:bandwidth_2mbit|300|0"
  "bw2mbit_500|profile:bandwidth_2mbit|500|0"
  "bw2mbit_shallow_500|tc:tbf rate 2mbit burst 32kbit latency 100ms|500|0"
  "bw2mbit_deep_500|tc:tbf rate 2mbit burst 32kbit latency 1000ms|500|0"
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
reset_lo() {
  [ "$NETWORK_USED" = 1 ] || return 0
  bash "$HERE/network_profiles/reset.sh" lo >/dev/null 2>&1 || true
}
apply_network() {
  case "$1" in
    none) reset_lo ;;
    profile:*) NETWORK_USED=1; bash "$HERE/network_profiles/${1#profile:}.sh" lo ;;
    tc:*) NETWORK_USED=1
          sudo tc qdisc del dev lo root 2>/dev/null || true
          # shellcheck disable=SC2086  # plain tc argument list
          sudo tc qdisc add dev lo root ${1#tc:}
          echo "Applied ${1#tc:} on lo" ;;
  esac
}

need_sudo=0
for entry in "${CONDITIONS[@]}"; do
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
cd "$HERE"
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

done_dirs=()
for entry in "${CONDITIONS[@]}"; do
  IFS='|' read -r label network clients slow <<< "$entry"
  selected "$label" "$network" || continue
  echo "$(date +%T)  $label ($network, $clients clients, $slow% slow)"
  apply_network "$network"
  "$PY" experiments/trace_tcp_feedback.py run "$OUT/$label" --port "$PORT" \
    --clients "$clients" --slow-percent "$slow" --duration "$DURATION" \
    --server-env LIVERECALL_BACKPRESSURE=0 --server-env LIVERECALL_NETWORK_GATE=0 \
    > "$OUT/$label.log" 2>&1 || echo "    FAILED, see $OUT/$label.log"
  done_dirs+=("$OUT/$label")
done
reset_lo

echo
"$PY" experiments/trace_tcp_feedback.py summary "${done_dirs[@]}" | tee "$OUT/summary.txt"
echo "$(date +%T)  finished; results in $OUT"
