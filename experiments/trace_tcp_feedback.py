#!/usr/bin/env python3
"""Trace what TCP tells the server about each client, for design measurements.

    python experiments/trace_tcp_feedback.py run OUT_DIR [--clients 100 ...]
    python experiments/trace_tcp_feedback.py window OUT_DIR
    python experiments/trace_tcp_feedback.py ages OUT_DIR
    python experiments/trace_tcp_feedback.py summary DIR [DIR ...]

`run` starts the server from the current checkout in this process (code
unchanged; experiment switches via --server-env), the supplied
load_clients.py and caption_source.py, and samples every client connection
every --interval seconds: bytes the client's TCP acknowledged (snd_una), the
window the client advertises, its window scale, the bytes and messages
written, and how long the oldest message not yet acknowledged has been
waiting (every write's end position in the stream is recorded). The client
CSV (clients.csv) records which clients are slow and when their application
read each message. Apply a network profile to lo beforehand to trace under
impairment.

`window` asks whether the receive window's right edge (acknowledged +
window) shows a client application that stops reading. `ages` gives the
distribution of the oldest-unacknowledged age and the share of time it is
above candidate limits D; `summary` prints that for several trace folders.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import math
import os
import socket
import statistics
import struct
import sys
import time
from collections import defaultdict, deque

LIMITS = (0.25, 0.3, 0.4, 0.5)  # candidate D values, seconds


def run(args) -> None:
    for item in args.server_env:
        key, _, value = item.partition("=")
        os.environ[key] = value
    sys.path.insert(0, os.getcwd())
    import solution_server as server  # noqa: E402  flags are read at import

    original_write = server.ClientSession.write

    async def write(self, text):
        """The server's write, plus where the message ends in the stream."""
        await original_write(self, text)
        end = server.stream_position(self.ws)
        if end is not None:
            self.__dict__.setdefault("trace_writes", deque()).append((end, time.time()))

    server.ClientSession.write = write

    os.makedirs(args.out, exist_ok=True)
    samples_path = os.path.join(args.out, "samples.csv")
    clients_path = os.path.join(args.out, "clients.csv")
    segments = max(4, math.floor(max(3.0, args.duration - 1.0 - 2.0) / 0.75))  # as run_demo.py

    async def sample(writer, stop: asyncio.Event) -> None:
        while not stop.is_set():
            now = time.time()
            for session in list(server.SESSIONS.values()):
                for client in list(session.clients):
                    info = server.tcp_info(client.ws)
                    if info is None or len(info) < server.TCPI_SND_WND + 4:
                        continue
                    acked = struct.unpack_from("<Q", info, server.TCPI_BYTES_ACKED)[0]
                    window = struct.unpack_from("<I", info, server.TCPI_SND_WND)[0]
                    wscale = info[6] & 0x0F  # tcpi_snd_wscale: the client's window scale
                    written = server.stream_position(client.ws)
                    writes = client.__dict__.get("trace_writes") or deque()
                    while writes and writes[0][0] <= acked:
                        writes.popleft()  # acknowledged
                    age = now - writes[0][1] if writes else 0.0
                    writer.writerow([f"{now:.4f}", client.client_id, acked, window, wscale,
                                     written if written is not None else "", client.frames_sent,
                                     client.mode, f"{age:.4f}"])
            await asyncio.sleep(args.interval)

    async def main() -> None:
        task = asyncio.create_task(server.main_async(args.port))
        for _ in range(50):
            try:
                socket.create_connection(("127.0.0.1", args.port), timeout=0.2).close()
                break
            except OSError:
                await asyncio.sleep(0.1)
        uri = f"ws://127.0.0.1:{args.port}"
        clients = await asyncio.create_subprocess_exec(
            sys.executable, "-u", "load_clients.py", "--uri", uri,
            "--clients", str(args.clients), "--duration", str(args.duration),
            "--output", clients_path, "--history-seconds", "30",
            "--slow-percent", str(args.slow_percent), "--slow-ms", str(args.slow_ms),
            "--disconnect-percent", "0", "--disconnect-at", "10", "--disconnect-seconds", "5",
            "--late-percent", "0", "--late-seconds", "5", "--seed", str(args.seed))
        stop = asyncio.Event()
        with open(samples_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["t", "client_id", "acked", "window", "wscale", "written",
                             "frames", "mode", "unacked_age"])
            sampler = asyncio.create_task(sample(writer, stop))
            await asyncio.sleep(1.0)
            source = await asyncio.create_subprocess_exec(
                sys.executable, "-u", "caption_source.py", "--uri", uri,
                "--segments", str(segments), "--quiet")
            await source.wait()
            await clients.wait()
            stop.set()
            await sampler
        task.cancel()

    asyncio.run(main())
    print(f"samples in {samples_path}, client log in {clients_path}")


def percentile(values: list[float], q: float) -> float:
    if not values:
        return float("nan")
    values = sorted(values)
    return values[min(len(values) - 1, int(q * len(values)))]


def age_stats(out: str) -> dict:
    """Per client group: oldest-unacknowledged ages (s) over all samples, and
    message latencies (ms) as the clients saw them."""
    slow, latency = set(), defaultdict(list)
    with open(os.path.join(out, "clients.csv"), newline="") as f:
        for row in csv.DictReader(f):
            group = "slow" if row["slow"] == "1" else "normal"
            if row["slow"] == "1":
                slow.add(row["client_id"])
            if row["event_type"] == "CAPTION_UPDATE" and row["latency_ms"]:
                latency[group].append(float(row["latency_ms"]))
    ages, over = defaultdict(list), defaultdict(set)
    with open(os.path.join(out, "samples.csv"), newline="") as f:
        for row in csv.DictReader(f):
            group = "slow" if row["client_id"] in slow else "normal"
            age = float(row["unacked_age"])
            ages[group].append(age)
            if age > 0.4:
                over[group].add(row["client_id"])
    return {g: {"ages": ages[g], "latency": latency[g], "clients_over": len(over[g])}
            for g in ages}


def describe_ages(stats: dict) -> str:
    a = stats["ages"]
    share = "  ".join(f">{d * 1000:.0f}ms {100 * sum(x > d for x in a) / len(a):6.3f}%" for d in LIMITS)
    return (f"age P50 {1000 * percentile(a, .5):5.0f}  P99 {1000 * percentile(a, .99):5.0f}  "
            f"P99.9 {1000 * percentile(a, .999):5.0f}  max {1000 * max(a):6.0f} ms | {share} | "
            f"clients ever >400ms {stats['clients_over']:3} | "
            f"latency P95 {percentile(stats['latency'], .95):5.0f} ms")


def ages(args) -> None:
    for group, stats in sorted(age_stats(args.out).items()):
        print(f"{group:6} ({len(stats['ages'])} samples): {describe_ages(stats)}")


def summary(args) -> None:
    width = max(len(os.path.basename(os.path.normpath(d))) for d in args.dirs)
    for d in args.dirs:
        name = os.path.basename(os.path.normpath(d))
        try:
            stats = age_stats(d)
        except FileNotFoundError:
            print(f"{name:{width}}  (no trace)")
            continue
        for group, s in sorted(stats.items()):
            print(f"{name:{width}} {group:6} {describe_ages(s)}")


def window(args) -> None:
    slow, reads = set(), defaultdict(list)
    with open(os.path.join(args.out, "clients.csv"), newline="") as f:
        for row in csv.DictReader(f):
            if row["slow"] == "1":
                slow.add(row["client_id"])
            if row["event_type"] in ("CAPTION_UPDATE", "HISTORY") and row["receive_ts"]:
                reads[row["client_id"]].append(float(row["receive_ts"]))
    series = defaultdict(list)  # client -> [(t, acked, right edge, frames, wscale)]
    with open(os.path.join(args.out, "samples.csv"), newline="") as f:
        for row in csv.DictReader(f):
            acked, window = int(row["acked"]), int(row["window"])
            series[row["client_id"]].append((float(row["t"]), acked, acked + window,
                                             int(row["frames"]), int(row["wscale"]), window))

    def windows(points, span):
        """(start, end) sample pairs `span` seconds apart."""
        j = 0
        for i, start in enumerate(points):
            while j < len(points) and points[j][0] < start[0] + span:
                j += 1
            if j >= len(points):
                return
            yield start, points[j]

    report = {"slow": [], "normal": []}
    for client, points in series.items():
        group = "slow" if client in slow else "normal"
        stalled, total, first_stall = 0, 0, None
        for a, b in windows(points, args.span):
            d_acked, d_right = b[1] - a[1], b[2] - a[2]
            if d_acked < args.min_bytes:
                continue  # too little delivered to judge
            total += 1
            if d_right <= args.stall_ratio * d_acked:
                stalled += 1
                first_stall = first_stall or a[0]
        # Bytes per message, to turn right-edge progress into messages.
        frames = points[-1][3] - points[0][3]
        per_msg = (points[-1][1] - points[0][1]) / frames if frames > 0 else float("nan")
        steps = [abs(b[5] - a[5]) for a, b in zip(points, points[1:]) if b[5] != a[5]]
        entry = {"client": client, "windows": total, "stalled": stalled, "first_stall": first_stall,
                 "per_msg": per_msg, "wscale": points[-1][4],
                 "step": statistics.median(steps) if steps else 0}
        if group == "slow" and first_stall is not None:
            # Right-edge progress after the first stall, as messages per second,
            # against the rate at which the application actually read.
            later = [p for p in points if p[0] >= first_stall]
            if len(later) > 1 and per_msg == per_msg and per_msg > 0:
                dt = later[-1][0] - later[0][0]
                entry["edge_rate"] = (later[-1][2] - later[0][2]) / per_msg / dt
                app = [t for t in reads[client] if later[0][0] <= t <= later[-1][0]]
                entry["app_rate"] = len(app) / dt
            # When did the client library fall 16 messages behind? Messages
            # written minus messages its application had read by then.
            app_reads = sorted(reads[client])
            for p in points:
                read_by_then = sum(1 for t in app_reads if t <= p[0])
                if p[3] - read_by_then > 16:
                    entry["queue_full_at"] = p[0]
                    break
        report[group].append(entry)

    t0 = min(p[0] for points in series.values() for p in points)
    for group in ("normal", "slow"):
        entries = report[group]
        if not entries:
            continue
        stalled = sum(e["stalled"] for e in entries)
        total = sum(e["windows"] for e in entries)
        print(f"{group}: {len(entries)} clients, {stalled}/{total} one-second windows look stalled "
              f"(right edge moved <= {args.stall_ratio:.0%} of what was acknowledged); "
              f"window scale {sorted({e['wscale'] for e in entries})}, "
              f"median window step {statistics.median(e['step'] for e in entries):.0f} B, "
              f"{statistics.median(e['per_msg'] for e in entries if e['per_msg'] == e['per_msg']):.0f} B/message")
        if group == "slow":
            for e in sorted(entries, key=lambda e: e["client"]):
                fs = e["first_stall"]
                qf = e.get("queue_full_at")
                print(f"   client {e['client']:>3}: library 16 behind at "
                      f"{(qf - t0) if qf else float('nan'):5.1f}s, window stalled from "
                      f"{(fs - t0) if fs else float('nan'):5.1f}s; after that the right edge moves "
                      f"{e.get('edge_rate', float('nan')):.2f} msg/s, app read {e.get('app_rate', float('nan')):.2f} msg/s")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("out")
    r.add_argument("--port", type=int, default=9263)
    r.add_argument("--clients", type=int, default=100)
    r.add_argument("--duration", type=float, default=40)
    r.add_argument("--slow-percent", type=float, default=10)
    r.add_argument("--slow-ms", type=float, default=500)
    r.add_argument("--seed", type=int, default=3103)
    r.add_argument("--interval", type=float, default=0.05)
    r.add_argument("--server-env", action="append", default=[],
                   help="KEY=VALUE for the server, e.g. LIVERECALL_BACKPRESSURE=0")
    w = sub.add_parser("window")
    w.add_argument("out")
    w.add_argument("--span", type=float, default=1.0)
    w.add_argument("--min-bytes", type=int, default=300)
    w.add_argument("--stall-ratio", type=float, default=0.1)
    g = sub.add_parser("ages")
    g.add_argument("out")
    s = sub.add_parser("summary")
    s.add_argument("dirs", nargs="+")
    args = p.parse_args()
    {"run": run, "window": window, "ages": ages, "summary": summary}[args.cmd](args)


if __name__ == "__main__":
    main()
