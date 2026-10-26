#!/usr/bin/env python3
"""
CS3103 Assignment 4 - LiveRecall server.

Required evaluator command:
    python solution_server.py --port 8765

Data path
---------
    caption_source.py -> handle_source() -> SessionState.accept()
        revision rules plus a bounded window of finalized captions
    SessionState.publish() -> ClientSession.enqueue() for every client
        the source handler never waits for a client
    ClientSession.run()
        one sender task per student connection drains that client's own
        bounded outbox, so a slow client can only delay itself

Delivery modes (per client)
---------------------------
    FULL   normal operation: every accepted revision is forwarded in order.
    HOLD   a lag probe is overdue, so the client or the network is not keeping
           up. Nothing is written; a newer revision replaces the unsent older
           revisions of the same segment. Finals are never dropped.
    PACED  the client application reads slower than the caption stream. Its
           messages are rate-limited to just below its measured reading rate,
           finals first, so few (and only current) messages wait in its
           receive buffer.

Lag probes: after writing data the sender sends a WebSocket ping. The client
answers only once it has read everything sent before the ping. A websockets
client keeps reading until 16 unread messages are queued and then stops
reading from TCP until its application has worked the queue down to 4, so an
overdue pong is the earliest sign of a slow reader. Server-side send buffers
stay empty in that case because the client's kernel keeps accepting data.

Experiment switches (environment variables, inherited by run_demo.py):
    LIVERECALL_BACKPRESSURE=0     no lag probes: always FULL
    LIVERECALL_PACING=0           HOLD/coalescing only, never PACED
    LIVERECALL_CATCHUP_HISTORY=1  send several missed finals to a lagging
                                  client as one HISTORY message
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import struct
from collections import Counter, OrderedDict, deque
from typing import Any

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed, InvalidMessage

try:
    import fcntl
    import termios

    TIOCOUTQ = termios.TIOCOUTQ
except (ImportError, AttributeError):  # not Linux: kernel send queue unknown
    fcntl = None
    TIOCOUTQ = None


def env_flag(name: str, default: bool) -> bool:
    """Read an on/off experiment switch such as LIVERECALL_PACING=0."""
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in ("", "0", "false", "no", "off")


# History: bounded server state.
HISTORY_MAX_SECONDS = 60.0        # longest recovery window served (spec)
HISTORY_RETENTION_SECONDS = 75.0  # kept a little longer than served
HISTORY_MAX_FINALS = 5000         # hard cap for an extremely bursty source
HISTORY_CHUNK_BYTES = 256 * 1024  # split replies well below the 1 MiB frame limit
DEFAULT_HISTORY_SECONDS = 30.0

# Lag detection.
PROBE_INTERVAL = 0.25        # at most one lag probe per 0.25 s, only after data
LAG_MARGIN = 0.25            # a probe is overdue after min RTT + LAG_MARGIN
INITIAL_RTT = 0.25           # RTT assumed before the first sample
RTT_SAMPLES = 32             # min-RTT window
HOLD_SAMPLE_INTERVAL = 0.1   # kernel send-queue check period while held
RECEIVER_CONFIRM = 0.5       # pong still missing this long after the kernel
                             # reported every byte acknowledged: the client
                             # application is not reading
MIN_RECEIVER_STALL = 1.0
ONSET_LOOKBACK = 1.0         # a stall cannot have begun earlier than this
                             # before the overdue probe

# Pacing slow readers. A websockets client stops reading from TCP above 16
# queued messages and resumes once its application has read the queue down
# to 4: during a stall it reads 13 messages, the first of which was already
# being read, so the stall lasts about 12 full read intervals.
PACING_FRACTION = 0.85       # steady state: 85% of the measured reading rate
DRAIN_FRAMES = 12
RESUME_QUEUE = 4             # messages still queued at the client when it resumes
MIN_PACE_RATE = 0.2          # messages per second
PACE_BURST = 2.0             # token bucket depth; a provisional needs a spare
                             # token so the next final is not delayed by it
PACED_RETRY_SECONDS = 60.0   # stall-free this long: try full rate again
PACED_RETRY_MAX = 600.0

# Outbox bounds.
MAX_PENDING = 512                      # entries before forced coalescing
MAX_PENDING_AGE = HISTORY_MAX_SECONDS  # oldest unsent caption older: disconnect

STATS_INTERVAL = 10.0

BACKPRESSURE = env_flag("LIVERECALL_BACKPRESSURE", True)
PACING = env_flag("LIVERECALL_PACING", True)
CATCHUP_HISTORY = env_flag("LIVERECALL_CATCHUP_HISTORY", False)
CATCHUP_MIN_FINALS = 2
CATCHUP_MAX_FINALS = 200

FULL, HOLD, PACED = "full", "hold", "paced"

DEFAULT_SESSION = "L1"
SESSIONS: dict[str, SessionState] = {}
BACKGROUND_TASKS: set[asyncio.Task] = set()


def validate_caption_update(msg: Any) -> bool:
    """Return whether a source message has the fields needed for revision state."""
    if not isinstance(msg, dict):
        return False

    session = msg.get("session")
    segment = msg.get("segment")
    revision = msg.get("revision")
    source_ts = msg.get("source_ts")
    if isinstance(source_ts, bool) or not isinstance(source_ts, (int, float)):
        return False
    try:
        finite_timestamp = math.isfinite(float(source_ts))
    except (OverflowError, ValueError):
        return False

    return (
        isinstance(session, str)
        and bool(session.strip())
        and type(segment) is int
        and segment > 0
        and type(revision) is int
        and revision > 0
        and finite_timestamp
        and isinstance(msg.get("text"), str)
        and type(msg.get("final")) is bool
    )


class Caption:
    """One accepted revision, serialized once and shared by every client."""

    __slots__ = ("segment", "revision", "final", "received", "text")

    def __init__(self, segment: int, revision: int, final: bool,
                 received: float, text: str) -> None:
        self.segment = segment
        self.revision = revision
        self.final = final
        self.received = received  # server monotonic time
        self.text = text          # CAPTION_UPDATE JSON forwarded to clients


class SessionState:
    """Caption state of one lecture session.

    Revision rules: a revision is accepted only if it is newer than the stored
    revision of its segment and the segment is not final yet, so an older
    revision can never overwrite a newer one. Finals are kept in arrival
    order for HISTORY and expire after HISTORY_RETENTION_SECONDS; segments at
    or below `floor` have expired and are rejected rather than re-accepted.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self.clients: set[ClientSession] = set()
        self.sources = 0
        self.reset()

    def reset(self) -> None:
        self.latest: dict[int, Caption] = {}  # segment -> newest accepted revision
        self.active: dict[int, Caption] = {}  # segments without a final yet
        self.finals: deque[Caption] = deque()
        self.floor = 0
        self.max_segment = 0

    def accept(self, msg: dict[str, Any], text: str, now: float) -> Caption | None:
        """Store a validated update; return it if accepted, None if stale."""
        segment = msg["segment"]
        revision = msg["revision"]
        if segment <= self.floor:
            return None
        current = self.latest.get(segment)
        if current is not None and (current.final or revision <= current.revision):
            return None

        caption = Caption(segment, revision, msg["final"], now, text)
        self.latest[segment] = caption
        if caption.final:
            self.active.pop(segment, None)
            self.finals.append(caption)
            self.expire_finals(now)
        else:
            self.active[segment] = caption
        self.max_segment = max(self.max_segment, segment)
        return caption

    def restart_if_new_lecture(self, segment: int) -> None:
        """Called with the first update of each source connection: segment
        numbers starting from 1 again mean a new lecture or test run. A stale
        segment 1 later in the same connection is simply rejected."""
        if segment == 1 and self.max_segment > 1:
            print(f"session {self.name}: new source restarted at segment 1; "
                  "clearing previous lecture state", flush=True)
            self.reset()

    def publish(self, caption: Caption) -> None:
        for client in self.clients:
            client.enqueue(caption)

    def expire_finals(self, now: float) -> None:
        cutoff = now - HISTORY_RETENTION_SECONDS
        finals = self.finals
        while finals and (finals[0].received < cutoff
                          or len(finals) > HISTORY_MAX_FINALS):
            old = finals.popleft()
            if self.latest.get(old.segment) is old:
                del self.latest[old.segment]
            self.floor = max(self.floor, old.segment)

    def prune(self, now: float) -> None:
        """Periodic housekeeping: expire finals and abandoned provisional segments."""
        self.expire_finals(now)
        cutoff = now - HISTORY_RETENTION_SECONDS
        for segment in [s for s, c in self.active.items() if c.received < cutoff]:
            del self.active[segment]
            self.latest.pop(segment, None)
            self.floor = max(self.floor, segment)

    def finals_after(self, last_segment: int, last_revision: int,
                     window: float, now: float) -> list[Caption]:
        """Finals the client does not hold yet, received within `window`, oldest first."""
        cutoff = now - window
        items = []
        for caption in reversed(self.finals):
            if caption.received < cutoff:
                break
            if (caption.segment > last_segment
                    or (caption.segment == last_segment and caption.revision > last_revision)):
                items.append(caption)
        items.reverse()
        return items

    def provisional_after(self, segment: int) -> list[Caption]:
        """Current provisional state of active segments above `segment`."""
        return sorted((c for c in self.active.values() if c.segment > segment),
                      key=lambda c: c.segment)


class ClientSession:
    """One student connection with its own bounded outbox and sender task."""

    def __init__(self, ws, client_id: Any, session: SessionState) -> None:
        self.ws = ws
        self.client_id = client_id
        self.session = session
        self.loop = asyncio.get_running_loop()
        self.wake = asyncio.Event()
        self.closing = False

        # Outbox: segment -> unsent revisions of that segment, oldest first.
        # Segments leave in insertion order, i.e. segment order.
        self.pending: OrderedDict[int, deque[Caption]] = OrderedDict()
        self.n_pending = 0
        self.preamble: deque[str] = deque()  # HISTORY reply, sent before anything else
        self.catching_up = False             # backlog left over from a HOLD

        self.mode = FULL
        self.mode_before_hold = FULL
        self.hold_onset = 0.0
        self.acked_since: float | None = None

        self.probe_sent_at: float | None = None
        self.last_probe_at = -math.inf
        self.rtts: deque[float] = deque(maxlen=RTT_SAMPLES)
        self.rtt_min = INITIAL_RTT
        self.frames_sent = 0
        self.frames_at_probe = 0
        self.last_write_at = 0.0
        self.probe_last_write = 0.0
        # Latest message the client confirmed reading (its pong came back in
        # time). It may itself have filled the client's queue: the ping that
        # followed it in the same TCP segment is still answered.
        self.confirmed_write_at: float | None = None

        self.reading_rate = 0.0
        self.tokens = 0.0
        self.tokens_at = 0.0
        self.paced_since = 0.0
        self.retry_after = PACED_RETRY_SECONDS
        self.retried = False

    # Outbox ---------------------------------------------------------------

    def enqueue(self, caption: Caption) -> None:
        """Add an accepted revision; coalesce unless the client is keeping up."""
        if self.closing:
            return
        queue = self.pending.get(caption.segment)
        if queue is None:
            self.pending[caption.segment] = deque((caption,))
            self.n_pending += 1
        elif self.mode == FULL:
            queue.append(caption)
            self.n_pending += 1
        else:
            # Lagging: the newer revision makes the unsent ones obsolete. A
            # final is the newest revision of its segment, so it is never lost.
            self.n_pending += 1 - len(queue)
            queue.clear()
            queue.append(caption)

        if self.n_pending > MAX_PENDING:
            self.compact()
        oldest = next(iter(self.pending.values()))[0]
        if (self.n_pending > MAX_PENDING
                or self.loop.time() - oldest.received > MAX_PENDING_AGE):
            self.overflow()
            return
        self.wake.set()

    def compact(self) -> None:
        """Keep only the newest unsent revision of every segment."""
        for queue in self.pending.values():
            if len(queue) > 1:
                newest = queue[-1]
                queue.clear()
                queue.append(newest)
        self.n_pending = len(self.pending)

    def pop(self) -> Caption:
        segment, queue = next(iter(self.pending.items()))
        caption = queue.popleft()
        self.n_pending -= 1
        if not queue:
            del self.pending[segment]
        return caption

    def leading_finals(self) -> list[Caption]:
        """Unsent finals at the front of the outbox (candidates for one HISTORY)."""
        batch = []
        for queue in self.pending.values():
            if not queue[0].final:
                break
            batch.append(queue[0])
            if len(batch) >= CATCHUP_MAX_FINALS:
                break
        return batch

    def overflow(self) -> None:
        """Last resort for a client hopelessly behind: keep state bounded and
        let the client resume through HISTORY."""
        self.closing = True
        self.pending.clear()
        self.n_pending = 0
        self.wake.set()
        print(f"client {self.client_id}: more than {MAX_PENDING_AGE:.0f}s behind; "
              "closing so it can resume through HISTORY", flush=True)
        spawn(self.ws.close(code=1013, reason="client too far behind"))

    # Sender ---------------------------------------------------------------

    async def run(self) -> None:
        """Sender task: drain the outbox according to the delivery mode."""
        try:
            while self.preamble:
                await self.write(self.preamble.popleft())
            while not self.closing:
                self.wake.clear()
                delay = self.poll(self.loop.time())
                if delay == 0.0:
                    await self.send_next()
                    continue
                try:
                    await asyncio.wait_for(self.wake.wait(), delay)
                except asyncio.TimeoutError:
                    pass
        except ConnectionClosed:
            pass

    def poll(self, now: float) -> float | None:
        """Update the delivery mode. Return 0.0 if the next message may be sent
        now, otherwise how long to wait (None: until woken)."""
        deadline = None
        if self.probe_sent_at is not None and self.mode != HOLD:
            overdue_at = self.probe_sent_at + self.rtt_min + LAG_MARGIN
            if now >= overdue_at:
                self.enter_hold()
            else:
                deadline = overdue_at

        if self.mode == HOLD:
            if self.acked_since is None:
                outstanding = unacked_bytes(self.ws)
                if outstanding is not None and outstanding > 0:
                    return HOLD_SAMPLE_INTERVAL
                # Everything we wrote has reached the client's kernel (or the
                # platform cannot tell), yet the pong is missing.
                self.acked_since = now
            return None  # on_pong() wakes the sender

        if self.mode == PACED:
            retry_at = self.paced_since + self.retry_after
            if now >= retry_at:
                self.leave_paced()
            else:
                deadline = retry_at if deadline is None else min(deadline, retry_at)

        if self.n_pending and self.mode == PACED:
            rate = self.pace_rate()
            self.tokens = min(PACE_BURST, self.tokens + (now - self.tokens_at) * rate)
            self.tokens_at = now
            need = 1.0 if next(iter(self.pending.values()))[0].final else PACE_BURST
            if self.tokens >= need:
                return 0.0
            ready_at = now + (need - self.tokens) / rate
            deadline = ready_at if deadline is None else min(deadline, ready_at)
        elif self.n_pending:
            return 0.0
        return None if deadline is None else max(deadline - now, 0.001)

    async def send_next(self) -> None:
        batch = []
        if CATCHUP_HISTORY and (self.mode != FULL or self.catching_up):
            batch = self.leading_finals()
        if len(batch) >= CATCHUP_MIN_FINALS:
            for _ in batch:
                self.pop()
            text = history_text([caption.text for caption in batch])
        else:
            text = self.pop().text
        if self.mode == PACED:
            self.tokens -= 1.0
        if not self.pending:
            self.catching_up = False

        await self.write(text)
        now = self.loop.time()
        if (BACKPRESSURE and self.probe_sent_at is None
                and now - self.last_probe_at >= PROBE_INTERVAL):
            await self.probe(now)

    async def write(self, text: str) -> None:
        await self.ws.send(text)
        self.frames_sent += 1
        self.last_write_at = self.loop.time()

    def pace_rate(self) -> float:
        """Messages per second for a PACED client: its full reading rate while
        unsent finals are piling up, a safety margin below it otherwise."""
        leading_finals = 0
        for queue in self.pending.values():
            if not queue[0].final:
                break
            leading_finals += 1
            if leading_finals >= 2:
                return self.reading_rate
        return PACING_FRACTION * self.reading_rate

    # Lag probes and mode changes ------------------------------------------

    async def probe(self, now: float) -> None:
        self.probe_sent_at = self.last_probe_at = now
        self.frames_at_probe = self.frames_sent
        self.probe_last_write = self.last_write_at
        waiter = await self.ws.ping()
        waiter.add_done_callback(lambda done, sent_at=now: self.on_pong(done, sent_at))

    def on_pong(self, waiter: asyncio.Future, sent_at: float) -> None:
        failed = waiter.cancelled() or waiter.exception() is not None
        if self.probe_sent_at != sent_at:
            return
        self.probe_sent_at = None
        if failed:
            return
        now = self.loop.time()
        if self.mode == HOLD:
            self.leave_hold(now, now - sent_at)
        else:
            self.add_rtt(now - sent_at)
        self.confirmed_write_at = self.probe_last_write
        self.wake.set()

    def add_rtt(self, rtt: float) -> None:
        self.rtts.append(rtt)
        self.rtt_min = min(self.rtts)

    def enter_hold(self) -> None:
        self.mode_before_hold = self.mode
        self.mode = HOLD
        # The client stopped reading between the last message it confirmed and
        # the overdue probe; the earlier bound gives a conservative (lower)
        # reading-rate estimate.
        if self.confirmed_write_at is not None:
            self.hold_onset = max(self.confirmed_write_at, self.probe_sent_at - ONSET_LOOKBACK)
        else:
            self.hold_onset = self.probe_sent_at
        self.acked_since = None
        self.compact()

    def leave_hold(self, now: float, probe_rtt: float) -> None:
        stall = now - self.hold_onset
        previous = self.mode_before_hold
        self.catching_up = self.n_pending > 0
        receiver_limited = (
            stall >= MIN_RECEIVER_STALL
            and self.acked_since is not None
            and now - self.acked_since >= RECEIVER_CONFIRM
        )
        if not receiver_limited or not PACING:
            # Network queueing, loss recovery or a short hiccup: resume the
            # previous mode with the coalesced (newest) state.
            if not receiver_limited:
                self.add_rtt(probe_rtt)
            self.mode = previous
            self.tokens_at = now
            return

        measured = DRAIN_FRAMES / stall
        if previous == PACED:
            measured = min(measured, 0.7 * self.reading_rate)  # stalled while paced
        elif self.retried:
            self.retry_after = min(2 * self.retry_after, PACED_RETRY_MAX)
        self.reading_rate = max(MIN_PACE_RATE, measured)
        self.mode = PACED
        # Token debt: let the client read the messages still queued on its
        # side (RESUME_QUEUE plus anything written after the overdue probe)
        # before sending more, so what it reads next is current.
        still_queued = RESUME_QUEUE + (self.frames_sent - self.frames_at_probe)
        self.tokens = 1.0 - still_queued
        self.tokens_at = now
        self.paced_since = now
        print(f"client {self.client_id}: slow reader (stalled {stall:.1f}s, reads "
              f"~{self.reading_rate:.2f} msg/s); pacing", flush=True)

    def leave_paced(self) -> None:
        """Stall-free for a while: try full-rate delivery again."""
        self.mode = FULL
        self.retried = True
        print(f"client {self.client_id}: no stall for {self.retry_after:.0f}s; "
              "trying full rate again", flush=True)


def history_text(items: list[str]) -> str:
    """HISTORY message built from already-serialized CAPTION_UPDATE objects."""
    return '{"type": "HISTORY", "items": [' + ", ".join(items) + "]}"


def history_frames(captions: list[Caption]) -> list[str]:
    """One HISTORY reply, split into several messages only if very large."""
    frames: list[str] = []
    chunk: list[str] = []
    size = 0
    for caption in captions:
        if chunk and size + len(caption.text) > HISTORY_CHUNK_BYTES:
            frames.append(history_text(chunk))
            chunk, size = [], 0
        chunk.append(caption.text)
        size += len(caption.text) + 2
    frames.append(history_text(chunk))
    return frames


def unacked_bytes(ws) -> int | None:
    """Bytes written but not yet acknowledged by the client's TCP (Linux)."""
    transport = ws.transport
    sock = transport.get_extra_info("socket") if transport is not None else None
    if TIOCOUTQ is None or sock is None:
        return None
    try:
        kernel = struct.unpack("i", fcntl.ioctl(sock.fileno(), TIOCOUTQ, b"\0\0\0\0"))[0]
    except (OSError, ValueError):
        return None
    return kernel + transport.get_write_buffer_size()


def spawn(coro) -> None:
    task = asyncio.ensure_future(coro)
    BACKGROUND_TASKS.add(task)
    task.add_done_callback(BACKGROUND_TASKS.discard)


def get_session(name: str) -> SessionState:
    session = SESSIONS.get(name)
    if session is None:
        session = SESSIONS[name] = SessionState(name)
    return session


def session_name(value: Any) -> str:
    return value if isinstance(value, str) and value.strip() else DEFAULT_SESSION


def cursor_value(value: Any) -> int:
    """last_segment/last_revision as a non-negative int (0 if missing/invalid)."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0
    if not math.isfinite(value) or value < 0:
        return 0
    return int(value)


def history_window(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return DEFAULT_HISTORY_SECONDS
    return min(max(float(value), 0.0), HISTORY_MAX_SECONDS)


async def handle_source(ws, hello: dict[str, Any]) -> None:
    """
    Read messages from a caption source and publish accepted caption updates.

    Invalid JSON, invalid updates, stale revisions and unsupported message
    types are ignored. SOURCE_DONE is logged but not forwarded.
    """
    hello_session = session_name(hello.get("session"))
    get_session(hello_session).sources += 1
    loop = asyncio.get_running_loop()
    first_update = True
    print(f"source connected: session={hello_session}", flush=True)

    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if not isinstance(msg, dict):
                continue

            msg_type = msg.get("type")
            if msg_type == "CAPTION_UPDATE":
                # Forward the source's JSON text unchanged when possible, so
                # extra fields such as test-phase labels reach clients too.
                if "session" not in msg:
                    msg["session"] = hello_session
                    text = json.dumps(msg)
                elif isinstance(raw, str):
                    text = raw
                else:
                    text = json.dumps(msg)
                if not validate_caption_update(msg):
                    continue
                session = get_session(msg["session"])
                if first_update:
                    first_update = False
                    session.restart_if_new_lecture(msg["segment"])
                caption = session.accept(msg, text, loop.time())
                if caption is not None:
                    session.publish(caption)
            elif msg_type == "SOURCE_DONE":
                print("source finished", flush=True)
    except ConnectionClosed:
        pass
    finally:
        get_session(hello_session).sources -= 1
        print("source disconnected", flush=True)


async def handle_client(ws, hello: dict[str, Any]) -> None:
    """
    Reply to CLIENT_HELLO with HISTORY, then stream live updates until disconnect.

    New clients (last_segment=0) receive the finals of the last
    `history_seconds`. Resuming clients receive every final after their
    last_segment/last_revision cursor within the 60 s window, so recovery is
    incremental and never repeats a final the client already holds.
    """
    session = get_session(session_name(hello.get("session")))
    client_id = hello.get("client_id")
    last_segment = cursor_value(hello.get("last_segment"))
    last_revision = cursor_value(hello.get("last_revision"))
    window = (HISTORY_MAX_SECONDS if last_segment > 0
              else history_window(hello.get("history_seconds")))

    client = ClientSession(ws, client_id, session)
    # Snapshot and subscribe with no await in between: every accepted caption
    # reaches this client exactly once, either in HISTORY or live.
    missed = session.finals_after(last_segment, last_revision, window, client.loop.time())
    client.preamble.extend(history_frames(missed))
    # The client may already hold a provisional of segment last_segment+1, but
    # nothing newer, so later active segments can be shown immediately.
    for caption in session.provisional_after(last_segment + 1):
        client.enqueue(caption)
    session.clients.add(client)
    sender = asyncio.create_task(client.run())
    print(f"client connected: id={client_id}; clients={len(session.clients)}; "
          f"history={len(missed)}", flush=True)

    try:
        # Student clients normally don't send further messages after hello.
        async for _ in ws:
            pass
    except ConnectionClosed:
        pass
    finally:
        session.clients.discard(client)
        client.closing = True
        sender.cancel()
        try:
            await sender
        except (asyncio.CancelledError, ConnectionClosed):
            pass
        print(f"client disconnected: id={client_id}; clients={len(session.clients)}",
              flush=True)


async def handler(ws) -> None:
    """
    Read and validate the FIRST WebSocket message, then route by hello type.

    Connections that do not send valid JSON within five seconds, or send an
    unsupported hello type, are closed with policy-violation status.
    """
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=5)
        hello = json.loads(raw)
    except (asyncio.TimeoutError, json.JSONDecodeError, UnicodeDecodeError):
        await ws.close(code=1008, reason="Expected a valid hello message")
        return
    except ConnectionClosed:
        return

    msg_type = hello.get("type") if isinstance(hello, dict) else None
    if msg_type == "SOURCE_HELLO":
        await handle_source(ws, hello)
    elif msg_type == "CLIENT_HELLO":
        await handle_client(ws, hello)
    else:
        await ws.close(code=1008, reason="Expected SOURCE_HELLO or CLIENT_HELLO")


def report_stats() -> None:
    clients = [client for session in SESSIONS.values() for client in session.clients]
    if not clients:
        return
    modes = Counter(client.mode for client in clients)
    queued = sum(client.n_pending for client in clients)
    history = sum(len(session.finals) for session in SESSIONS.values())
    print(f"stats: clients={len(clients)} full={modes[FULL]} hold={modes[HOLD]} "
          f"paced={modes[PACED]} queued={queued} history_finals={history}", flush=True)


async def housekeeping() -> None:
    """Expire old history even when no captions arrive; print periodic stats."""
    loop = asyncio.get_running_loop()
    next_report = loop.time() + STATS_INTERVAL
    while True:
        await asyncio.sleep(1.0)
        now = loop.time()
        for name, session in list(SESSIONS.items()):
            session.prune(now)
            if not session.clients and not session.sources and not session.latest:
                del SESSIONS[name]
        if now >= next_report:
            next_report = now + STATS_INTERVAL
            report_stats()


class PortCheckFilter(logging.Filter):
    """Hide the handshake error logged when a TCP port check (as done by
    run_demo.py) connects and closes without sending a WebSocket request."""

    def filter(self, record: logging.LogRecord) -> bool:
        exc = record.exc_info[1] if record.exc_info else None
        return not (isinstance(exc, InvalidMessage) and isinstance(exc.__cause__, EOFError))


def raise_fd_limit() -> None:
    """Allow more than ~1000 simultaneous connections where the OS permits."""
    try:
        import resource

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        target = 65536 if hard == resource.RLIM_INFINITY else min(hard, 65536)
        if soft < target:
            resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
    except (ImportError, ValueError, OSError):
        pass


async def main_async(port: int) -> None:
    """Start the WebSocket server on all interfaces and keep it running."""
    raise_fd_limit()
    logging.getLogger("websockets.server").addFilter(PortCheckFilter())
    async with serve(
        handler,
        "0.0.0.0",
        port,
        max_size=2**20,
        ping_interval=20,
        ping_timeout=60,
    ):
        spawn(housekeeping())
        print(f"LiveRecall server listening on ws://0.0.0.0:{port} "
              f"(backpressure={BACKPRESSURE}, pacing={PACING}, "
              f"catchup_history={CATCHUP_HISTORY})", flush=True)
        await asyncio.Future()


def main() -> None:
    """Parse the optional listening port and run the asynchronous server."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    try:
        asyncio.run(main_async(args.port))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
