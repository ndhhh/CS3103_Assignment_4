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

Delivery: where a caption can wait
-----------------------------------
On its way to a student, a caption passes three places:

    outbox  ->  socket and network  ->  client kernel and library  ->  app
    (ours)      (TCP has it)            (delivered, not yet read)

Only in the outbox can a waiting revision still be replaced by a newer one
of the same segment. So the server keeps a backlog in the outbox and releases
messages only as fast as the later stages drain them. Finals are never
dropped: a final is the newest revision of its segment. When nothing is
backlogged every revision goes out at once, so clients that keep up get the
full revision stream; revisions are replaced only while the client is being
held back.

The two later stages report back in different ways, so each has its own
control loop.

Loop 1, the network (ACKs). TCP reports which bytes the client's kernel has
acknowledged. How long our oldest unacknowledged message has been waiting is
how far the path is behind: a few ms on loopback, about one round trip on a
long path, much more on an overloaded link or while a lost packet is being
repaired (nothing behind the gap can be delivered meanwhile). While it is
above NETWORK_LAG_LIMIT (0.4 s, just under the 500 ms freshness threshold),
provisional revisions stay in the outbox; finals are always written.

Loop 2, the application (pongs). A websockets client reads ahead into a
16-message queue whatever its application is doing, stops reading the socket
above 16 and resumes at 4. Its kernel keeps acknowledging data, so loop 1
cannot see a slow reader, and its receive window stays open (128 KB buffers
for ~42-byte compressed messages). The one application-level signal is the
pong: the library answers a ping only when it reads it. So every 4th message
carries a ping. When the client's TCP has acknowledged a ping but no pong has
come back within PONG_GRACE, the library has stopped reading: the client is
held (nothing is written). When the pong arrives, the stall length gives the
reading rate (the library resumes after the application has read 12
messages); a client that reads slower than we were sending is then paced
just below that rate, finals first, starting once the messages still queued
on its side have been read. The ping spacing is how precisely we know where
the library stopped: what we wrote after that point is backlog the reader
must still get through, and the queued count must assume the worst case
within one spacing. Every 4th message keeps that within the 4 messages the
library itself leaves queued, at one extra packet (our ACK of the pong) per
4 messages.

When the source has finished (SOURCE_DONE, or its connection closed),
nothing newer is coming, so pacing has nothing left to keep current: paced
clients get what is left at once.

A pong can also be lost on the way back; TCP then repairs it only on a
growing timeout, because the client has nothing else to send. A held client
is therefore pinged again every CONFIRM_INTERVAL: a client that reads
answers, its answers arrive behind the lost one (the kernel counts the
segment but cannot deliver it yet), and the hold ends at once. Those answers
also make the client's TCP resend the lost one immediately.

Delivery modes (per client)
---------------------------
    FULL   normal operation, subject to loop 1.
    HOLD   the library stopped reading (loop 2): nothing is written; newer
           revisions replace unsent older ones.
    PACED  a slow reader: rate-limited to its measured reading rate, finals
           first, still subject to loop 1.

Catch-up: when a held-back client has two or more unsent finals, they leave
together as one HISTORY message (finals only, oldest first, none the client
already holds), so a slow reader spends one read on them instead of one per
final. Only clients that joined with nothing to recover get this; a resumed
or late-joining connection keeps exactly one HISTORY, its recovery reply.

Experiment switches (environment variables, inherited by run_demo.py):
    LIVERECALL_BACKPRESSURE=0     no pings: loop 2 off, always FULL
    LIVERECALL_PACING=0           holds, but never PACED
    LIVERECALL_NETWORK_GATE=0     loop 1 off
    LIVERECALL_CATCHUP_HISTORY=0  never batch finals into a catch-up HISTORY
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import socket
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

TCP_INFO = getattr(socket, "TCP_INFO", None)  # Linux only
TCP_CORK = getattr(socket, "TCP_CORK", None)  # Linux only
# struct tcp_info offsets: u64 fields since Linux 4.6, snd_wnd since 5.4
TCPI_BYTES_ACKED = 120
TCPI_BYTES_RECEIVED = 128
TCPI_DATA_SEGS_IN = 152
TCPI_SND_WND = 228
# Diagnostics only (stuck-client reports): name -> (offset, format)
TCPI_REPORT = {
    "retransmits": (2, "<B"), "backoff": (4, "<B"), "rto_ms": (8, "<I"),
    "unacked": (24, "<I"), "lost": (32, "<I"), "rx_data_ms": (52, "<I"),
    "rx_ack_ms": (56, "<I"), "rtt_ms": (68, "<I"), "cwnd": (80, "<I"),
    "total_retrans": (100, "<I"),
}


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

# Loop 1, network: how far the path to a client may fall behind before
# provisional revisions stay in the outbox. Just under the 500 ms freshness
# threshold; on unloaded paths (loopback, 100 ms delay, jitter, reordering)
# the oldest unacknowledged message was never this old (experiments/
# run_trace_suite.sh).
NETWORK_LAG_LIMIT = 0.4
LINK_TICK = 0.05             # asyncio has no event for an incoming ACK: clients
                             # waiting on loop 1 are checked this often
UNACKED_WRITES = 1024        # write positions remembered per client

# Loop 2, application. The websockets client library reads ahead until
# LIBRARY_QUEUE messages are waiting for its application, then stops reading
# the socket until the application has worked the queue down to
# LIBRARY_RESUME. During a stall it reads LIBRARY_QUEUE + 1 - LIBRARY_RESUME
# messages, the first already being read: the stall lasts DRAIN_FRAMES reads.
LIBRARY_QUEUE = 16
LIBRARY_RESUME = 4
DRAIN_FRAMES = LIBRARY_QUEUE - LIBRARY_RESUME
PROBE_EVERY = LIBRARY_RESUME  # a ping on every 4th message: where the library
                              # stopped is known to within 4 messages
PONG_GRACE = 0.25            # pong still missing this long after the client's TCP
                             # acknowledged the ping: the library is not reading
                             # (allows for a busy client process)
PROBE_CHECK = 0.1            # how often an unanswered ping is re-examined
CONFIRM_INTERVAL = 0.5       # while held, ping again this often (lost pongs)
SEND_RATE_WINDOW = 10.0      # seconds of writes used for the sending rate
LOG_STALL = 1.0              # stalls shorter than this are not logged

# Pacing slow readers.
PACING_FRACTION = 0.85       # steady state: 85% of the estimated reading rate,
                             # which is at most ~14% high (see enter_hold)
MIN_PACE_RATE = 0.2          # messages per second
PACE_BURST = 2.0             # token bucket depth; a provisional needs a spare
                             # token so the next final is not delayed by it
PACED_RETRY_SECONDS = 60.0   # stall-free this long: try full rate again
PACED_RETRY_MAX = 600.0

# Outbox bounds.
MAX_PENDING = 512                      # entries before forced coalescing
MAX_PENDING_AGE = HISTORY_MAX_SECONDS  # oldest unsent caption older: disconnect

STATS_INTERVAL = 10.0
REPORT_HELD_AFTER = 2.0      # stats also describe clients held longer than this
REPORT_HELD_MAX = 10

BACKPRESSURE = env_flag("LIVERECALL_BACKPRESSURE", True)
PACING = env_flag("LIVERECALL_PACING", True)
NETWORK_GATE = env_flag("LIVERECALL_NETWORK_GATE", True)
CATCHUP_HISTORY = env_flag("LIVERECALL_CATCHUP_HISTORY", True)
CATCHUP_MIN_FINALS = 2
CATCHUP_MAX_FINALS = 200

FULL, HOLD, PACED = "full", "hold", "paced"

DEFAULT_SESSION = "L1"
SESSIONS: dict[str, SessionState] = {}
WAITING: set[ClientSession] = set()  # clients waiting on loop 1 for an ACK
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
        self.ended = False  # the source finished: nothing newer is coming
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
        self.ended = False
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

    def end_stream(self) -> None:
        """The source finished: paced clients may take what is left."""
        self.ended = True
        for client in self.clients:
            client.wake.set()

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


class Outbox:
    """A client's unsent captions: segment -> unsent revisions, oldest first.
    Segments leave in insertion order, i.e. segment order."""

    def __init__(self) -> None:
        self.pending: OrderedDict[int, deque[Caption]] = OrderedDict()
        self.count = 0

    def __bool__(self) -> bool:
        return self.count > 0

    def add(self, caption: Caption, replace: bool) -> None:
        """Queue a revision. With `replace` it supersedes the unsent older
        revisions of its segment; a final is always the newest revision."""
        queue = self.pending.get(caption.segment)
        if queue is None:
            self.pending[caption.segment] = deque((caption,))
            self.count += 1
        elif replace:
            self.count += 1 - len(queue)
            queue.clear()
            queue.append(caption)
        else:
            queue.append(caption)
            self.count += 1

    def compact(self) -> None:
        """Keep only the newest unsent revision of every segment."""
        for queue in self.pending.values():
            if len(queue) > 1:
                newest = queue[-1]
                queue.clear()
                queue.append(newest)
        self.count = len(self.pending)

    def head(self) -> Caption:
        return next(iter(self.pending.values()))[0]

    def pop(self) -> Caption:
        segment, queue = next(iter(self.pending.items()))
        caption = queue.popleft()
        self.count -= 1
        if not queue:
            del self.pending[segment]
        return caption

    def leading_finals(self, limit: int) -> list[Caption]:
        """Unsent finals at the front (candidates for one HISTORY)."""
        batch = []
        for queue in self.pending.values():
            if not queue[0].final:
                break
            batch.append(queue[0])
            if len(batch) >= limit:
                break
        return batch

    def clear(self) -> None:
        self.pending.clear()
        self.count = 0


class NetworkGate:
    """Loop 1: how far the network path to one client is behind, from the
    bytes its TCP has acknowledged."""

    def __init__(self, ws) -> None:
        self.ws = ws
        # (stream position just past a written message, write time), oldest
        # first; entries leave once the client's TCP acknowledged them.
        self.writes: deque[tuple[int, float]] = deque(maxlen=UNACKED_WRITES)
        self.holding = False  # provisionals are being kept in the outbox

    def wrote(self, now: float) -> None:
        end = stream_position(self.ws)
        if end is not None:
            self.writes.append((end, now))

    def lag(self, now: float) -> float:
        """How long the oldest unacknowledged message has been waiting."""
        writes = self.writes
        if not writes:
            return 0.0
        if now - writes[0][1] <= NETWORK_LAG_LIMIT:
            return now - writes[0][1]  # an upper bound, and already under the limit
        acked = tcp_info_field(tcp_info(self.ws), TCPI_BYTES_ACKED, "<Q")
        if acked is None:
            return 0.0  # the platform cannot tell: loop 1 never holds
        while writes and writes[0][0] <= acked:
            writes.popleft()
        return now - writes[0][1] if writes else 0.0

    def behind(self, now: float) -> bool:
        return NETWORK_GATE and self.lag(now) > NETWORK_LAG_LIMIT


class ReaderMonitor:
    """Loop 2's observations: pings on every PROBE_EVERY-th message, and
    whether the client library answers them."""

    def __init__(self, ws) -> None:
        self.ws = ws
        self.sent_at: float | None = None      # outstanding ping
        self.end: int | None = None            # stream position just past it
        self.delivered_at: float | None = None
        self.frames_at_probe = 0
        self.write_at_probe = 0.0
        # The last ping answered: the library had read every message written
        # before it.
        self.confirmed_frames = 0
        self.confirmed_write_at: float | None = None
        # While held: confirmation pings and the client's incoming data.
        self.confirm_needed = False
        self.last_confirm_at = -math.inf
        self.confirms = 0
        self.hold_counters: tuple[int, int] | None = None
        self.client_reading = False
        # In-order bytes received from the client when its replies were found
        # stuck behind a lost segment: no stall can be seen until more arrive.
        self.reply_gap: int | None = None

    def due(self, frames_sent: int) -> bool:
        """Should the message about to be written carry a ping?"""
        return (BACKPRESSURE and self.sent_at is None
                and frames_sent + 1 - self.frames_at_probe >= PROBE_EVERY)

    async def probe(self, now: float, frames_sent: int, write_at: float, on_pong) -> None:
        self.sent_at = now
        self.delivered_at = None
        self.frames_at_probe = frames_sent
        self.write_at_probe = write_at
        waiter = await self.ws.ping()
        self.end = stream_position(self.ws)
        waiter.add_done_callback(lambda done, sent_at=now: on_pong(done, sent_at))

    def delivered(self) -> bool:
        """Has the client's TCP received the outstanding ping (or closed its
        receive window)? True when the platform cannot tell."""
        if self.reply_gap is not None:
            counters = tcp_receive_counters(self.ws)
            if counters is not None and counters[0] == self.reply_gap:
                return False  # its answer would be stuck behind the lost reply
            self.reply_gap = None
        info = tcp_info(self.ws)
        acked = tcp_info_field(info, TCPI_BYTES_ACKED, "<Q")
        if self.end is None or acked is None:
            return True
        return acked >= self.end or tcp_info_field(info, TCPI_SND_WND, "<I") == 0

    def check(self, now: float) -> tuple[bool, float | None]:
        """(library stopped reading?, seconds until the next look)."""
        if self.sent_at is None:
            return False, None
        if self.delivered_at is None:
            if not self.delivered():
                return False, PROBE_CHECK  # still on its way: network delay, not a stall
            self.delivered_at = now
        stall_at = self.delivered_at + PONG_GRACE
        if now >= stall_at:
            return True, None
        return False, stall_at - now

    def confirm(self) -> None:
        """The outstanding ping was answered."""
        self.sent_at = None
        self.confirmed_frames = self.frames_at_probe
        self.confirmed_write_at = self.write_at_probe

    def hold_started(self) -> None:
        self.hold_counters = tcp_receive_counters(self.ws)
        self.client_reading = False
        self.confirms = 0
        self.confirm_needed = BACKPRESSURE  # the first confirmation ping

    def watch(self, now: float) -> None:
        """While held: a client that is not reading cannot answer pings, so it
        sends nothing. A data segment the kernel cannot deliver yet
        (data_segs_in rises, in-order bytes do not) is an answer stuck
        behind a lost one: the client is reading."""
        if self.client_reading or self.hold_counters is None:
            return
        current = tcp_receive_counters(self.ws)
        if current is None:
            return
        delivered, segments = current
        base_delivered, base_segments = self.hold_counters
        if delivered != base_delivered:
            self.hold_counters = current  # in-order data, e.g. a client keepalive ping
        elif segments > base_segments:
            self.client_reading = True

    def release(self) -> None:
        """Hold ended because the client is reading: forget the ping whose
        answer was lost, and see no stall until the lost data arrives."""
        counters = tcp_receive_counters(self.ws)
        self.reply_gap = counters[0] if counters is not None else None
        self.sent_at = None


class Pacer:
    """Rate limit for a slow reader: a token bucket at its reading rate."""

    def __init__(self) -> None:
        self.rate = 0.0
        self.tokens = 0.0
        self.tokens_at = 0.0
        self.since = 0.0
        self.retry_after = PACED_RETRY_SECONDS
        self.retried = False

    def start(self, rate: float, still_queued: int, now: float) -> None:
        """Pace at `rate` messages/s, after the `still_queued` messages that
        are waiting on the client's side have been read."""
        self.rate = max(MIN_PACE_RATE, rate)
        self.tokens = 1.0 - still_queued
        self.tokens_at = now
        self.since = now

    def wait(self, now: float, final: bool, finals_waiting: bool) -> float:
        """Seconds until the head message may be written (0: now). The full
        reading rate while finals pile up, PACING_FRACTION of it otherwise; a
        provisional needs a spare token so it does not delay the next final."""
        rate = self.rate if finals_waiting else PACING_FRACTION * self.rate
        self.tokens = min(PACE_BURST, self.tokens + (now - self.tokens_at) * rate)
        self.tokens_at = now
        need = 1.0 if final else PACE_BURST
        return 0.0 if self.tokens >= need else (need - self.tokens) / rate

    def spend(self) -> None:
        self.tokens -= 1.0


class ClientSession:
    """One student connection: its outbox, the two control loops, and the
    sender task that drains the outbox."""

    def __init__(self, ws, client_id: Any, session: SessionState) -> None:
        self.ws = ws
        self.client_id = client_id
        self.session = session
        self.loop = asyncio.get_running_loop()
        self.wake = asyncio.Event()
        self.closing = False

        self.outbox = Outbox()
        self.gate = NetworkGate(ws)         # loop 1
        self.reader = ReaderMonitor(ws)     # loop 2: observations
        self.pacer = Pacer()                # loop 2: rate limit
        self.preamble: deque[str] = deque()  # HISTORY reply, sent before anything else
        self.catching_up = False             # backlog left over from being held back
        self.catchup_allowed = False         # may receive catch-up HISTORY messages

        self.mode = FULL
        self.mode_before_hold = FULL
        self.hold_onset = 0.0
        self.frames_sent = 0
        self.write_times: deque[float] = deque(maxlen=256)
        self.last_write_at = 0.0
        self.first_write_at: float | None = None

    # Outbox ---------------------------------------------------------------

    def enqueue(self, caption: Caption) -> None:
        """Add an accepted revision. While the client is held back, it
        replaces the unsent older revisions of its segment."""
        if self.closing:
            return
        self.outbox.add(caption, replace=self.mode != FULL or self.gate.holding)
        if self.outbox.count > MAX_PENDING:
            self.outbox.compact()
        if (self.outbox.count > MAX_PENDING
                or self.loop.time() - self.outbox.head().received > MAX_PENDING_AGE):
            self.overflow()
            return
        self.wake.set()

    def overflow(self) -> None:
        """Last resort for a client hopelessly behind: keep state bounded and
        let the client resume through HISTORY."""
        self.closing = True
        self.outbox.clear()
        self.wake.set()
        print(f"client {self.client_id}: more than {MAX_PENDING_AGE:.0f}s behind; "
              "closing so it can resume through HISTORY", flush=True)
        spawn(self.ws.close(code=1013, reason="client too far behind"))

    # Sender ---------------------------------------------------------------

    async def run(self) -> None:
        """Sender task: drain the outbox as the two loops allow."""
        try:
            while self.preamble:
                await self.write(self.preamble.popleft())
            while not self.closing:
                self.wake.clear()
                delay = self.poll(self.loop.time())
                if self.reader.confirm_needed:
                    self.reader.confirm_needed = False
                    self.reader.last_confirm_at = self.loop.time()
                    self.reader.confirms += 1
                    waiter = await self.ws.ping()
                    waiter.add_done_callback(consume_result)
                    continue
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
        """Apply both loops. Return 0.0 if the head message may be written now,
        otherwise how long to wait (None: until woken)."""
        deadline = None

        def until(t: float | None) -> None:
            nonlocal deadline
            if t is not None:
                deadline = t if deadline is None else min(deadline, t)

        # Loop 2: has the library stopped reading?
        if self.mode != HOLD:
            stalled, wait = self.reader.check(now)
            if stalled:
                self.enter_hold(now)
            elif wait is not None:
                until(now + wait)
        if self.mode == HOLD:
            self.reader.watch(now)
            if not self.reader.client_reading:
                if now - self.reader.last_confirm_at >= CONFIRM_INTERVAL:
                    self.reader.confirm_needed = BACKPRESSURE
                return PROBE_CHECK  # until the pong ends the hold
            self.release_hold(now)
        if self.mode == PACED:
            retry_at = self.pacer.since + self.pacer.retry_after
            if now >= retry_at:
                self.leave_paced()
            else:
                until(retry_at)

        if not self.outbox:
            return None if deadline is None else max(deadline - now, 0.001)
        head = self.outbox.head()

        # Loop 1: provisionals wait while the path is behind; finals never do.
        if not head.final:
            if self.gate.behind(now):
                if not self.gate.holding:
                    self.gate.holding = True
                    self.outbox.compact()
                self.catching_up = True
                WAITING.add(self)  # watch_network() wakes us when ACKs catch up
                return None if deadline is None else max(deadline - now, 0.001)
            self.gate.holding = False

        if self.mode == PACED and not self.session.ended:
            finals_waiting = len(self.outbox.leading_finals(2)) >= 2
            wait = self.pacer.wait(now, head.final, finals_waiting)
            if wait > 0:
                until(now + wait)
                return max(deadline - now, 0.001)
        return 0.0

    async def send_next(self) -> None:
        batch = []
        if self.catchup_allowed and (self.mode != FULL or self.catching_up):
            batch = self.outbox.leading_finals(CATCHUP_MAX_FINALS)
        if len(batch) >= CATCHUP_MIN_FINALS:
            for _ in batch:
                self.outbox.pop()
            text = history_text([caption.text for caption in batch])
        else:
            text = self.outbox.pop().text
        if self.mode == PACED:
            self.pacer.spend()
        if not self.outbox:
            self.catching_up = False

        probe = self.reader.due(self.frames_sent)
        # Cork so the caption and its ping leave in one TCP segment.
        corked = probe and set_cork(self.ws, True)
        try:
            await self.write(text)
            if probe:
                await self.reader.probe(self.loop.time(), self.frames_sent,
                                        self.last_write_at, self.on_pong)
        finally:
            if corked:
                set_cork(self.ws, False)

    async def write(self, text: str) -> None:
        await self.ws.send(text)
        now = self.loop.time()
        self.frames_sent += 1
        self.last_write_at = now
        self.write_times.append(now)
        if self.first_write_at is None:
            self.first_write_at = now
        self.gate.wrote(now)

    def write_time(self, frame: int) -> float | None:
        """When the frame-th message was written, if still remembered."""
        back = self.frames_sent - frame
        if 0 <= back < len(self.write_times):
            return self.write_times[-1 - back]
        return None

    def send_rate(self, until: float) -> float:
        """Messages per second written to this client in the SEND_RATE_WINDOW
        before `until` (or since its first write, if that is more recent)."""
        start = until - SEND_RATE_WINDOW
        recent = [t for t in self.write_times if start <= t <= until]
        if not recent:
            return 0.0
        span = until - max(start, self.write_times[0])
        return len(recent) / span if span > 0 else 0.0

    # Loop 2: holds and pacing ---------------------------------------------

    def on_pong(self, waiter: asyncio.Future, sent_at: float) -> None:
        if self.reader.sent_at != sent_at:
            return  # an earlier ping, or one given up on in release_hold()
        if waiter.cancelled() or waiter.exception() is not None:
            self.reader.sent_at = None
            return
        if self.mode == HOLD:
            self.leave_hold(self.loop.time())
        self.reader.confirm()
        self.wake.set()

    def enter_hold(self, now: float) -> None:
        self.mode_before_hold = self.mode
        self.mode = HOLD
        self.reader.hold_started()
        # The library stopped reading after one of the messages from the last
        # answered ping up to the one before the unanswered ping. The middle
        # of that interval is off by at most (PROBE_EVERY - 1) / 2 messages of
        # our sending, against a stall of DRAIN_FRAMES reads by a reader
        # slower than us: the reading rate comes out at most ~14% high, and
        # pacing at PACING_FRACTION of it stays below the true rate.
        earliest = self.reader.confirmed_write_at or self.first_write_at or now
        latest = self.write_time(self.reader.frames_at_probe - 1) or earliest
        self.hold_onset = (earliest + max(earliest, latest)) / 2
        self.outbox.compact()

    def release_hold(self, now: float) -> None:
        """The client answered a later ping while the earlier answer is
        missing: it is reading, and that answer was lost on the way back."""
        self.reader.release()
        self.mode = self.mode_before_hold
        self.catching_up = bool(self.outbox)
        self.pacer.tokens_at = now
        print(f"client {self.client_id}: {now - self.hold_onset:.1f}s stall treated as "
              "network delay, not a slow reader (client answered a later ping)", flush=True)

    def leave_hold(self, now: float) -> None:
        """The pong arrived: the library resumed after the application read
        DRAIN_FRAMES messages, which gives the reading rate."""
        stall = max(now - self.hold_onset, 1e-3)
        previous = self.mode_before_hold
        self.catching_up = bool(self.outbox)
        self.reader.watch(now)
        measured = DRAIN_FRAMES / stall
        sent = self.send_rate(self.hold_onset)
        if not PACING or self.reader.client_reading or measured >= sent:
            # It reads at least as fast as we send: the stall was the network
            # (a delayed pong) or a burst, not a slow reader.
            self.mode = previous
            self.pacer.tokens_at = now
            if stall >= LOG_STALL:
                print(f"client {self.client_id}: {stall:.1f}s stall treated as network delay, "
                      f"not a slow reader (reads ~{measured:.2f} msg/s, we sent ~{sent:.2f})",
                      flush=True)
            return
        if previous == FULL and self.pacer.retried:
            # It stalled again after a retry at full rate: retry later next time.
            self.pacer.retry_after = min(2 * self.pacer.retry_after, PACED_RETRY_MAX)
        # Still queued on the client's side: LIBRARY_RESUME messages, plus
        # everything written since the last confirmed read except the one
        # that filled the library's queue.
        still_queued = LIBRARY_RESUME + max(0, self.frames_sent - self.reader.confirmed_frames - 1)
        self.pacer.start(measured, still_queued, now)
        self.mode = PACED
        print(f"client {self.client_id}: slow reader (stalled {stall:.1f}s, reads "
              f"~{self.pacer.rate:.2f} msg/s, we sent ~{sent:.2f}); pacing", flush=True)

    def leave_paced(self) -> None:
        """Stall-free for a while: try full-rate delivery again."""
        self.mode = FULL
        self.pacer.retried = True
        print(f"client {self.client_id}: no stall for {self.pacer.retry_after:.0f}s; "
              "trying full rate again", flush=True)

    def describe(self, now: float) -> str:
        """One-line state for diagnostics."""
        state = f"client {self.client_id}: mode={self.mode}"
        if self.mode == HOLD:
            state += (f" for {now - self.hold_onset:.1f}s, confirmations={self.reader.confirms}, "
                      f"client_reading={self.reader.client_reading}")
        state += (f", network lag {self.gate.lag(now):.2f}s holding={self.gate.holding}, "
                  f"pending={self.outbox.count}, written={self.frames_sent}, "
                  f"last write {now - self.last_write_at:.1f}s ago")
        return f"{state}; tcp: {tcp_report(self.ws)}"


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


def tcp_info(ws) -> bytes | None:
    """Linux TCP_INFO of the connection's socket."""
    transport = ws.transport
    sock = transport.get_extra_info("socket") if transport is not None else None
    if TCP_INFO is None or sock is None:
        return None
    try:
        return sock.getsockopt(socket.IPPROTO_TCP, TCP_INFO, 256)
    except OSError:
        return None


def tcp_info_field(info: bytes | None, offset: int, fmt: str) -> int | None:
    """One struct tcp_info field, or None if this kernel does not report it."""
    if info is None or len(info) < offset + struct.calcsize(fmt):
        return None
    return struct.unpack_from(fmt, info, offset)[0]


def tcp_receive_counters(ws) -> tuple[int, int] | None:
    """(in-order bytes received, data segments received) from Linux TCP_INFO."""
    info = tcp_info(ws)
    delivered = tcp_info_field(info, TCPI_BYTES_RECEIVED, "<Q")
    segments = tcp_info_field(info, TCPI_DATA_SEGS_IN, "<I")
    if delivered is None or segments is None:
        return None
    return delivered, segments


def stream_position(ws) -> int | None:
    """Bytes handed to the connection so far: acknowledged plus still queued.
    The queue is read first, so an ACK arriving in between can only move the
    result later (a ping then counts as delivered slightly late), never
    earlier."""
    queued = unacked_bytes(ws)
    acked = tcp_info_field(tcp_info(ws), TCPI_BYTES_ACKED, "<Q")
    if queued is None or acked is None:
        return None
    return acked + queued


def set_cork(ws, on: bool) -> bool:
    """Set Linux TCP_CORK: while set, small writes are held and leave as one
    segment when it is cleared. Returns whether the option could be set."""
    transport = ws.transport
    sock = transport.get_extra_info("socket") if transport is not None else None
    if TCP_CORK is None or sock is None:
        return False
    try:
        sock.setsockopt(socket.IPPROTO_TCP, TCP_CORK, 1 if on else 0)
    except OSError:
        return False
    return True


def tcp_report(ws) -> str:
    """Retransmission and receive state from TCP_INFO, for diagnostics."""
    info = tcp_info(ws)
    if info is None:
        return "n/a"
    fields = {name: tcp_info_field(info, offset, fmt) for name, (offset, fmt) in TCPI_REPORT.items()}
    for name in ("rto_ms", "rtt_ms"):
        if fields[name] is not None:
            fields[name] //= 1000  # reported in microseconds
    return " ".join(f"{name}={value}" for name, value in fields.items())


def close_description(ws) -> str:
    """How the connection ended, plus the socket error if the transport failed."""
    try:
        text = str(ws.protocol.close_exc)
    except Exception:  # close state not available
        text = "unknown"
    error = getattr(ws, "recv_exc", None)
    if error is not None and not isinstance(error, ConnectionClosed):
        text += f" ({type(error).__name__}: {error})"
    return text


def consume_result(future: asyncio.Future) -> None:
    """Done-callback for pings whose result we ignore."""
    if not future.cancelled():
        future.exception()


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
    types are ignored. SOURCE_DONE is not forwarded; it ends pacing until
    the next caption.
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
                get_session(session_name(msg.get("session", hello_session))).end_stream()
    except ConnectionClosed:
        pass
    finally:
        session = get_session(hello_session)
        session.sources -= 1
        if session.sources == 0:
            session.end_stream()
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
    # Catch-up HISTORY only for clients that start with nothing to recover. A
    # resumed or late-joining connection gets exactly one HISTORY (its
    # recovery reply), so its recovery is never confused with catch-up.
    client.catchup_allowed = CATCHUP_HISTORY and last_segment == 0 and not session.finals
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
        WAITING.discard(client)
        client.closing = True
        sender.cancel()
        try:
            await sender
        except (asyncio.CancelledError, ConnectionClosed):
            pass
        print(f"client disconnected: id={client_id}; clients={len(session.clients)}; "
              f"{close_description(ws)}", flush=True)
        if client.mode == HOLD:
            print(f"  it was held: {client.describe(client.loop.time())}", flush=True)


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
    queued = sum(client.outbox.count for client in clients)
    history = sum(len(session.finals) for session in SESSIONS.values())
    network = sum(client.gate.holding for client in clients)
    print(f"stats: clients={len(clients)} full={modes[FULL]} hold={modes[HOLD]} "
          f"paced={modes[PACED]} queued={queued} history_finals={history} "
          f"network_held={network} waiting_for_ack={len(WAITING)}", flush=True)
    now = asyncio.get_running_loop().time()
    held = sorted((client for client in clients
                   if client.mode == HOLD and now - client.hold_onset > REPORT_HELD_AFTER),
                  key=lambda client: client.hold_onset)
    for client in held[:REPORT_HELD_MAX]:
        print(f"held: {client.describe(now)}", flush=True)


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


async def watch_network() -> None:
    """Loop 1's wake-up: asyncio has no event for an incoming ACK, so one task
    looks at the clients waiting for their path to catch up every LINK_TICK
    and wakes those whose oldest unacknowledged message is young enough
    again."""
    loop = asyncio.get_running_loop()
    while True:
        await asyncio.sleep(LINK_TICK)
        now = loop.time()
        for client in list(WAITING):
            if client.closing or not client.gate.behind(now):
                WAITING.discard(client)
                client.wake.set()


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
        if NETWORK_GATE:
            spawn(watch_network())
        print(f"LiveRecall server listening on ws://0.0.0.0:{port} "
              f"(backpressure={BACKPRESSURE}, pacing={PACING}, "
              f"network_gate={NETWORK_GATE}, catchup_history={CATCHUP_HISTORY})", flush=True)
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
