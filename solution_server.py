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
    HOLD   a lag probe reached the client's TCP but its application has not
           answered it, so the client may have stopped reading. Nothing is
           written; a newer revision replaces the unsent older revisions of
           the same segment. Finals are never dropped.
    PACED  the client application reads slower than the caption stream. Its
           messages are rate-limited to just below its measured reading rate,
           finals first, so few (and only current) messages wait in its
           receive buffer.

Catch-up: when a lagging client has two or more unsent finals, they are sent
together as one HISTORY message (finals only, oldest first, none the client
already holds), so a slow reader spends one read on them instead of one per
final. Only clients that joined with nothing to recover get this; a resumed
or late-joining connection keeps exactly one HISTORY, its recovery reply.

Lag probes: after writing data the sender sends a WebSocket ping. The client
answers only once it has read everything sent before the ping. A websockets
client keeps reading until 16 unread messages are queued and then stops
reading from TCP until its application has worked the queue down to 4, so an
overdue pong is the earliest sign of a slow reader. Server-side send buffers
stay empty in that case because the client's kernel keeps accepting data.

A late pong only starts a HOLD once the kernel reports that the client's TCP
has acknowledged the ping (or closed its receive window): the ping arrived
and the application did not answer. A ping still on its way, behind a
congested link or a lost segment, is network delay; holding the client back
would only add to it, so the client stays in its mode.

The pong itself can be lost too, and TCP then waits longer and longer before
resending it, because the client has nothing else to send. So a held client
is pinged again every 0.5 s. A client that is not reading cannot answer; one
that is reading does, and its answers arrive out of order behind the lost one
(the kernel counts the segment but cannot deliver it). That ends the hold at
once, with no new hold until the lost data arrives, and the out-of-order
arrival makes the client's TCP resend the lost reply right away. A hold only
turns into pacing if every byte was acknowledged, the client sent us nothing
in the meantime and the reading rate the stall implies is clearly below the
rate we were sending. Otherwise the pong was held up in the network and the
client goes back to its previous mode.

A pong that does not ride on the client's ACK costs a packet, which matters
on a constrained link. Clients with no lag in the last 30 s are therefore
probed once a second (or every 8 messages, if sooner); lagging or paced
clients are probed with every message, at most every 0.25 s. Each time a ping
is found still on its way, that client's probe intervals double (up to 8x),
so probing backs off on a congested link; a prompt answer halves them again.

No probes at all while the path queues: when TCP's smoothed RTT is more than
0.2 s above the base RTT, the client is not probed until it is back within
0.1 s. On an overloaded link the pongs compete with captions and get lost, and
under heavy local drops Linux was seen to abort such two-way connections with
ETIMEDOUT within seconds, without the client noticing. A slow reader on a
congested path is then handled like any other client. The base RTT is the
lowest minimum RTT of any client connection: clients share the path to the
server (one classroom network, or loopback in the tests), and a connection
opened while the link was already congested never sees its own base RTT. A
client much farther away than the others would look congested and simply not
be probed.

Lean delivery on an overloaded link: when a message sent to a client is still
unacknowledged after 1 s and the path is queueing (TCP's RTT more than 0.2 s
above the base RTT), that client's share of the link cannot carry the
caption stream, and everything handed to TCP queues behind it (lost segments
hold up all later ones). A late message on a path that is not queueing (a
single loss being repaired) does not count. The client then gets at most one provisional
revision in flight: the next is written only once TCP has acknowledged what
was sent, and meanwhile newer revisions replace the unsent older revisions
of the same segment. Finals never wait: they are written at once (waiting
finals as one catch-up HISTORY). What is sent is therefore current, and
fewer bytes compete for the link. The wait applies only while TCP's smoothed
RTT is at least 50 ms, so a lean client whose link recovers gets every
revision again at once; it returns to normal delivery once its path has not
been queueing for 10 s. Lean clients are not probed.

Experiment switches (environment variables, inherited by run_demo.py):
    LIVERECALL_BACKPRESSURE=0     no lag probes: always FULL
    LIVERECALL_PACING=0           HOLD/coalescing only, never PACED
    LIVERECALL_CATCHUP_HISTORY=0  never batch finals into a catch-up HISTORY
    LIVERECALL_LEAN=0             never switch a client to lean delivery
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
TCPI_RTT = 68                # smoothed RTT, microseconds
TCPI_MIN_RTT = 148           # minimum RTT seen, microseconds
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

# Lag detection.
PROBE_INTERVAL = 0.25        # at most one lag probe per 0.25 s, only after data.
                             # The ping shares the caption's TCP segment (corked);
                             # the pong rides on the client's ACK only if the
                             # client answers at once, otherwise it is a packet
HEALTHY_PROBE_INTERVAL = 1.0 # no recent lag: a probe per second...
HEALTHY_PROBE_FRAMES = 8     # ...or after 8 unprobed messages, if sooner
WATCH_SECONDS = 30.0         # lagged this recently: probe every PROBE_INTERVAL
PROBE_BACKOFF_MAX = 8.0      # a ping still in the network when checked doubles
                             # the client's probe intervals (up to 8x); a prompt
                             # answer halves them again
QUEUEING_STOP = 0.2          # no probes while TCP's RTT is this far above the
QUEUEING_RESUME = 0.1        # base RTT, until it is back under this
LAG_MARGIN = 0.25            # a probe is overdue after min RTT + LAG_MARGIN...
DELIVERY_GRACE = LAG_MARGIN / 2  # ...and once the client's TCP has acknowledged
                                 # the ping this long ago (the pong may trail
                                 # the ACK by jitter or a busy client)
INITIAL_RTT = 0.25           # RTT assumed before the first sample
RTT_SAMPLES = 32             # min-RTT window
HOLD_SAMPLE_INTERVAL = 0.1   # kernel send-queue check period while held
CONFIRM_INTERVAL = 0.5       # while held, ping again this often. A client that
                             # is reading answers; arriving behind a lost reply,
                             # the answers show that and make its TCP resend the
                             # lost one at once instead of after a backed-off
                             # retransmission timeout
RECEIVER_CONFIRM = 0.5       # pong still missing this long after the kernel
                             # reported every byte acknowledged: the client
                             # application is not reading
MIN_RECEIVER_STALL = 1.0
ONSET_LOOKBACK = 1.0         # a stall cannot have begun earlier than this
                             # before the overdue probe
# A reader can only fall 16 messages behind if it reads clearly slower than
# we send to it. A stall implying a reading rate above this fraction of our
# recent sending rate was the network instead (e.g. a lost pong being
# retransmitted while our data was still acknowledged).
READER_RATE_RATIO = 0.7
SEND_RATE_WINDOW = 10.0      # seconds of writes used for the sending rate

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

# Lean delivery on an overloaded link (one message in flight, coalescing).
LEAN_ENTER_AGE = 1.0         # a message unacknowledged this long on a queueing path: go lean
LEAN_EXIT_SECONDS = 10.0     # path not queueing (QUEUEING_STOP) this long: back to normal
LINK_TICK = 0.05             # how often waiting lean clients are checked for an ACK
LEAN_GATE_RTT = 0.05         # one message in flight only if TCP's RTT is at least this
LINK_CHECK_INTERVAL = 0.25   # how often every client's oldest unacknowledged message is checked
UNACKED_WRITES = 1024        # write positions remembered per client

# Outbox bounds.
MAX_PENDING = 512                      # entries before forced coalescing
MAX_PENDING_AGE = HISTORY_MAX_SECONDS  # oldest unsent caption older: disconnect

STATS_INTERVAL = 10.0
REPORT_HELD_AFTER = 2.0      # stats also describe clients held longer than this
REPORT_HELD_MAX = 10

BACKPRESSURE = env_flag("LIVERECALL_BACKPRESSURE", True)
PACING = env_flag("LIVERECALL_PACING", True)
CATCHUP_HISTORY = env_flag("LIVERECALL_CATCHUP_HISTORY", True)
LEAN = env_flag("LIVERECALL_LEAN", True)
CATCHUP_MIN_FINALS = 2
CATCHUP_MAX_FINALS = 200

FULL, HOLD, PACED = "full", "hold", "paced"

DEFAULT_SESSION = "L1"
SESSIONS: dict[str, SessionState] = {}
BASE_RTT_US = math.inf  # lowest minimum RTT of any client connection (link_queueing)
GATED: set[ClientSession] = set()  # lean clients waiting for their last message's ACK
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
        self.catching_up = False             # backlog left over from a HOLD or lean delivery
        self.catchup_allowed = False         # may receive catch-up HISTORY messages
        self.lean = False                    # one message in flight, coalescing
        self.lean_calm_since: float | None = None
        # (stream position after each write, write time) not yet acknowledged
        self.unacked_writes: deque[tuple[int, float]] = deque(maxlen=UNACKED_WRITES)

        self.mode = FULL
        self.mode_before_hold = FULL
        self.hold_onset = 0.0
        self.acked_since: float | None = None
        self.confirm_needed = False   # send a confirmation ping
        self.confirms = 0             # confirmation pings sent in the current hold
        self.last_confirm_at = -math.inf
        # In-order bytes received from the client when its replies were found
        # stuck behind a lost segment; no new hold until more arrive.
        self.reply_gap: int | None = None
        self.hold_counters: tuple[int, int] | None = None
        self.client_reading = False   # out-of-order client data seen during the hold

        self.probe_sent_at: float | None = None
        self.probe_end: int | None = None    # stream position just past the ping
        self.probe_delivered_at: float | None = None
        self.probe_congested = False         # the ping was still in the network when checked
        self.probe_backoff = 1.0
        self.queueing = False                # path congested: no probes (link_queueing)
        self.last_probe_at = -math.inf
        self.lag_seen_at = -math.inf
        self.rtts: deque[float] = deque(maxlen=RTT_SAMPLES)
        self.rtt_min = INITIAL_RTT
        self.frames_sent = 0
        self.frames_at_probe = 0
        self.frames_confirmed = 0            # frames_at_probe of the last answered probe
        self.write_times: deque[float] = deque(maxlen=256)
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
        if self.lean:
            self.catching_up = True  # waiting finals may leave as one HISTORY
        if queue is None:
            self.pending[caption.segment] = deque((caption,))
            self.n_pending += 1
        elif self.mode == FULL and not self.lean:
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
                if self.confirm_needed:
                    self.confirm_needed = False
                    self.last_confirm_at = self.loop.time()
                    self.confirms += 1
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
        """Update the delivery mode. Return 0.0 if the next message may be sent
        now, otherwise how long to wait (None: until woken)."""
        deadline = None
        if self.probe_sent_at is not None and self.mode != HOLD:
            expected = self.probe_sent_at + self.rtt_min  # pong due on a clear path
            check_at = expected + DELIVERY_GRACE
            if self.probe_delivered_at is None and now >= check_at:
                if self.ping_delivered():
                    self.probe_delivered_at = now
                elif not self.probe_congested:
                    # Queued or lost on the way: probe this client less often.
                    self.probe_congested = True
                    self.probe_backoff = min(PROBE_BACKOFF_MAX, 2 * self.probe_backoff)
            if self.probe_delivered_at is None:
                # Still in the network (or answered before the check): no
                # sign of a slow reader yet, keep sending and look again.
                deadline = check_at if now < check_at else now + HOLD_SAMPLE_INTERVAL
            else:
                overdue_at = max(expected + LAG_MARGIN, self.probe_delivered_at + DELIVERY_GRACE)
                if now >= overdue_at:
                    self.enter_hold()
                else:
                    deadline = overdue_at

        if self.mode == HOLD:
            if self.acked_since is None:
                outstanding = unacked_bytes(self.ws)
                if (outstanding is None or outstanding == 0
                        or tcp_info_field(tcp_info(self.ws), TCPI_SND_WND, "<I") == 0):
                    # Everything we wrote has reached the client's kernel, or
                    # its receive buffer is full (or the platform cannot
                    # tell), yet the pong is missing.
                    self.acked_since = now
            self.watch_client_data()
            if self.client_reading:
                self.release_hold(now)  # and carry on sending below
            else:
                if now - self.last_confirm_at >= CONFIRM_INTERVAL:
                    self.confirm_needed = BACKPRESSURE
                return HOLD_SAMPLE_INTERVAL  # keep watching until on_pong() ends the hold

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
            head_final = next(iter(self.pending.values()))[0].final
            if self.lean and not head_final and self.slow_path() and self.in_flight():
                GATED.add(self)  # watch_links() wakes us once it is acknowledged
                return None if deadline is None else max(deadline - now, 0.001)
            GATED.discard(self)
            return 0.0
        return None if deadline is None else max(deadline - now, 0.001)

    def in_flight(self) -> bool:
        """Is anything we wrote still unacknowledged by the client's TCP?"""
        return bool(unacked_bytes(self.ws))

    def slow_path(self) -> bool:
        """TCP's smoothed RTT is at least LEAN_GATE_RTT. On a faster path,
        waiting for each ACK (checked every LINK_TICK) would only add delay."""
        rtt = tcp_info_field(tcp_info(self.ws), TCPI_RTT, "<I")
        return rtt is None or rtt >= LEAN_GATE_RTT * 1e6

    def check_link(self, now: float) -> None:
        """Go lean when a message has been unacknowledged for LEAN_ENTER_AGE
        on a queueing path (a single loss on an uncongested path does not
        count); go back once the path has not been queueing for
        LEAN_EXIT_SECONDS."""
        acked = tcp_info_field(tcp_info(self.ws), TCPI_BYTES_ACKED, "<Q")
        if acked is None:
            return
        writes = self.unacked_writes
        while writes and writes[0][0] <= acked:
            writes.popleft()
        age = now - writes[0][1] if writes else 0.0
        delay = self.queueing_delay()
        queueing = delay is not None and delay != math.inf and delay > QUEUEING_STOP
        if not self.lean:
            if age > LEAN_ENTER_AGE and queueing:
                self.lean = True
                self.lean_calm_since = None
                print(f"client {self.client_id}: a message unacknowledged for {age:.1f}s, "
                      f"path queueing {delay:.2f}s; lean delivery "
                      "(finals at once, one provisional in flight)", flush=True)
        elif queueing:
            self.lean_calm_since = None
        elif self.lean_calm_since is None:
            self.lean_calm_since = now
        elif now - self.lean_calm_since >= LEAN_EXIT_SECONDS:
            self.lean = False
            self.wake.set()
            print(f"client {self.client_id}: path not queueing for {LEAN_EXIT_SECONDS:.0f}s; "
                  "normal delivery", flush=True)

    async def send_next(self) -> None:
        batch = []
        if self.catchup_allowed and (self.mode != FULL or self.catching_up):
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

        probe_due = self.probe_due(self.loop.time())
        # Cork so the caption and its probe ping leave in one TCP segment.
        corked = probe_due and set_cork(self.ws, True)
        try:
            await self.write(text)
            if probe_due:
                await self.probe(self.loop.time())
        finally:
            if corked:
                set_cork(self.ws, False)

    async def write(self, text: str) -> None:
        await self.ws.send(text)
        self.frames_sent += 1
        self.last_write_at = self.loop.time()
        self.write_times.append(self.last_write_at)
        if LEAN:
            end = stream_position(self.ws)
            if end is not None:
                self.unacked_writes.append((end, self.last_write_at))

    def send_rate(self, until: float) -> float:
        """Messages per second written to this client in the SEND_RATE_WINDOW
        before `until` (or since its first write, if that is more recent)."""
        start = until - SEND_RATE_WINDOW
        recent = [t for t in self.write_times if start <= t <= until]
        if not recent:
            return 0.0
        span = until - max(start, self.write_times[0])
        return len(recent) / span if span > 0 else 0.0

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

    def probe_due(self, now: float) -> bool:
        """Should the message about to be written carry a lag probe?"""
        if not BACKPRESSURE or self.probe_sent_at is not None or self.lean:
            return False
        since = now - self.last_probe_at
        backoff = self.probe_backoff
        if since < PROBE_INTERVAL * backoff:
            return False
        if not (self.mode != FULL or now - self.lag_seen_at < WATCH_SECONDS
                or since >= HEALTHY_PROBE_INTERVAL * backoff
                or self.frames_sent - self.frames_at_probe >= HEALTHY_PROBE_FRAMES * backoff):
            return False
        return not self.link_queueing()

    def queueing_delay(self) -> float | None:
        """TCP's smoothed RTT above the base RTT, in seconds: math.inf before
        the first RTT sample, None if the platform cannot tell."""
        global BASE_RTT_US
        info = tcp_info(self.ws)
        rtt = tcp_info_field(info, TCPI_RTT, "<I")
        min_rtt = tcp_info_field(info, TCPI_MIN_RTT, "<I")
        if rtt is None or min_rtt is None:
            return None
        if 0 < min_rtt < 0xFFFFFFFF:  # all ones: no sample yet
            BASE_RTT_US = min(BASE_RTT_US, min_rtt)
        if rtt == 0 or BASE_RTT_US == math.inf:
            return math.inf
        return (rtt - BASE_RTT_US) / 1e6

    def link_queueing(self) -> bool:
        """Is the path to this client queueing? TCP's smoothed RTT more than
        QUEUEING_STOP above the base RTT stops probing until it is back under
        QUEUEING_RESUME. Without an RTT sample yet, wait."""
        queueing = self.queueing_delay()
        if queueing is None:
            return False  # platform cannot tell: probe as usual
        if queueing == math.inf:
            return True
        limit = QUEUEING_RESUME if self.queueing else QUEUEING_STOP
        self.queueing = queueing > limit
        return self.queueing

    async def probe(self, now: float) -> None:
        self.probe_sent_at = self.last_probe_at = now
        self.probe_delivered_at = None
        self.probe_congested = False
        self.frames_at_probe = self.frames_sent
        self.probe_last_write = self.last_write_at
        waiter = await self.ws.ping()
        self.probe_end = stream_position(self.ws)
        waiter.add_done_callback(lambda done, sent_at=now: self.on_pong(done, sent_at))

    def ping_delivered(self) -> bool:
        """Has the client's TCP received the outstanding probe? True when the
        platform cannot tell, so any late pong then counts as lag."""
        if self.reply_gap is not None:
            counters = tcp_receive_counters(self.ws)
            if counters is not None and counters[0] == self.reply_gap:
                return False  # its answer is stuck behind the lost reply
            self.reply_gap = None
        info = tcp_info(self.ws)
        acked = tcp_info_field(info, TCPI_BYTES_ACKED, "<Q")
        if self.probe_end is None or acked is None:
            return True
        return acked >= self.probe_end or tcp_info_field(info, TCPI_SND_WND, "<I") == 0

    def on_pong(self, waiter: asyncio.Future, sent_at: float) -> None:
        failed = waiter.cancelled() or waiter.exception() is not None
        if self.probe_sent_at != sent_at:
            return
        self.probe_sent_at = None
        if failed:
            return
        now = self.loop.time()
        if not self.probe_congested:
            self.probe_backoff = max(1.0, self.probe_backoff / 2)
        if self.mode == HOLD:
            self.leave_hold(now, now - sent_at)
        else:
            self.add_rtt(now - sent_at)
        self.confirmed_write_at = self.probe_last_write
        self.frames_confirmed = self.frames_at_probe
        self.wake.set()

    def add_rtt(self, rtt: float) -> None:
        self.rtts.append(rtt)
        self.rtt_min = min(self.rtts)

    def watch_client_data(self) -> None:
        """A client whose application has stopped reading cannot answer our
        pings, so it sends no data. If a data segment from it arrives that the
        kernel cannot deliver yet (data_segs_in rises, in-order bytes do not),
        the client is reading and its earlier reply is stuck in TCP
        retransmission: the stall is the network's."""
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

    def enter_hold(self) -> None:
        self.mode_before_hold = self.mode
        self.mode = HOLD
        self.lag_seen_at = self.loop.time()
        self.hold_counters = tcp_receive_counters(self.ws)
        self.client_reading = False
        self.confirms = 0
        # A client that is still reading answers this ping, and that answer
        # reaches our kernel even if an earlier reply is being retransmitted.
        self.confirm_needed = BACKPRESSURE
        # The client stopped reading between the last message it confirmed and
        # the overdue probe; the earlier bound gives a conservative (lower)
        # reading-rate estimate.
        if self.confirmed_write_at is not None:
            self.hold_onset = max(self.confirmed_write_at, self.probe_sent_at - ONSET_LOOKBACK)
        else:
            self.hold_onset = self.probe_sent_at
        self.acked_since = None
        self.compact()

    def describe(self, now: float) -> str:
        """One-line state for diagnostics."""
        state = f"client {self.client_id}: mode={self.mode}"
        if self.mode == HOLD:
            state += (f" for {now - self.hold_onset:.1f}s, confirmations={self.confirms}, "
                      f"all_acked={self.acked_since is not None}, client_reading={self.client_reading}")
        state += (f", lean={self.lean}, pending={self.n_pending}, written={self.frames_sent}, "
                  f"last write {now - self.last_write_at:.1f}s ago, probe_backoff={self.probe_backoff:g}")
        return f"{state}; tcp: {tcp_report(self.ws)}"

    def release_hold(self, now: float) -> None:
        """The client answered a later ping while the probe's answer is still
        missing: it is reading, and that answer was lost on the way back.
        Resume now instead of waiting for TCP to resend it, and do not hold
        again until the lost data has arrived."""
        counters = tcp_receive_counters(self.ws)
        self.reply_gap = counters[0] if counters is not None else None
        self.probe_sent_at = None  # its late pong is ignored
        self.mode = self.mode_before_hold
        self.catching_up = self.n_pending > 0
        self.tokens_at = now
        print(f"client {self.client_id}: {now - self.hold_onset:.1f}s stall treated as "
              "network delay, not a slow reader (client answered a later ping)", flush=True)

    def leave_hold(self, now: float, probe_rtt: float) -> None:
        stall = now - self.hold_onset
        previous = self.mode_before_hold
        self.catching_up = self.n_pending > 0
        self.watch_client_data()
        measured = DRAIN_FRAMES / stall
        sent = self.send_rate(self.hold_onset)
        all_acked = self.acked_since is not None and now - self.acked_since >= RECEIVER_CONFIRM
        receiver_limited = (
            stall >= MIN_RECEIVER_STALL
            and all_acked
            and not self.client_reading
            and measured <= READER_RATE_RATIO * sent
        )
        if not receiver_limited or not PACING:
            # Network queueing, loss recovery or a short hiccup: resume the
            # previous mode with the coalesced (newest) state.
            if not receiver_limited:
                self.add_rtt(probe_rtt)
                if stall >= MIN_RECEIVER_STALL and all_acked:
                    reason = ("client kept sending data" if self.client_reading else
                              f"would mean reading ~{measured:.2f} msg/s while we sent ~{sent:.2f}")
                    print(f"client {self.client_id}: {stall:.1f}s stall treated as network "
                          f"delay, not a slow reader ({reason})", flush=True)
            self.mode = previous
            self.tokens_at = now
            return

        if previous == PACED:
            measured = min(measured, 0.7 * self.reading_rate)  # stalled while paced
        elif self.retried:
            self.retry_after = min(2 * self.retry_after, PACED_RETRY_MAX)
        self.reading_rate = max(MIN_PACE_RATE, measured)
        self.mode = PACED
        # Token debt: let the client read the messages still queued on its
        # side (RESUME_QUEUE plus anything written after the overdue probe)
        # before sending more, so what it reads next is current. It stopped
        # reading somewhere after the last answered probe, so messages
        # written between that probe and the overdue one may be queued too.
        # Probing every message, that is only the overdue probe's own
        # caption, which RESUME_QUEUE was measured with; a client probed
        # once a second can have several.
        unconfirmed = max(0, self.frames_at_probe - self.frames_confirmed - 1)
        still_queued = RESUME_QUEUE + unconfirmed + (self.frames_sent - self.frames_at_probe)
        self.tokens = 1.0 - still_queued
        self.tokens_at = now
        self.paced_since = now
        print(f"client {self.client_id}: slow reader (stalled {stall:.1f}s, reads "
              f"~{self.reading_rate:.2f} msg/s, we sent ~{sent:.2f}); pacing", flush=True)

    def leave_paced(self) -> None:
        """Stall-free for a while: try full-rate delivery again."""
        self.mode = FULL
        self.retried = True
        self.lag_seen_at = self.loop.time()  # keep probing it closely
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
        GATED.discard(client)
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
    queued = sum(client.n_pending for client in clients)
    history = sum(len(session.finals) for session in SESSIONS.values())
    backed_off = sum(client.probe_backoff > 1 for client in clients)
    queueing = sum(client.queueing for client in clients)
    base = f"{BASE_RTT_US / 1000:.3f}ms" if BASE_RTT_US != math.inf else "none"
    lean = sum(client.lean for client in clients)
    print(f"stats: clients={len(clients)} full={modes[FULL]} hold={modes[HOLD]} "
          f"paced={modes[PACED]} queued={queued} history_finals={history} "
          f"probe_backoff={backed_off} queueing={queueing} base_rtt={base} "
          f"lean={lean} waiting_for_ack={len(GATED)}", flush=True)
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


async def watch_links() -> None:
    """Wake lean clients whose last message has been acknowledged, and check
    every client's oldest unacknowledged message every LINK_CHECK_INTERVAL.
    One task polls the kernel for all clients (asyncio has no event for an
    incoming ACK)."""
    loop = asyncio.get_running_loop()
    next_check = loop.time()
    while True:
        await asyncio.sleep(LINK_TICK)
        for client in list(GATED):
            if client.closing or not client.in_flight():
                GATED.discard(client)
                client.wake.set()
        now = loop.time()
        if now >= next_check:
            next_check = now + LINK_CHECK_INTERVAL
            for session in list(SESSIONS.values()):
                for client in list(session.clients):
                    client.check_link(now)


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
        if LEAN:
            spawn(watch_links())
        print(f"LiveRecall server listening on ws://0.0.0.0:{port} "
              f"(backpressure={BACKPRESSURE}, pacing={PACING}, "
              f"catchup_history={CATCHUP_HISTORY}, lean={LEAN})", flush=True)
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
