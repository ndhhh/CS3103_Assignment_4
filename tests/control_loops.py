"""The two control loops (network gate, reader monitor) and pacing, with
simulated kernel counters instead of a real connection."""
import asyncio, sys
from helpers import ROOT
sys.path.insert(0, str(ROOT))
import solution_server as s

class K:                 # what TCP_INFO / TIOCOUTQ would report
    acked = 0            # bytes acknowledged by the client's TCP
    written = 0          # bytes handed to the socket
    rx_inorder = 0       # in-order bytes received from the client
    rx_segs = 0          # data segments received from the client

def field(info, offset, fmt):
    return {s.TCPI_BYTES_ACKED: K.acked, s.TCPI_SND_WND: 65535}.get(offset)
s.tcp_info = lambda ws: b"x"
s.tcp_info_field = field
s.stream_position = lambda ws: K.written
s.unacked_bytes = lambda ws: K.written - K.acked
s.tcp_receive_counters = lambda ws: (K.rx_inorder, K.rx_segs)
s.set_cork = lambda ws, on: False

class WS:
    async def send(self, text): K.written += 40
    async def ping(self):
        K.written += 6
        return asyncio.get_running_loop().create_future()

class Clock:
    def __init__(self, t): self.t = t
    def time(self): return self.t

def check(cond, msg):
    print(("ok   " if cond else "FAIL ") + msg)
    if not cond: sys.exit(1)

def cap(seg, rev, final, t):
    return s.Caption(seg, rev, final, t, f'{{"segment": {seg}, "revision": {rev}}}')

def done():
    f = asyncio.get_running_loop().create_future(); f.set_result(0.0); return f

async def main():
    sess = s.SessionState("L1")
    c = s.ClientSession(WS(), "c", sess)
    clock = Clock(100.0); c.loop = clock

    # ---- Loop 1 -----------------------------------------------------------
    c.enqueue(cap(1, 1, False, 100.0))
    check(c.poll(100.0) == 0.0, "healthy path: provisional goes out at once")
    await c.send_next(); K.acked = K.written
    c.enqueue(cap(1, 2, False, 100.0)); c.enqueue(cap(1, 3, False, 100.0))
    check(c.outbox.count == 2, "not held back: every revision is kept (full coverage)")
    while c.outbox:
        await c.send_next()
    clock.t = 100.6                       # nothing acknowledged for 0.6 s
    c.enqueue(cap(2, 1, False, 100.6))
    check(c.poll(100.6) is None and c.gate.holding and c in s.WAITING,
          "path 0.6 s behind: provisional waits in the outbox")
    c.enqueue(cap(2, 2, False, 100.6))
    check(c.outbox.count == 1 and c.outbox.head().revision == 2, "newer revision replaces the waiting one")
    c.enqueue(cap(2, 3, True, 100.6))
    check(c.outbox.head().final and c.poll(100.6) == 0.0, "final replaces it and goes out despite the lag")
    await c.send_next()
    c.enqueue(cap(3, 1, False, 100.7))
    check(c.poll(100.7) != 0.0 and c.gate.holding, "provisional behind it still waits")
    K.acked = K.written                   # the path caught up
    check(not c.gate.behind(100.7), "ACKs caught up: gate open (watch_network would wake it)")
    check(c.poll(100.7) == 0.0 and not c.gate.holding, "provisional goes out, holding cleared")
    await c.send_next(); s.WAITING.discard(c)

    # ---- Loop 2: probes -----------------------------------------------------
    c2 = s.ClientSession(WS(), "r", sess); c2.loop = Clock(200.0)
    N = s.PROBE_EVERY
    due = [c2.reader.due(n) for n in range(0, N + 1)]
    check(due == [False] * (N - 1) + [True, True], f"first ping on message {N}")
    for i in range(1, N + 1):             # N messages; the last carries a ping
        c2.loop.t = 200.0 + 0.25 * i
        K.acked = K.written               # earlier messages acknowledged
        c2.enqueue(cap(10 + i, 1, False, c2.loop.t))
        assert c2.poll(c2.loop.t) == 0.0
        await c2.send_next()
    check(c2.reader.sent_at is not None and c2.frames_sent == N, f"ping sent with message {N}")
    t = c2.reader.sent_at
    for dt in (0.1, 0.5, 2.0):            # ping still in the network: never a hold
        c2.poll(t + dt)
    check(c2.mode == s.FULL, "unacknowledged ping (network delay): no hold, however long")
    K.acked = K.written                   # ping delivered at t + 2.0
    c2.poll(t + 2.0)
    check(c2.mode == s.FULL, "just delivered: no hold yet")
    c2.poll(t + 2.0 + s.PONG_GRACE + 0.01)
    check(c2.mode == s.HOLD and c2.reader.confirm_needed, "delivered and unanswered for PONG_GRACE: hold, confirmation ping")
    c2.reader.confirm_needed = False; c2.reader.last_confirm_at = t + 2.3
    c2.enqueue(cap(30, 1, False, t + 2.3))
    check(c2.poll(t + 2.4) == s.PROBE_CHECK, "held: nothing is written")
    c2.poll(t + 2.85)
    check(c2.reader.confirm_needed, "confirmation ping again after CONFIRM_INTERVAL")
    K.rx_segs += 1                        # an answer behind a lost pong: the client is reading
    check(c2.poll(t + 2.9) == 0.0 and c2.mode == s.FULL, "out-of-order answer: hold released")
    check(c2.reader.reply_gap == K.rx_inorder and c2.reader.sent_at is None, "lost ping forgotten, gap remembered")
    check(not c2.reader.delivered(), "while the gap lasts, pings count as undelivered")
    K.rx_inorder += 10
    check(c2.reader.delivered() and c2.reader.reply_gap is None, "gap filled: normal again")

    # ---- Loop 2: slow reader -> pacing ----------------------------------------
    c3 = s.ClientSession(WS(), "slow", sess); c3.loop = Clock(300.0)
    for i in range(1, 41):                # 4 msg/s for 10 s; a ping every N messages
        c3.loop.t = 300.0 + 0.25 * i
        c3.enqueue(cap(100 + i, 1, False, c3.loop.t))
        if c3.reader.sent_at is not None:  # earlier pings answered promptly
            c3.on_pong(done(), c3.reader.sent_at)
        assert c3.poll(c3.loop.t) == 0.0
        await c3.send_next()
        K.acked = K.written
    confirmed = c3.reader.confirmed_frames
    check(confirmed == 40 - N and c3.reader.sent_at is not None, "pings answered; the one on message 40 outstanding")
    t = c3.loop.t
    c3.poll(t + 0.01); c3.poll(t + 0.3)
    check(c3.mode == s.HOLD, "slow reader held")
    onset = c3.hold_onset
    w_confirmed, w_before = 300.0 + 0.25 * confirmed, 300.0 + 0.25 * 39
    check(abs(onset - (w_confirmed + w_before) / 2) < 1e-9,
          "stall timed from the middle of the possible pause interval")
    c3.loop.t = onset + 6.0               # library resumed after a 6 s stall
    c3.on_pong(done(), c3.reader.sent_at)
    check(c3.mode == s.PACED and abs(c3.pacer.rate - 2.0) < 0.01, f"paced at 12 msg / 6 s = {c3.pacer.rate:.2f} msg/s")
    debt = s.LIBRARY_RESUME + (c3.frames_sent - confirmed - 1)
    check(abs(c3.pacer.tokens - (1 - debt)) < 1e-9, f"waits for {debt} messages still queued on the client")
    now = c3.loop.t
    c3.enqueue(cap(200, 1, True, now))
    w = c3.poll(now)
    check(w is not None and w > 0, "final waits for a token")
    c3.pacer.tokens = 1.0; c3.pacer.tokens_at = now
    check(c3.poll(now) == 0.0, "final needs one token")
    await c3.send_next()
    c3.enqueue(cap(201, 1, False, now))
    c3.pacer.tokens = 1.0; c3.pacer.tokens_at = now
    check(c3.poll(now) > 0, "provisional needs a spare token")

    # ---- Loop 2: short stall = network delay ----------------------------------
    c4 = s.ClientSession(WS(), "busy", sess); c4.loop = Clock(400.0)
    for i in range(1, 17):
        c4.loop.t = 400.0 + 0.25 * i
        c4.enqueue(cap(300 + i, 1, False, c4.loop.t))
        if c4.reader.sent_at is not None and c4.frames_sent < 15:
            c4.on_pong(done(), c4.reader.sent_at)
        assert c4.poll(c4.loop.t) == 0.0
        await c4.send_next()
        K.acked = K.written
    t = c4.reader.sent_at
    c4.poll(t + 0.01); c4.poll(t + 0.3)
    check(c4.mode == s.HOLD, "pong late: held")
    c4.loop.t = t + 0.4
    stall = c4.loop.t - c4.hold_onset
    c4.on_pong(done(), c4.reader.sent_at)
    check(c4.mode == s.FULL, f"pong 0.4 s after the ping: 12 reads in {stall:.1f} s since the last "
          "confirmed one beat our sending rate, back to full delivery")

    # ---- Pacing retry ------------------------------------------------------------
    c3.loop.t = c3.pacer.since + s.PACED_RETRY_SECONDS + 1
    c3.poll(c3.loop.t)
    check(c3.mode == s.FULL and c3.pacer.retried, "stall-free for 60 s: full rate again")
    print("control loops: all checks passed")

asyncio.run(main())
