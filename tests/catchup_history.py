"""Catch-up HISTORY goes only to clients that joined with nothing to recover;
late and resumed connections get exactly one HISTORY. Every slow client gets
every final exactly once and never an older revision after a newer one."""
import asyncio, json, time
import websockets
from helpers import start_server
PORT, SEGMENTS = 9302, 40
URI = f"ws://127.0.0.1:{PORT}"

async def source(ready):
    await ready.wait()
    async with websockets.connect(URI) as ws:
        await ws.send(json.dumps({"type": "SOURCE_HELLO", "session": "L1"}))
        for seg in range(1, SEGMENTS + 1):
            for rev in (1, 2, 3):
                await ws.send(json.dumps({"type": "CAPTION_UPDATE", "session": "L1", "segment": seg, "revision": rev,
                                          "source_ts": time.time(), "final": rev == 3, "text": f"s{seg}r{rev}"}))
                await asyncio.sleep(0.05)
        await ws.send(json.dumps({"type": "SOURCE_DONE", "session": "L1"}))

async def slow_client(name, last_segment, delay, ready):
    await asyncio.sleep(delay)
    got = []
    async with websockets.connect(URI) as ws:
        await ws.send(json.dumps({"type": "CLIENT_HELLO", "client_id": name, "session": "L1",
                                  "last_segment": last_segment, "last_revision": 3 if last_segment else 0, "history_seconds": 30}))
        got.append(json.loads(await ws.recv()))
        if ready is not None:
            ready.set()
        while True:
            try:
                got.append(json.loads(await asyncio.wait_for(ws.recv(), 4)))
            except asyncio.TimeoutError:
                return name, last_segment, got
            await asyncio.sleep(0.2)          # reads 5 messages/s: slower than the stream

def check(name, last_segment, got):
    assert got[0]["type"] == "HISTORY", f"{name}: first message is not HISTORY"
    histories = [m for m in got if m["type"] == "HISTORY"]
    finals, highest = [], {}
    for m in got:
        for c in (m["items"] if m["type"] == "HISTORY" else [m]):
            if m["type"] == "HISTORY":
                assert c["final"], f"{name}: provisional caption inside HISTORY"
            assert c["revision"] > highest.get(c["segment"], 0), f"{name}: stale/duplicate {c['segment']}/{c['revision']}"
            highest[c["segment"]] = c["revision"]
            if c["final"]:
                finals.append(c["segment"])
    expected = list(range(last_segment + 1, SEGMENTS + 1))
    assert sorted(finals) == expected, f"{name}: finals {sorted(set(expected) - set(finals))} missing"
    assert finals == sorted(finals), f"{name}: finals out of order"
    return len(histories)

async def main():
    srv = start_server(PORT, capture=True)
    try:
        ready = asyncio.Event()
        clients = [slow_client("A-fresh", 0, 0.0, ready), slow_client("B-late", 0, 2.0, None),
                   slow_client("C-resumed", 3, 2.0, None)]
        results = await asyncio.gather(source(ready), *clients)
        counts = {name: check(name, last, got) for name, last, got in results[1:]}
        print("HISTORY messages per client:", counts)
        assert counts["A-fresh"] >= 2, "fresh slow client got no catch-up HISTORY"
        assert counts["B-late"] == 1 and counts["C-resumed"] == 1, "late/resumed client got an extra HISTORY"
        print("catch-up rules: all checks passed")
    finally:
        srv.terminate(); out = srv.communicate()[0]
        print("".join(l for l in out.splitlines(True) if "slow reader" in l), end="")

asyncio.run(main())
