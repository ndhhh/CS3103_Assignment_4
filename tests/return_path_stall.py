"""A fast client behind a TCP-terminating proxy whose client->server bytes
are held back for 3 s must still receive every final, without duplicates; a
genuinely slow reader must be detected. The proxy acknowledges the server's
data itself, so to the server the held pong looks exactly like a paused
library: being paced is the known, bounded cost (provisionals only, until the
full-rate retry) and is reported, not asserted. A pong lost in the kernel is
covered by control_loops.py (answers arrive out of order)."""
import asyncio, json, time
import websockets
from helpers import start_server
SERVER, PROXY = 9304, 9305
blackout = asyncio.Event()       # set while client->server bytes are held back

async def proxy_conn(c_reader, c_writer):
    s_reader, s_writer = await asyncio.open_connection("127.0.0.1", SERVER)
    async def up():  # client -> server, held back during the blackout
        held = []
        try:
            while True:
                try:
                    data = await asyncio.wait_for(c_reader.read(65536), 0.05)
                except asyncio.TimeoutError:
                    data = None
                if data == b"":
                    break
                if data:
                    held.append(data)
                if held and not blackout.is_set():
                    for chunk in held:
                        s_writer.write(chunk)
                    held.clear()
                    await s_writer.drain()
        finally:
            s_writer.close()
    async def down():  # server -> client, never held
        try:
            while data := await s_reader.read(65536):
                c_writer.write(data)
                await c_writer.drain()
        except ConnectionError:
            pass
        finally:
            c_writer.close()
    await asyncio.gather(up(), down())

async def source():
    async with websockets.connect(f"ws://127.0.0.1:{SERVER}") as ws:
        await ws.send(json.dumps({"type": "SOURCE_HELLO", "session": "L1"}))
        for seg in range(1, 41):                       # 4 messages/s, like caption_source.py
            for rev in (1, 2, 3):
                await ws.send(json.dumps({"type": "CAPTION_UPDATE", "session": "L1", "segment": seg, "revision": rev,
                                          "source_ts": time.time(), "final": rev == 3, "text": f"s{seg}r{rev}"}))
                await asyncio.sleep(0.2 if rev < 3 else 0.35)

async def client(name, port, read_delay, ready):
    got = []
    async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
        await ws.send(json.dumps({"type": "CLIENT_HELLO", "client_id": name, "session": "L1",
                                  "last_segment": 0, "last_revision": 0, "history_seconds": 30}))
        got.append(json.loads(await ws.recv()))
        ready.set()
        while True:
            try:   # like the instructor's client, it never gives up while connected
                got.append(json.loads(await asyncio.wait_for(ws.recv(), 12)))
            except asyncio.TimeoutError:
                return got
            await asyncio.sleep(read_delay)

async def blackout_schedule():
    await asyncio.sleep(6)
    blackout.set()
    await asyncio.sleep(3)
    blackout.clear()

async def main():
    srv = start_server(SERVER, capture=True)
    proxy = await asyncio.start_server(proxy_conn, "127.0.0.1", PROXY)
    try:
        r1, r2 = asyncio.Event(), asyncio.Event()
        fast = asyncio.create_task(client("fast-behind-proxy", PROXY, 0.0, r1))
        slow = asyncio.create_task(client("slow-reader", SERVER, 0.5, r2))
        await r1.wait(); await r2.wait()
        await asyncio.gather(source(), blackout_schedule())
        fast_got = await fast
        await slow
    finally:
        proxy.close(); srv.terminate(); out = srv.communicate()[0]
    decisions = [l for l in out.splitlines() if "slow reader" in l or "full rate" in l or "network delay" in l]
    print("\n".join(decisions))
    updates = [(c["segment"], c["revision"], c["final"]) for m in fast_got
               for c in (m["items"] if m["type"] == "HISTORY" else [m])]
    finals = {u[0] for u in updates if u[2]}
    print(f"fast client behind proxy: {len(updates)}/120 revisions, finals {len(finals)}/40, "
          f"duplicates={len(updates) - len(set(updates))}")
    assert len(finals) == 40, "fast client lost finals"
    assert len(updates) == len(set(updates)), "duplicates"
    if any("fast-behind-proxy" in l and l.endswith("pacing") for l in decisions):
        print("note: proxied fast client paced (known limitation, provisionals only)")
    assert any("slow-reader" in l and l.endswith("pacing") for l in decisions), "genuine slow reader not detected"
    print("return-path stall check: all checks passed")

asyncio.run(main())
