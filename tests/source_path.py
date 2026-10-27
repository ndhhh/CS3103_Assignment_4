"""End to end: the source sends text, no-session and binary updates; a live
client gets every revision once, a late client gets HISTORY plus the current
provisional, and a new source starting at segment 1 begins a new lecture."""
import asyncio, json, time
import websockets
from helpers import start_server
PORT = 9301
URI = f"ws://127.0.0.1:{PORT}"
HELLO = {"type": "CLIENT_HELLO", "session": "L1", "last_segment": 0, "last_revision": 0, "history_seconds": 30}

def upd(seg, rev, final, **extra):
    return {"type": "CAPTION_UPDATE", "session": "L1", "segment": seg, "revision": rev,
            "source_ts": time.time(), "final": final, "text": f"s{seg} r{rev}", **extra}

async def main():
    srv = start_server(PORT)
    try:
        live = await websockets.connect(URI)
        await live.send(json.dumps({**HELLO, "client_id": 1}))
        assert json.loads(await live.recv()) == {"type": "HISTORY", "items": []}
        src = await websockets.connect(URI)
        await src.send(json.dumps({"type": "SOURCE_HELLO", "session": "L1"}))
        await src.send(json.dumps(upd(1, 1, False, phase="normal")))
        m = upd(1, 2, True); del m["session"]; await src.send(json.dumps(m))          # no session field
        await src.send(json.dumps(upd(2, 1, False)).encode())                         # binary frame
        await src.send(json.dumps(upd(1, 1, False)))                                  # stale: must not be forwarded
        got = [json.loads(await asyncio.wait_for(live.recv(), 2)) for _ in range(3)]
        assert [(g["segment"], g["revision"]) for g in got] == [(1, 1), (1, 2), (2, 1)], got
        assert all(g["session"] == "L1" for g in got) and got[0]["phase"] == "normal", got
        try:
            extra = await asyncio.wait_for(live.recv(), 0.5); raise AssertionError(f"stale update forwarded: {extra}")
        except asyncio.TimeoutError: pass
        late = await websockets.connect(URI)
        await late.send(json.dumps({**HELLO, "client_id": 2}))
        hist = json.loads(await late.recv())
        assert [(i["segment"], i["final"]) for i in hist["items"]] == [(1, True)], hist
        cur = json.loads(await asyncio.wait_for(late.recv(), 2))
        assert (cur["segment"], cur["revision"], cur["final"]) == (2, 1, False), cur
        print("source path + late join: all checks passed")
    finally:
        srv.terminate(); srv.wait()

async def restart():
    srv = start_server(PORT)
    try:
        cli = await websockets.connect(URI)
        await cli.send(json.dumps({**HELLO, "client_id": 3}))
        await cli.recv()
        for run in range(2):
            src = await websockets.connect(URI)
            await src.send(json.dumps({"type": "SOURCE_HELLO", "session": "L1"}))
            for seg in (1, 2):
                await src.send(json.dumps({**upd(seg, 1, True), "text": f"run{run}"}))
            await src.close()
        got = [json.loads(await asyncio.wait_for(cli.recv(), 2))["text"] for _ in range(4)]
        assert got == ["run0", "run0", "run1", "run1"], got
        print("source restart: all checks passed")
    finally:
        srv.terminate(); srv.wait()

asyncio.run(main())
asyncio.run(restart())
