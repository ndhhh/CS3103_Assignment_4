"""A normally reading client: each caption and the ping it carries must
arrive in one TCP segment (read from the client's TCP_INFO, Linux only)."""
import asyncio, json, socket, struct, time
import websockets
from helpers import start_server
PORT = 9303
URI = f"ws://127.0.0.1:{PORT}"

def data_segs_in(ws):
    info = ws.transport.get_extra_info("socket").getsockopt(socket.IPPROTO_TCP, socket.TCP_INFO, 256)
    return struct.unpack_from("<I", info, 152)[0]

async def main():
    srv = start_server(PORT)
    try:
        cli = await websockets.connect(URI)
        await cli.send(json.dumps({"type": "CLIENT_HELLO", "client_id": 1, "session": "L1", "last_segment": 0,
                                   "last_revision": 0, "history_seconds": 30}))
        await cli.recv()
        base = data_segs_in(cli)
        async def reader():
            n = 0
            try:
                while True:
                    await asyncio.wait_for(cli.recv(), 1.5); n += 1
            except asyncio.TimeoutError:
                return n
        async def source():
            async with websockets.connect(URI) as src:
                await src.send(json.dumps({"type": "SOURCE_HELLO", "session": "L1"}))
                for seg in range(1, 21):
                    for rev in (1, 2, 3):
                        await src.send(json.dumps({"type": "CAPTION_UPDATE", "session": "L1", "segment": seg, "revision": rev,
                                                   "source_ts": time.time(), "final": rev == 3, "text": "y" * 60}))
                        await asyncio.sleep(0.25)
        got, _ = await asyncio.gather(reader(), source())
        segs = data_segs_in(cli) - base
        print(f"client read {got}/60 captions in {segs} data segments")
        assert got == 60 and segs <= got + 2, "pings are not sharing caption segments"
        print("cork check: all checks passed")
        await cli.close()
    finally:
        srv.terminate(); srv.wait()

asyncio.run(main())
