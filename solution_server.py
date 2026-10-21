#!/usr/bin/env python3
"""
CS3103 Assignment 4 - LiveRecall naive working baseline.

This starter is intentionally useful but incomplete. It provides the required
CLI and a basic WebSocket broadcaster so that caption_source.py -> server ->
browser/load clients works on day one.

It DOES NOT implement the graded networking mechanisms:
  - revision-state management or deduplication
  - recent-history recovery or late-join recovery
  - bounded per-client application queues
  - slow-client isolation
  - semantic coalescing / backpressure
  - constrained-bandwidth overload handling

Required evaluator command:
    python solution_server.py --port 8765

Teams should replace or substantially extend the implementation while keeping
this command-line interface and the protocol in the assignment specification.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from typing import Any

import websockets

"""
Cmd Line for setting up multipass + mounting files to be run

```bash
multipass launch --name liverecall --cpus 2 --memory 4G --disk 15G
multipass shell liverecall
mkdir Assignment4
exit
multipass set local.privileged-mounts=true
multipass mount <Absolute path to assignment files> liverecall:/home/ubuntu/Assignment4
multipass shell liverecall
```

Inside Ubuntu:

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip iproute2
python3 -m venv ~/liverecall-venv
source ~/liverecall-venv/bin/activate
pip install -r requirements.txt
```

When finished, exit the VM shell and run on the host:

```bash
multipass stop liverecall
"""

# Naive global set of currently connected student clients.
# There is deliberately no per-client queue or recovery state here.
CLIENTS: set[Any] = set()

# Naive global set of disconnected student clients.
# May need to refer to this for clients that reconnect later on.
CLIENTS_DISCONNECTED: set[Any] = set()

# Server stores the set of captions that have
SAVED_CAPTIONS: set[Any] = set()


async def broadcast_naively(raw_message: str) -> None:
    """Send one message directly to every connected client.

    This is intentionally a naive baseline. The source handler waits for all
    sends to complete. Under pressure, this design may accumulate delay and is
    not the target architecture for the assignment.
    """
    # If no clients currently, then dont send anything
    if not CLIENTS:
        return

    snapshot = list(CLIENTS)
    results = await asyncio.gather(
        *(client.send(raw_message) for client in snapshot),
        return_exceptions=True,
    )
    for client, result in zip(snapshot, results):
        if isinstance(result, Exception):
            CLIENTS.discard(client)


async def handle_source(ws, hello: dict[str, Any]) -> None:
    """Read messages from a caption source and relay caption updates to clients.

    Invalid JSON and unsupported message types are ignored. SOURCE_DONE is
    logged but not forwarded, and the source is logged as disconnected on exit.
    """
    session = hello.get("session")
    print(f"source connected: session={session}", flush=True)

    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            msg_type = msg.get("type")
            if msg_type == "CAPTION_UPDATE":
                # Baseline behavior: forward exactly what was received.
                # No history, state, validation, coalescing, or bounded queues.
                await broadcast_naively(raw)
            elif msg_type == "SOURCE_DONE":
                # Test-control message. The specification does not require it
                # to be forwarded to clients.
                print("source finished", flush=True)
            else:
                # Forward compatibility: ignore fields/messages this baseline
                # doesn't understand rather than crashing.
                continue
    finally:
        print("source disconnected", flush=True)


async def handle_client(ws, hello: dict[str, Any]) -> None:
    """
    Initialise a student client connection and keep it open until disconnect.

    Server checks if client is a new connection with CLIENT_HELLO(last_segment=0, last_revision=0, history_seconds=30)
    or a reconnected client with CLIENT_HELLO(last_segment=X, last_revision=X, history_seconds=30)
    """
    # TODO: Verify the exact json structure for the below 2 lines
    last_segment = hello.get("last_segment")
    last_revision = hello.get("last_revision")
    if last_segment == 0 and last_revision == 0:
        # If both fields are zero, this is a new client
        await handle_new_client(ws, hello)
    else:
        # If non-zero, this is a reconnected client
        await handle_reconnected_client(ws, hello)


async def handle_new_client(ws, hello: dict[str, Any]) -> None:
    """
    Initialize a student client connection and keep it open until disconnect.

    The baseline sends an empty HISTORY response, tracks the connection in
    CLIENTS for broadcasts, and removes it when the connection ends.
    """
    client_id = hello.get("client_id")

    # The baseline has no stored recovery state. Sending an empty HISTORY makes
    # the connection behavior visible and keeps the starter easy to inspect.
    # Students must implement correct HISTORY semantics for the assignment.
    await ws.send(json.dumps({"type": "HISTORY", "items": []}))

    CLIENTS.add(ws)
    print(f"client connected: id={client_id}; clients={len(CLIENTS)}", flush=True)
    try:
        # Student clients normally don't send further messages after hello.
        # Keep the WebSocket open until the client disconnects.
        async for _ in ws:
            pass
    finally:
        CLIENTS.discard(ws)
        print(f"client disconnected: id={client_id}; clients={len(CLIENTS)}", flush=True)


async def handle_reconnected_client(ws, hello: dict[str, Any]) -> None:
    """
    Accepts a connection from a previously connected client.

    Previously connected clients give a CLIENT_HELLO(last_segment=X, last_revision=X, history_seconds=30)
    Server should pick up on this and give the most recent captions in the past `history_seconds`
    """
    # TODO: Filter through the server's saved captions and 
    # give only the latest updates in the last history_seconds
    current_time = time.time()
    last_segment = hello.get("last_segment")
    last_revision = hello.get("last_revision")
    history_seconds = hello.get('history_seconds')
    latest_captions = [
        c for c in SAVED_CAPTIONS if 
        (current_time - c.get('source_ts') < history_seconds) and
            ((c.get('segment') > last_segment) or 
             (c.get('segment') == last_segment and c.get('revision') > last_revision))
    ]

    await ws.send(json.dumps({"type": "HISTORY", "items": [latest_captions]}))


async def handler(ws) -> None:
    """Read and validate the first WebSocket message, then route by hello type.

    Connections that do not send valid JSON within five seconds, or send an
    unsupported hello type, are closed with policy-violation status.
    """
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=5)
        hello = json.loads(raw)
    except (asyncio.TimeoutError, json.JSONDecodeError):
        await ws.close(code=1008, reason="Expected a valid hello message")
        return

    msg_type = hello.get("type")
    if msg_type == "SOURCE_HELLO":
        await handle_source(ws, hello)
    elif msg_type == "CLIENT_HELLO":
        await handle_client(ws, hello)
    else:
        await ws.close(code=1008, reason="Expected SOURCE_HELLO or CLIENT_HELLO")


async def main_async(port: int) -> None:
    """Start the WebSocket server on all interfaces and keep it running."""
    async with websockets.serve(
        handler,
        "0.0.0.0",
        port,
        max_size=2**20,
        ping_interval=20,
        ping_timeout=20,
    ):
        print(f"LiveRecall baseline listening on ws://0.0.0.0:{port}", flush=True)
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
