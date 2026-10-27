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
import math
import time
from contextlib import suppress
from typing import Any

import websockets


# Naive global set of currently connected student clients.
# There is deliberately no per-client queue or recovery state here.
CLIENTS: set[Any] = set()

# Naive global set of disconnected student clients.
# May need to refer to this for clients that reconnect later on.
# TODO: Decide if this is necessary
CLIENTS_DISCONNECTED: set[Any] = set()

# Server stores the set of captions that are finalised +
# the latest segment and revision
SAVED_CAPTIONS: list[Any] = []

MAX_HISTORY = 90


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


def upsert_caption(msg: dict[str, Any]) -> dict[str, Any] | None:
    """Store and return a newer caption revision, or reject stale/duplicate state."""
    existing_index = next(
        (
            index
            for index, caption in enumerate(SAVED_CAPTIONS)
            if caption.get("session") == msg["session"]
            and caption.get("segment") == msg["segment"]
        ),
        None,
    )

    accepted_caption = dict(msg)
    if existing_index is None:
        SAVED_CAPTIONS.append(accepted_caption)
        return accepted_caption

    existing_caption = SAVED_CAPTIONS[existing_index]
    if existing_caption.get("final") is True:
        return None
    if msg["revision"] <= existing_caption.get("revision", 0):
        return None

    SAVED_CAPTIONS[existing_index] = accepted_caption
    return accepted_caption


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
    """
    Read messages from a caption source and relay caption updates to clients.

    Invalid JSON and unsupported message types are ignored. SOURCE_DONE is
    logged but not forwarded, and the source is logged as disconnected on exit.
    """
    session = hello.get("session")
    print(f"source connected: session={session}", flush=True)

    try:
        # Keepo listening to source caption updates
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(msg, dict):
                continue

            msg_type = msg.get("type")
            if msg_type == "CAPTION_UPDATE":
                if not validate_caption_update(msg):
                    continue

                accepted_caption = upsert_caption(msg)
                if accepted_caption is None:
                    continue

                await broadcast_naively(json.dumps(accepted_caption))
                
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


async def prune_history(max_history: int = MAX_HISTORY):
    """
    Prunes captions in SAVED_CAPTIONS that are older than `max_history`.
    
    This is to minimise space and satisfy client recovery. 
    It cannot keep an unbounded lecture transcript.
    """
    current_time = time.time()
    # Take only captions that existed in the last `max_history` seconds
    pruned_captions = [c for c in SAVED_CAPTIONS if 0 <= current_time - c.get("source_ts") <= max_history]
    SAVED_CAPTIONS[:] = pruned_captions # Overwrite


async def prune_history_periodically() -> None:
    """Prune expired captions every second, including while the source is idle."""
    while True:
        await prune_history()
        await asyncio.sleep(1)


async def handle_client(ws, hello: dict[str, Any]) -> None:
    """
    Initialise a student client connection and keep it open until disconnect.

    Server checks if client is a new connection with CLIENT_HELLO(last_segment=0, last_revision=0, history_seconds=30)
    or a reconnected client with CLIENT_HELLO(last_segment=X, last_revision=X, history_seconds=30)
    """
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
    history_seconds = hello.get("history_seconds")
    current_time = time.time()

    # Send all captions within the last `history_seconds` that are final + latest provisionary caption
    latest_captions = [
        c for c in SAVED_CAPTIONS if 
        (0 <= current_time - c.get('source_ts') < history_seconds and
        c.get("final") or SAVED_CAPTIONS.index(c) == len(SAVED_CAPTIONS) - 1)
    ]

    sorted_captions = sorted(latest_captions, key=lambda x: x.get("source_ts"))
    # The baseline has no stored recovery state. Sending an empty HISTORY makes
    # the connection behavior visible and keeps the starter easy to inspect.
    # Students must implement correct HISTORY semantics for the assignment.
    await ws.send(json.dumps({"type": "HISTORY", "items": sorted_captions}))
    await add_client(ws, client_id)


async def handle_reconnected_client(ws, hello: dict[str, Any]) -> None:
    """
    Accepts a connection from a previously connected client.

    Previously connected clients give a CLIENT_HELLO(last_segment=X, last_revision=X, history_seconds=30)
    Server should pick up on this and give the most recent captions in the past `history_seconds`
    """
    client_id = hello.get("client_id")
    current_time = time.time()
    last_segment = hello.get("last_segment")
    last_revision = hello.get("last_revision")
    history_seconds = hello.get('history_seconds')

    """
    Filter through the list of captions stored in server 
    and only send those that are most updated and missed by the client
    when they were disconnected.
    """
    latest_captions = [
        c for c in SAVED_CAPTIONS if 
        # caption was within the last history_seconds and
        (0 <= current_time - c.get('source_ts') < history_seconds) and
            # this is a new segment that client does not have or
            ((c.get('segment') > last_segment) or 
             # it is an update to the revision the client had
             (c.get('segment') == last_segment and c.get('revision') > last_revision))
    ]
    sorted_captions = sorted(latest_captions, key=lambda x: x.get("source_ts"))
    await ws.send(json.dumps({"type": "HISTORY", "items": sorted_captions}))
    await add_client(ws, client_id)


async def add_client(ws, client_id):
    # Add to list of connected clients
    CLIENTS.add(ws)
    print(f"client connected: id={client_id}; clients={len(CLIENTS)}", flush=True)
    try:
        # Student clients normally don't send further messages after hello.
        # Keep the WebSocket open until the client disconnects.
        async for _ in ws:
            pass
    finally:
        # When they disconnect, remove from list.
        CLIENTS.discard(ws)
        print(f"client disconnected: id={client_id}; clients={len(CLIENTS)}", flush=True)


async def handler(ws) -> None:
    """
    Read and validate the FIRST WebSocket message, then route by hello type.

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
        prune_task = asyncio.create_task(prune_history_periodically())
        try:
            print(f"LiveRecall baseline listening on ws://0.0.0.0:{port}", flush=True)
            await asyncio.Future()
        finally:
            prune_task.cancel()
            with suppress(asyncio.CancelledError):
                await prune_task


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
