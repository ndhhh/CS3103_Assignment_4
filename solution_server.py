#!/usr/bin/env python3
"""LiveRecall WebSocket server. Run with: python solution_server.py --port 8765."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import time
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed

from client_outbox import ClientOutbox, encode_message


@dataclass
class ClientState:
    ws: Any
    session: str = "L1"
    cursor: tuple[int, int] = (0, 0)
    last_update: tuple[int, int] = (0, 0)
    history_since: float = 0
    recovering: bool = False
    needs_history: bool = True
    outbox: ClientOutbox = field(default_factory=ClientOutbox)
    sender: asyncio.Task | None = None
    closing: asyncio.Task | None = None


CLIENTS: dict[Any, ClientState] = {}
# Server state saves captions with a dict, indexed by (session, segment)
# for future extension if multiple sessions are added.
SAVED_CAPTIONS: dict[tuple[str, int], Any] = {}

MAX_HISTORY = 90
MAX_MESSAGE_BYTES = 2**20
HISTORY_CHUNK_BYTES = 256 * 1024


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
    key = (msg["session"], msg["segment"])
    existing_caption = SAVED_CAPTIONS.get(key)
    accepted_caption = dict(msg)

    # Save new caption if it is not found in history, i.e. completely fresh
    if existing_caption is None:
        SAVED_CAPTIONS[key] = accepted_caption
        return accepted_caption
    # Ignore caption if there exists a finalised caption with same segment number
    if existing_caption.get("final") is True:
        return None
    # Ignore caption if there exists a more updated segment
    if msg["revision"] <= existing_caption.get("revision", 0):
        return None
    # Else save entire caption (a dict) in the server state
    SAVED_CAPTIONS[key] = accepted_caption
    return accepted_caption


def close_client(client, reason="Connection closed"):
    """Close a client, removing them from stored clients, as well as their outbox."""
    if client.closing is None:
        CLIENTS.pop(client.ws, None)
        client.outbox.close()
        client.closing = asyncio.create_task(client.ws.close(code=1000, reason=reason))
    return client.closing


async def publish_update(update):
    """Offers the updated caption to each client via their outbox to handle slow clients."""
    # Snapshot before cooperative yields, then encode once and share the
    # immutable packet across all client queues.
    message = encode_message(update)
    clients = tuple(CLIENTS.values())
    for index, client in enumerate(clients, 1):
        if client.recovering:
            continue
        # If client outbox cannot fit new caption, set it to recovery state
        if not client.outbox.offer(message):
            client.recovering = True
            client.history_since = time.time() - MAX_HISTORY
        if index % 256 == 0:
            await asyncio.sleep(0)


async def send_client_messages(client):
    """
    Depletes each client's outbox, updates recovery cursors after sends, 
    and retries history recovery (to catch up with server) when the queue drains.
    """
    sent = 0
    try:
        while True:
            if client.recovering and client.outbox.count == 0:
                client.recovering = not queue_recovery(client)
            message = await client.outbox.get()
            if message is None:
                break
            try:
                await client.ws.send(message.text)
                if message.position is not None:
                    client.last_update = max(client.last_update, message.position)
                    if message.final or message.key is None:
                        client.cursor = max(client.cursor, message.position)
            finally:
                client.outbox.complete(message)
            sent += 1
            if sent % 16 == 0:
                await asyncio.sleep(0)
    except ConnectionClosed:
        pass
    finally:
        close_client(client)


async def handle_source(ws, hello: dict[str, Any]) -> None:
    """
    Reads message from connected source and sends it to be
    updated in client state + offers to all clients' bounded outboxes.
    """
    async for raw in ws:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(msg, dict) or msg.get("type") != "CAPTION_UPDATE":
            continue
        if not validate_caption_update(msg) or msg["session"] != hello.get("session"):
            continue
        accepted_caption = upsert_caption(msg)
        if accepted_caption is not None:
            await publish_update(accepted_caption)


async def prune_history(max_history: int = MAX_HISTORY):
    """Remove captions outside the server's retention window."""
    global SAVED_CAPTIONS # Reference to the global state dict.
    current_time = time.time()
    SAVED_CAPTIONS = {
        key: caption for key, caption in SAVED_CAPTIONS.items()
        if 0 <= current_time - caption["source_ts"] <= max_history
    }


async def prune_history_periodically() -> None:
    """
    Prune expired captions every second, including while the source is idle.

    Update: This was previously only run everytime the server got a new caption from source,
    which led to stale captions being kept continuously when source is idle.
    """
    while True:
        await prune_history()
        await asyncio.sleep(1)


def validate_client_hello(hello):
    """Check for Client HELLO fields."""
    segment = hello.get("last_segment")
    revision = hello.get("last_revision")
    seconds = hello.get("history_seconds")
    session = hello.get("session")
    return (
        isinstance(session, str) and bool(session.strip())
        and type(segment) is int and segment >= 0
        and type(revision) is int and revision >= 0
        and (segment == 0) == (revision == 0)
        and type(seconds) in (int, float) and 0 <= seconds <= 60
    )


def queue_history(client, items):
    """
    Offer client queue a HISTORY packet without blocking
    and resets its `needs_history` status if successful.

    If outbox is full, failure is reported (for client to recover).
    """
    if not client.outbox.offer(encode_message({"type": "HISTORY", "items": items})):
        return False
    client.needs_history = False
    return True


def queue_recovery(client):
    """
    Rebuild client's missing recent finalised and 
    newer active provisional captions from server state.

    Return False if unable to be queued.
    """
    now = time.time()
    cursor = client.cursor
    # Exclude expired/future-dated entries before sorting; recovery only needs
    # the retained window and this path may run repeatedly for slow clients.
    captions = sorted(
        (c for c in SAVED_CAPTIONS.values()
         if c["session"] == client.session and 0 <= now - c["source_ts"] <= MAX_HISTORY),
        key=lambda c: c["source_ts"],
    )
    items = []
    size = 32
    for caption in captions:
        if (not caption["final"] or caption["source_ts"] < client.history_since
                or not 0 <= now - caption["source_ts"] <= MAX_HISTORY):
            continue
        if (caption["segment"], caption["revision"]) <= cursor:
            continue
        message = encode_message(caption)
        item_size = message.size + 1
        if item_size + 32 > MAX_MESSAGE_BYTES:
            if (items or client.needs_history) and not queue_history(client, items):
                return False
            items = []
            size = 32
            if not client.outbox.offer(message):
                return False
            continue
        if items and size + item_size > HISTORY_CHUNK_BYTES:
            if not queue_history(client, items):
                return False
            items = []
            size = 32
        items.append(caption)
        size += item_size

    if items or client.needs_history:
        if not queue_history(client, items):
            return False

    for caption in captions:
        if (not caption["final"] and (caption["segment"], caption["revision"]) > client.last_update
                and 0 <= now - caption["source_ts"] <= MAX_HISTORY):
            if not client.outbox.offer(encode_message(caption)):
                return False
    return True


async def handle_client(ws, hello):
    """Handles a connection from a client (be it old or new)."""
    # Do nothing if client HELLO is malformed
    if not validate_client_hello(hello):
        await ws.close(code=1008, reason="Invalid client hello")
        return

    # Cursor keeps track of the client's state: 
    # its last segment and revision
    cursor = (hello["last_segment"], hello["last_revision"])
    client = ClientState(ws, session=hello["session"], cursor=cursor, last_update=cursor,
                         history_since=time.time() - hello["history_seconds"])
    # Apply bounded provisional coalescing uniformly; no client-supplied slow
    # flag or simulator behavior is needed to protect a lagging reader.
    client.outbox.set_provisional_coalescing(True)
    client.recovering = not queue_recovery(client)
    # No await between taking the history snapshot and subscribing to live updates.
    CLIENTS[ws] = client
    client.sender = asyncio.create_task(send_client_messages(client))
    try:
        # Keep client websocket open.
        async for raw in ws:
            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                continue
    finally:
        # Close client when it is disconnected.
        await close_client(client)
        client.sender.cancel()
        with suppress(asyncio.CancelledError):
            await client.sender


async def handler(ws) -> None:
    """
    Top level handler objects for all requests (client, source HELLO).
    Respective requests are routed to their specific (client or source) handlers.
    """
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=5)
        hello = json.loads(raw)
        # Ignore malformed/non-HELLO messages
        if not isinstance(hello, dict):
            await ws.close(code=1008, reason="Expected a hello object")
            return
        # Route each HELLO to their respective handlers
        if hello.get("type") == "SOURCE_HELLO":
            await handle_source(ws, hello)
        elif hello.get("type") == "CLIENT_HELLO":
            await handle_client(ws, hello)
        else:
            await ws.close(code=1008, reason="Expected SOURCE_HELLO or CLIENT_HELLO")
    except (asyncio.TimeoutError, json.JSONDecodeError):
        await ws.close(code=1008, reason="Expected a valid hello message")
    except ConnectionClosed:
        pass


async def main_async(port: int) -> None:
    """Start the WebSocket server on all interfaces and keep it running."""
    async with websockets.serve(
        handler,
        "0.0.0.0",
        port,
        max_size=MAX_MESSAGE_BYTES,
        write_limit=32 * 1024,
        close_timeout=1,
        ping_interval=20,
        ping_timeout=None,
    ):
        prune_task = asyncio.create_task(prune_history_periodically())
        try:
            print(f"LiveRecall listening on ws://0.0.0.0:{port}", flush=True)
            await asyncio.Future()
        finally:
            for client in tuple(CLIENTS.values()):
                close_client(client, "Server stopping")
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
