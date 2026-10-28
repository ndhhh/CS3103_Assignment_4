import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass


@dataclass(frozen=True)
class OutboundMessage:
    text: str
    size: int
    key: tuple[str, int] | None = None
    final: bool = False
    position: tuple[int, int] | None = None


@dataclass
class _QueuedMessage:
    message: OutboundMessage
    queued_at: float


def encode_message(message):
    """Encodes a JSON message into an `OutboundMessage`."""
    text = json.dumps(message, ensure_ascii=False, separators=(",", ":"))
    key = None
    position = None
    if message.get("type") == "CAPTION_UPDATE":
        key = (message["session"], message["segment"])
        position = (message["segment"], message["revision"])
    elif message.get("type") == "HISTORY" and message.get("items"):
        position = max((item["segment"], item["revision"]) for item in message["items"])
    return OutboundMessage(text, len(text.encode("utf-8")), key, message.get("final", False), position)


class ClientOutbox:
    """Main class; representation of a queue for a client to receive from."""
    # Time before provisional cpation is discarded
    STALE_PROVISIONAL_SECONDS = 0.5
    # Time outbox waits before sending a provisional caption
    # This allows newer revision to replace it
    PROVISIONAL_COALESCE_SECONDS = 0.45

    def __init__(self, max_items=128, max_bytes=2 * 1024 * 1024):
        # Default cap of 128 captions and 2 MiB of data
        self.max_items = max_items
        self.max_bytes = max_bytes
        self.pending: deque[_QueuedMessage] = deque() # Main (de)queue object
        # Fast lookup avoids rebuilding the bounded queue for each slow-reader update.
        self._pending_provisional: dict[tuple[str, int], _QueuedMessage] = {}
        self.ready = asyncio.Event()
        self.count = 0
        self.bytes = 0
        self.closed = False # Is client socket closed
        self.pressured = False
        # Coalescing mode keeps only the newest queued provisional per segment
        # and prioritizes grouped finals for every client.
        self.coalesce_provisionals = False
        # Tracks when get() passes packet to be sent so in-flight data 
        # is still considered in pressure/size accounting
        self.sending_since = None

    def set_provisional_coalescing(self, enabled):
        # Compact the current backlog when the uniform coalescing policy starts.
        self.coalesce_provisionals = enabled
        if enabled and self.pending:
            self._compact_pending(now=time.monotonic())

    def offer(self, message):
        if self.closed:
            return False

        now = time.monotonic()
        oldest = self.pending[0].queued_at if self.pending else now
        if self.sending_since is not None:
            oldest = min(oldest, self.sending_since)
        age = now - oldest

        was_pressured = self.pressured
        if self.count >= self.max_items // 2 or self.bytes >= self.max_bytes // 2 or age >= 0.25:
            self.pressured = True
        elif self.count <= self.max_items // 4 and self.bytes <= self.max_bytes // 4 and age < 0.1:
            self.pressured = False

        if self.pressured and not was_pressured and not self.coalesce_provisionals:
            # Compact once as the queue crosses its high-water mark, then use
            # the keyed fast path until pressure clears or a hard cap is hit.
            self._compact_pending(now=now)

        if (self.coalesce_provisionals or self.pressured) and message.key is not None:
            queued = self._pending_provisional.get(message.key)
            if queued is not None:
                # Replace a queued provisional in-place; finals also supersede
                # that segment's obsolete provisional packet.
                new_bytes = self.bytes - queued.message.size + message.size
                if new_bytes <= self.max_bytes:
                    queued.message = message
                    queued.queued_at = now
                    self.bytes = new_bytes
                    if message.final:
                        self._pending_provisional.pop(message.key, None)
                    return True

        if self.coalesce_provisionals or self.pressured:
            # Coalescing above handles repeated segment updates in O(1). Only
            # rebuild the queue when an actual item or byte cap is reached.
            if self.count + 1 > self.max_items or self.bytes + message.size > self.max_bytes:
                return self._compact_pending(message, now)
            self._append(message, now)
            return True

        if (self.count + 1 > self.max_items
                or self.bytes + message.size > self.max_bytes):
            return self._compact_pending(message, now)

        self._append(message, now)
        return True

    def _append(self, message, now):
        queued = _QueuedMessage(message, now)
        self.pending.append(queued)
        self.count += 1
        self.bytes += message.size
        if message.key is not None and not message.final:
            self._pending_provisional[message.key] = queued
        self.ready.set()

    def _compact_pending(self, message=None, now=None):
        """
        Compacts the pending queue down.

        Removes obsolete provsional updates for the same session + segment, keeping the newest queued state.
        This does not affect finalised captions waiting to be sent.
        """
        if now is None:
            now = time.monotonic()
        pending = list(self.pending)
        if message is not None:
            pending.append(_QueuedMessage(message, now))
        kept = []
        seen = set()
        for queued in reversed(pending):
            entry = queued.message
            if entry.key is not None:
                if not entry.final and entry.key in seen:
                    continue
                seen.add(entry.key)
            kept.append(queued)

        # Keep the message being sent charged until complete() is called.
        count = self.count - len(self.pending) + len(kept)
        size = self.bytes - sum(queued.message.size for queued in self.pending)
        size += sum(queued.message.size for queued in kept)
        if count > self.max_items or size > self.max_bytes:
            return False

        self.pending = deque(reversed(kept))
        self.count = count
        self.bytes = size
        self._pending_provisional = {
            queued.message.key: queued
            for queued in self.pending
            if queued.message.key is not None and not queued.message.final
        }
        self.ready.set()
        return True

    async def get(self):
        """
        Wait until packet(s) arrives or the outbox closes
        then return the next packet for sending.
        """
        while True:
            while not self.pending:
                if self.closed:
                    return None
                self.ready.clear()
                await self.ready.wait()
            if self.coalesce_provisionals or self.pressured:
                # Delayed provisional packets are superseded state, not history;
                # discard them so they cannot make newer captions wait behind them.
                self._discard_stale_provisionals(time.monotonic())
                if not self.pending:
                    continue
            queued = self.pending[0]
            if (self.coalesce_provisionals and queued.message.key is not None
                    and not queued.message.final):
                # Let near-term revisions replace this queued state before it
                # reaches the socket; the final then bypasses this delay.
                remaining = (
                    queued.queued_at + self.PROVISIONAL_COALESCE_SECONDS - time.monotonic()
                )
                if remaining > 0:
                    await asyncio.sleep(remaining)
                    continue
            break
        if (self.coalesce_provisionals or self.pressured
                or time.monotonic() - self.pending[0].queued_at >= 0.25):
            self._batch_finals()
        queued = self.pending.popleft()
        message = queued.message
        if message.key is not None and self._pending_provisional.get(message.key) is queued:
            self._pending_provisional.pop(message.key, None)
        # Keep packet to be sent marked as in-flight until complete() 
        # is called after the sending attempt.
        self.sending_since = time.monotonic()
        return message

    def _discard_stale_provisionals(self, now):
        cutoff = now - self.STALE_PROVISIONAL_SECONDS
        retained = deque()
        for queued in self.pending:
            message = queued.message
            if (message.key is not None and not message.final
                    and queued.queued_at < cutoff):
                self.count -= 1
                self.bytes -= message.size
                if self._pending_provisional.get(message.key) is queued:
                    self._pending_provisional.pop(message.key, None)
            else:
                retained.append(queued)
        self.pending = retained

    def _batch_finals(self):
        """
        Replace queued final caption messages with one bounded HISTORY packet.
        """
        messages = []
        selected_indices = set()
        size = len('{"type":"HISTORY","items":[]}'.encode("utf-8"))
        for index, queued in enumerate(self.pending):
            message = queued.message
            if not message.final or message.key is None:
                continue
            # A larger item cap amortizes socket-send overhead for slow readers;
            # the independent byte cap below still bounds each HISTORY packet.
            if len(messages) == 128:
                break
            next_size = size + message.size + bool(messages)
            if next_size > 256 * 1024:
                break
            messages.append(message)
            selected_indices.add(index)
            size = next_size
        if len(messages) == 1 and (self.coalesce_provisionals or self.pressured):
            # Send a lone final ahead of provisional messages rather than
            # waiting for another final to form a batch.
            index = next(iter(selected_indices))
            entry = self.pending[index]
            del self.pending[index]
            self.pending.appendleft(entry)
            return
        if len(messages) < 2:
            return
        total_bytes = self.bytes - sum(message.size for message in messages) + size
        if total_bytes > self.max_bytes:
            return

        text = '{"type":"HISTORY","items":[' + ','.join(message.text for message in messages) + ']}'
        position = max(message.position for message in messages)
        batch = OutboundMessage(text, size, position=position)
        queued_at = min(self.pending[index].queued_at for index in selected_indices)
        # Let finals bypass queued provisional messages while preserving the
        # newest provisional state in the outbox.
        retained = [
            item for index, item in enumerate(self.pending)
            if index not in selected_indices
        ]
        self.pending = deque([_QueuedMessage(batch, queued_at), *retained])
        self.count -= len(messages) - 1
        self.bytes = total_bytes

    def complete(self, message):
        self.count -= 1
        self.bytes -= message.size
        self.sending_since = None

    def close(self):
        self.closed = True
        self.count -= len(self.pending)
        self.bytes -= sum(queued.message.size for queued in self.pending)
        self.pending.clear()
        self._pending_provisional.clear()
        self.ready.set()
