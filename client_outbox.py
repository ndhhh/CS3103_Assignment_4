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


def encode_message(message):
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
    def __init__(self, max_items=128, max_bytes=2 * 1024 * 1024):
        self.max_items = max_items
        self.max_bytes = max_bytes
        self.pending = deque()
        self.ready = asyncio.Event()
        self.count = 0
        self.bytes = 0
        self.closed = False
        self.pressured = False
        self.sending_since = None

    def offer(self, message):
        if self.closed:
            return False

        now = time.monotonic()
        oldest = self.pending[0][1] if self.pending else now
        if self.sending_since is not None:
            oldest = min(oldest, self.sending_since)
        age = now - oldest

        if self.count >= self.max_items // 2 or self.bytes >= self.max_bytes // 2 or age >= 0.25:
            self.pressured = True
        elif self.count <= self.max_items // 4 and self.bytes <= self.max_bytes // 4 and age < 0.1:
            self.pressured = False

        if self.pressured or self.count + 1 > self.max_items or self.bytes + message.size > self.max_bytes:
            return self._compact_pending(message, now)

        self.pending.append((message, now))
        self.count += 1
        self.bytes += message.size
        self.ready.set()
        return True

    def _compact_pending(self, message, now):
        pending = [*self.pending, (message, now)]
        kept = []
        seen = set()
        for entry, queued_at in reversed(pending):
            if entry.key is not None:
                if not entry.final and entry.key in seen:
                    continue
                seen.add(entry.key)
            kept.append((entry, queued_at))

        # Keep the message being sent charged until complete() is called.
        count = self.count - len(self.pending) + len(kept)
        size = self.bytes - sum(entry.size for entry, _ in self.pending)
        size += sum(entry.size for entry, _ in kept)
        if count > self.max_items or size > self.max_bytes:
            return False

        self.pending = deque(reversed(kept))
        self.count = count
        self.bytes = size
        self.ready.set()
        return True

    async def get(self):
        while not self.pending:
            if self.closed:
                return None
            self.ready.clear()
            await self.ready.wait()
        if self.pressured or time.monotonic() - self.pending[0][1] >= 0.25:
            self._batch_finals()
        message, _ = self.pending.popleft()
        self.sending_since = time.monotonic()
        return message

    def _batch_finals(self):
        messages = []
        size = len('{"type":"HISTORY","items":[]}')
        for message, _ in self.pending:
            if not message.final or message.key is None or len(messages) == 32:
                break
            next_size = size + message.size + bool(messages)
            if next_size > 256 * 1024:
                break
            messages.append(message)
            size = next_size
        if len(messages) < 2:
            return
        total_bytes = self.bytes - sum(message.size for message in messages) + size
        if total_bytes > self.max_bytes:
            return

        text = '{"type":"HISTORY","items":[' + ','.join(message.text for message in messages) + ']}'
        position = max(message.position for message in messages)
        batch = OutboundMessage(text, size, position=position)
        queued_at = self.pending[0][1]
        for _ in messages:
            self.pending.popleft()
        self.pending.appendleft((batch, queued_at))
        self.count -= len(messages) - 1
        self.bytes = total_bytes

    def complete(self, message):
        self.count -= 1
        self.bytes -= message.size
        self.sending_since = None

    def close(self):
        self.closed = True
        self.count -= len(self.pending)
        self.bytes -= sum(message.size for message, _ in self.pending)
        self.pending.clear()
        self.ready.set()
