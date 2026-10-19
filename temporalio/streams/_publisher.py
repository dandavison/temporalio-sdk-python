"""Buffered appends for activities and clients."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

import temporalio.api.common.v1
import temporalio.converter
from temporalio.streams._backend import StreamBackend, StreamClose
from temporalio.streams._record import ActivityProducer, AppendRecord, Producer
from temporalio.streams._ref import StreamRef

_MAX_RECORDS = 1000
_MAX_BYTES = 4 * 1024 * 1024


class Publisher:
    """Buffers published values and appends them as one producer, with at most one append in
    flight, as the seq contract requires.

    An append is sent ``batch_interval`` after the first value it holds was published, or at once
    when the buffer reaches an append's size limits or a caller forces or awaits a flush. A failed
    append, after the backend's retries, drops the buffer and is raised from every later call.
    """

    def __init__(
        self,
        backend: StreamBackend,
        ref: StreamRef,
        producer: Producer,
        payload_converter: temporalio.converter.PayloadConverter,
        data_converter: temporalio.converter.DataConverter,
        batch_interval: timedelta,
        on_done: Callable[[], None] = lambda: None,
    ) -> None:
        """Create a publisher. ``on_done`` is called once, when it finishes, closes or fails."""
        self._backend = backend
        self._ref = ref
        self._producer = producer
        self._payload_converter = payload_converter
        self._data_converter = data_converter
        self._batch_interval = batch_interval.total_seconds()
        self._on_done: Callable[[], None] | None = on_done
        self._buffer: list[AppendRecord] = []
        self._buffer_bytes = 0
        self._seq = 0
        self._error: BaseException | None = None
        self._ended = ""
        self._sending: asyncio.Task[None] | None = None
        self._timer: asyncio.TimerHandle | None = None
        self._urgent = False

    def publish(self, value: Any, topic: str, force_flush: bool) -> None:
        """Convert the value now and buffer it. Must be called in the event loop."""
        self._check_open()
        payload = self._payload_converter.to_payload(value)
        self._buffer.append(AppendRecord(topic, payload))
        self._buffer_bytes += payload.ByteSize()
        self._schedule(force_flush or self._full())

    async def flush(self) -> None:
        """Wait until everything published so far is stored."""
        while True:
            self._check()
            if not self._buffer and not self._sending:
                return
            self._schedule(True)
            assert self._sending
            await asyncio.shield(self._sending)

    async def finish(self) -> None:
        """Flush, then record that the producer will write no more."""
        self._check_open()
        await self.flush()
        await self._end(
            "finished",
            self._backend.append(self._ref, self._producer, self._seq, [], finish=True),
        )

    async def close(self, close: StreamClose) -> None:
        """Flush, then close the stream. Only the stream's owner may close it."""
        self._check_open()
        await self.flush()
        assert isinstance(self._producer, ActivityProducer)
        await self._end(
            "closed the stream",
            self._backend.close(self._ref, self._producer, self._seq, close),
        )

    def release(self) -> None:
        """Call ``on_done``, unless it was called."""
        on_done = self._on_done
        self._on_done = None
        if on_done:
            on_done()

    async def _end(self, ended: str, call: Awaitable[Any]) -> None:
        try:
            await call
        except BaseException as err:
            self._error = err
            raise
        else:
            self._seq += 1
            self._ended = ended
        finally:
            self.release()

    def _check(self) -> None:
        if self._error:
            raise self._error

    def _check_open(self) -> None:
        self._check()
        if self._ended:
            raise RuntimeError(f"The stream producer {self._ended}")

    def _full(self) -> bool:
        return len(self._buffer) >= _MAX_RECORDS or self._buffer_bytes >= _MAX_BYTES

    def _schedule(self, urgent: bool) -> None:
        self._urgent = self._urgent or urgent
        if self._sending:
            return
        loop = asyncio.get_running_loop()
        if self._urgent:
            if self._timer:
                self._timer.cancel()
                self._timer = None
            self._sending = loop.create_task(self._send())
        elif not self._timer:
            self._timer = loop.call_later(self._batch_interval, self._on_timer)

    def _on_timer(self) -> None:
        self._timer = None
        self._schedule(True)

    async def _send(self) -> None:
        try:
            while self._buffer:
                self._urgent = False
                await self._append_batch()
                if not self._urgent and not self._full():
                    break
        except Exception as err:
            self._error = err
            self._buffer.clear()
            self._buffer_bytes = 0
            self.release()
        finally:
            self._sending = None
        if self._buffer and not self._error:
            self._schedule(False)

    async def _append_batch(self) -> None:
        batch = self._buffer[:_MAX_RECORDS]
        encoded = await self._data_converter._encode_payload_sequence(
            [r.payload for r in batch]
        )
        n = _fitting(encoded)
        await self._backend.append(
            self._ref,
            self._producer,
            self._seq,
            [AppendRecord(r.topic, p) for r, p in zip(batch[:n], encoded[:n])],
        )
        self._seq += 1
        del self._buffer[:n]
        self._buffer_bytes -= sum(r.payload.ByteSize() for r in batch[:n])


def _fitting(payloads: list[temporalio.api.common.v1.Payload]) -> int:
    """How many of the payloads, at least one, fit in one append. The codec can enlarge them, so
    this is decided after encoding.
    """
    total = 0
    for i, p in enumerate(payloads):
        total += p.ByteSize()
        if total > _MAX_BYTES and i:
            return i
    return len(payloads)


_last_epochs: dict[str, int] = {}
_live_producers: set[tuple[StreamRef, str]] = set()


def claim_producer(
    ref: StreamRef, id: str, epoch: int | None
) -> tuple[int, Callable[[], None]]:
    """Register a live client producer for the stream, returning its epoch and a release function.

    The default epoch is the current time in milliseconds, but never less than one more than the
    last epoch this process used for the ID.
    """
    key = (ref, id)
    if key in _live_producers:
        raise RuntimeError(
            f"A stream producer with ID {id!r} is already live on this stream in this process"
        )
    last = _last_epochs.get(id, -1)
    if epoch is None:
        epoch = max(time.time_ns() // 1_000_000, last + 1)
    _last_epochs[id] = max(epoch, last)
    _live_producers.add(key)
    return epoch, lambda: _live_producers.discard(key)
