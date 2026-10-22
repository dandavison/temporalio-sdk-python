"""Appends for activities and clients: awaited appends, an optional buffer over them, and client
producers' epochs.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Sequence
from datetime import timedelta
from typing import Any, Protocol

import temporalio.api.common.v1
import temporalio.converter
from temporalio.streams._backend import StreamBackend
from temporalio.streams._record import AppendRecord, Producer
from temporalio.streams._ref import StreamRef

_MAX_RECORDS = 1000
_MAX_BYTES = 4 * 1024 * 1024


class Appender:
    """Appends as one producer, with at most one append in flight, as the seq contract requires.

    Records go in as few appends as fit the size limits, each with the next seq. A failed append,
    after the backend's retries, is raised from every later call, since the producer's next seq
    is then unknown.
    """

    def __init__(
        self,
        backend: StreamBackend,
        ref: StreamRef,
        producer: Producer,
        data_converter: temporalio.converter.DataConverter,
    ) -> None:
        """Create an appender whose first append has seq 0."""
        self.backend = backend
        self.ref = ref
        self.producer = producer
        self.data_converter = data_converter
        self.seq = 0
        self._lock = asyncio.Lock()
        self._error: BaseException | None = None

    async def append(self, records: Sequence[AppendRecord]) -> None:
        """Encode the records with the codec and append them."""
        async with self._lock:
            self._check()
            encoded = await self.data_converter._encode_payload_sequence(
                [r.payload for r in records]
            )
            start = 0
            while start < len(records):
                n = _fitting(encoded[start : start + _MAX_RECORDS])
                await self._call(
                    self.backend.append(
                        self.ref,
                        self.producer,
                        self.seq,
                        [
                            AppendRecord(r.topic, p)
                            for r, p in zip(
                                records[start : start + n], encoded[start : start + n]
                            )
                        ],
                    )
                )
                start += n

    async def finish(self) -> None:
        """Record that the producer will write no more."""
        async with self._lock:
            self._check()
            await self._call(
                self.backend.append(self.ref, self.producer, self.seq, [], finish=True)
            )

    async def call(self, call: Callable[[int], Any]) -> None:
        """Make another call that takes the producer's next seq, such as an owner's close."""
        async with self._lock:
            self._check()
            await self._call(call(self.seq))

    async def _call(self, call: Any) -> None:
        try:
            await call
        except BaseException as err:
            self._error = err
            raise
        self.seq += 1

    def _check(self) -> None:
        if self._error:
            raise self._error


class _Publishing(Protocol):
    @property
    def payload_converter(self) -> temporalio.converter.PayloadConverter: ...

    async def _append(self, records: Sequence[AppendRecord]) -> None: ...


class BufferedStreamWriter:
    """Buffers published values and appends them through a writer, such as a client's
    :py:class:`temporalio.streams.StreamProducer` or an activity's stream handle.

    ``publish`` returns at once. An append is sent ``batch_interval`` after the first value it holds
    was published, or at once when the buffer reaches an append's size limits or a caller forces
    or awaits a flush; one append is in flight at a time. A failed append, after the backend's
    retries, drops the buffer and is raised from every later call. Nothing flushes the buffer for
    you: values still buffered when the process exits, or when an activity attempt completes, are
    lost. Use ``async with`` to flush on exit.

    .. warning::
        This API is a prototype and will change.
    """

    def __init__(self, writer: _Publishing, batch_interval: timedelta) -> None:
        """Create a buffer. Use ``buffered`` on a writer instead."""
        self._writer = writer
        self._batch_interval = batch_interval.total_seconds()
        self._buffer: list[AppendRecord] = []
        self._buffer_bytes = 0
        self._error: BaseException | None = None
        self._sending: asyncio.Task[None] | None = None
        self._timer: asyncio.TimerHandle | None = None
        self._urgent = False

    async def __aenter__(self) -> BufferedStreamWriter:
        """Return self."""
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        """Flush."""
        await self.flush()

    def publish(self, *values: Any, topic: str = "", force_flush: bool = False) -> None:
        """Convert the values now and buffer them. Must be called in the event loop."""
        self._check()
        for value in values:
            payload = self._writer.payload_converter.to_payload(value)
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

    def _check(self) -> None:
        if self._error:
            raise self._error

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
                batch, self._buffer, self._buffer_bytes = self._buffer, [], 0
                await self._writer._append(batch)
                if not self._urgent and not self._full():
                    break
        except Exception as err:
            self._error = err
            self._buffer.clear()
            self._buffer_bytes = 0
        finally:
            self._sending = None
        if self._buffer and not self._error:
            self._schedule(False)


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
_live_producers: set[tuple[str, StreamRef, str]] = set()


def claim_producer(
    backend: str, ref: StreamRef, id: str, epoch: int | None
) -> tuple[int, Callable[[], None]]:
    """Register a live client producer for the stream, returning its epoch and a release function.

    The default epoch is the current time in milliseconds, but never less than one more than the
    last epoch this process used for the ID.
    """
    key = (backend, ref, id)
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
