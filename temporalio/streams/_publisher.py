"""Appends for activities and clients: awaited appends, an optional buffer over them, and client
producers' epochs.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Sequence
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
    """Buffers published values in memory and appends them through a writer, such as a client's
    :py:class:`temporalio.streams.StreamProducer` or an activity's stream handle, when flushed.

    ``publish`` only buffers. ``flush`` appends everything buffered, in appends that fit the size
    limits (1000 records, 4 MiB), one at a time, and returns once all are stored. Leaving the
    ``async with`` block flushes, also when the block raised. Nothing else flushes: the buffer
    grows until flushed, and values still buffered when the process exits, or when an activity
    attempt completes, are lost. A failed append, after the backend's retries, is raised from this
    and every later call.

    .. warning::
        This API is a prototype and will change.
    """

    def __init__(self, writer: _Publishing) -> None:
        """Create a buffer. Use ``buffered`` on a writer instead."""
        self._writer = writer
        self._buffer: list[AppendRecord] = []
        self._error: BaseException | None = None

    async def __aenter__(self) -> BufferedStreamWriter:
        """Return self."""
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        """Flush."""
        await self.flush()

    def publish(self, *values: Any, topic: str = "") -> None:
        """Convert the values and buffer them."""
        self._check()
        for value in values:
            self._buffer.append(
                AppendRecord(topic, self._writer.payload_converter.to_payload(value))
            )

    async def flush(self) -> None:
        """Append everything buffered, and return once it is stored."""
        self._check()
        batch, self._buffer = self._buffer, []
        if not batch:
            return
        try:
            await self._writer._append(batch)
        except Exception as err:
            self._error = err
            raise

    def _check(self) -> None:
        if self._error:
            raise self._error


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
