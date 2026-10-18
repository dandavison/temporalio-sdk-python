"""Writers for activities and clients."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import temporalio.activity
import temporalio.converter
from temporalio.streams._backend import StreamBackend
from temporalio.streams._record import ActivityProducer, AppendRecord, Producer
from temporalio.streams._ref import StreamRef


class StreamWriter:
    """Appends values for one producer, assigning each append the next seq.

    Use one writer per producer and epoch: another writer for the same producer and epoch starts
    again at seq 0, and its appends are rejected, or not stored if one repeats the content of the
    latest stored append. A failed append does not advance the seq, so calling :py:meth:`append`
    again with the same values is a retry of it; the retry is a no-op if the failed append was
    stored, but only if the values encode to the same bytes, which a codec that encrypts with a
    random nonce does not do.
    """

    def __init__(
        self,
        backend: StreamBackend,
        ref: StreamRef,
        producer: Producer,
        data_converter: temporalio.converter.DataConverter = temporalio.converter.DataConverter.default,
    ) -> None:
        """Create a writer."""
        self._backend = backend
        self._ref = ref
        self._producer = producer
        self._data_converter = data_converter
        self._next_seq = 0

    @staticmethod
    def for_activity(
        backend: StreamBackend,
        ref: StreamRef,
        data_converter: temporalio.converter.DataConverter = temporalio.converter.DataConverter.default,
    ) -> StreamWriter:
        """Create a writer for the current activity attempt."""
        return StreamWriter(
            backend, ref, ActivityProducer(temporalio.activity.info()), data_converter
        )

    async def append(self, *values: Any, topic: str = "") -> Sequence[bytes]:
        """Convert and encode the values, and append them as one batch."""
        payloads = await self._data_converter.encode(values)
        cursors = await self._backend.append(
            self._ref,
            self._producer,
            self._next_seq,
            [AppendRecord(topic, p) for p in payloads],
        )
        self._next_seq += 1
        return cursors
