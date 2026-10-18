"""Streams owned by workflows: ordered records that workflows, activities and clients append, and
that readers read from a cursor.

.. warning::
    This API is a prototype and will change.

A workflow appends to its own stream with :py:func:`workflow_append`. The worker appends the
records at the end of the activation, before completing it, using the worker's
``stream_backend``. The append is not a command and adds nothing to history, so replay does not
repeat it.

Every record carries the (producer, epoch, seq) of its append. A reader may receive records from a
lower epoch of a producer before records from a higher one, for example from a failed activity
attempt and then from its retry; the epoch tells the reader which to discard.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from enum import IntEnum
from typing import Any

import temporalio.activity
import temporalio.api.common.v1
import temporalio.api.failure.v1
import temporalio.converter
import temporalio.workflow


@dataclass(frozen=True)
class StreamRef:
    """A stream owned by a workflow run chain.

    ``first_run_id`` is optional; when empty, the stream of the workflow ID's current chain is
    used.
    """

    workflow_id: str
    first_run_id: str = ""


@dataclass(frozen=True)
class ClientProducer:
    """A client appending under an identity and epoch that it chooses."""

    id: str
    epoch: int


@dataclass(frozen=True)
class ActivityProducer:
    """An activity attempt. Its epoch is the attempt stamp, which a retry or reset increases."""

    info: temporalio.activity.Info


@dataclass(frozen=True)
class WorkflowProducer:
    """A workflow task attempt of a workflow run, appending sink-style.

    Its epoch is (``previous_started_event_id``, ``attempt``). Records of a workflow are superseded
    only by a later attempt of the same workflow task.
    """

    workflow_id: str
    run_id: str
    previous_started_event_id: int
    attempt: int


Producer = ClientProducer | ActivityProducer | WorkflowProducer


@dataclass(frozen=True)
class ProducerIdentity:
    """The (producer, epoch, seq) stored with every record.

    ``workflow_task`` is set only for a workflow producer, and is part of its epoch.
    """

    producer: str
    workflow_task: int
    epoch: int
    seq: int


@dataclass(frozen=True)
class RecordInput:
    """A record to append."""

    topic: str
    payload: temporalio.api.common.v1.Payload


@dataclass(frozen=True)
class Record:
    """A stored record. ``cursor`` marks the boundary after it."""

    cursor: bytes
    producer: ProducerIdentity
    topic: str
    payload: temporalio.api.common.v1.Payload | None
    finish: bool


class CloseStatus(IntEnum):
    """How a stream was closed."""

    COMPLETED = 1
    FAILED = 2
    CANCELED = 3
    TERMINATED = 4
    TIMED_OUT = 5


@dataclass(frozen=True)
class StreamClose:
    """The close of a stream, written by its owner."""

    status: CloseStatus
    failure: temporalio.api.failure.v1.Failure | None = None
    reason: str = ""


@dataclass(frozen=True)
class ReadResponse:
    """The result of a read.

    ``cursor`` is where the next read resumes, past records a topic filter skipped. ``close`` is
    set once the stream is closed and the reader has every record.
    """

    records: Sequence[Record]
    cursor: bytes | None
    close: StreamClose | None


class StreamError(Exception):
    """Base class of stream errors."""


class StreamClosedError(StreamError):
    """The stream is closed."""


class StreamNotFoundError(StreamError):
    """The stream does not exist."""


class StreamExpiredError(StreamError):
    """The stream existed but has been deleted."""


class StreamNotOwnerError(StreamError):
    """Only the stream's owner may close it."""


class StreamCursorError(StreamError):
    """The cursor belongs to another stream."""


class SeqConflictError(StreamError):
    """The seq is already stored with different content."""


class SeqSupersededError(StreamError):
    """The seq is lower than the producer's latest."""


class EpochFencedError(StreamError):
    """The epoch is lower than one already seen for this producer."""


class StreamBackend(ABC):
    """Stores streams. A producer must not have more than one append in flight within an epoch."""

    @abstractmethod
    async def append(
        self,
        ref: StreamRef,
        producer: Producer,
        seq: int,
        records: Sequence[RecordInput],
        *,
        finish: bool = False,
    ) -> Sequence[bytes]:
        """Append records as one atomic, contiguous batch, returning their cursors.

        The first append creates the stream. A retry of the producer's latest seq with the same
        content stores nothing and returns the original cursors. ``finish`` records that the
        producer will write no more; it does not close the stream.
        """

    @abstractmethod
    async def close(
        self, ref: StreamRef, owner: WorkflowProducer, seq: int, close: StreamClose
    ) -> None:
        """Close the stream. Only the stream's owner may close it."""

    @abstractmethod
    async def read(
        self,
        ref: StreamRef,
        *,
        after: bytes | None = None,
        topics: Sequence[str] = (),
        max_records: int = 0,
        wait: timedelta = timedelta(0),
        last: int = 0,
    ) -> ReadResponse:
        """Read records after a cursor, or from the beginning.

        ``topics`` filters by topic; empty means all. ``wait`` is how long the read may wait for a
        matching record. ``last``, if positive, reads the last N matching records instead.
        """


def workflow_append(value: Any, *, topic: str = "") -> None:
    """Append a value to the calling workflow's own stream.

    The worker appends it at the end of the current activation. Must be called from workflow code.
    """
    temporalio.workflow._Runtime.current().workflow_stream_append(topic, value)


class StreamWriter:
    """Appends values for one producer, assigning each append the next seq."""

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
        payloads = await self._data_converter.encode(
            [self._data_converter.payload_converter.to_payload(v) for v in values]
        )
        cursors = await self._backend.append(
            self._ref,
            self._producer,
            self._next_seq,
            [RecordInput(topic, p) for p in payloads],
        )
        self._next_seq += 1
        return cursors


async def decode(
    record: Record,
    type_hint: type | None = None,
    data_converter: temporalio.converter.DataConverter = temporalio.converter.DataConverter.default,
) -> Any:
    """Decode and convert a data record's payload."""
    if record.payload is None:
        raise ValueError("record has no payload")
    [payload] = await data_converter.decode([record.payload])
    if type_hint is None:
        return data_converter.payload_converter.from_payload(payload)
    return data_converter.payload_converter.from_payload(payload, type_hint)
