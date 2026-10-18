"""The interface a stream store implements."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from enum import IntEnum

import temporalio.api.failure.v1
import temporalio.api.protocol.v1
from temporalio.streams._record import AppendRecord, Producer, Record, WorkflowProducer
from temporalio.streams._ref import StreamRef


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


@dataclass(frozen=True)
class WorkflowEnding:
    """How a run ends, from the terminal command of its completion.

    ``cron`` and ``retry_policy`` say whether the run chain may continue: a completed cron run is
    followed by another, and a failed run with a retry policy may be retried.
    """

    close: StreamClose
    cron: bool
    retry_policy: bool


@dataclass(frozen=True)
class WorkflowActivationCompleted:
    """The records a successful activation appended to its workflow's own stream.

    ``ending`` is set when the activation's completion ends the run.
    """

    ref: StreamRef
    producer: WorkflowProducer
    records: Sequence[AppendRecord]
    ending: WorkflowEnding | None


class StreamBackend(ABC):
    """Stores streams. A producer must not have more than one append in flight within an epoch."""

    @abstractmethod
    async def append(
        self,
        ref: StreamRef,
        producer: Producer,
        seq: int,
        records: Sequence[AppendRecord],
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

    @abstractmethod
    async def workflow_activation_completed(
        self, completed: WorkflowActivationCompleted
    ) -> Sequence[temporalio.api.protocol.v1.Message]:
        """Append an activation's records. Called by the worker after each successful activation,
        before its completion is sent.

        In commit mode, returns the records as protocol messages instead, which the worker sends in
        the activation's completion. If the completion ends the run, a backend that the server
        cannot write to keeps the close until :py:meth:`workflow_evicted` says the server accepted
        the completion.
        """

    @abstractmethod
    async def workflow_evicted(self, run_id: str, *, execution_ending: bool) -> None:
        """Forget an evicted run. Called by the worker on every eviction.

        ``execution_ending`` is set when the eviction follows the server accepting the run's
        terminal completion.
        """
