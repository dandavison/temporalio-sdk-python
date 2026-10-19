"""The interface a stream store implements."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import IntEnum

import temporalio.api.failure.v1
import temporalio.api.protocol.v1
from temporalio.bridge.proto.streams import WorkflowReadRequest, WorkflowReadResponse
from temporalio.streams._record import (
    ActivityProducer,
    AppendRecord,
    Producer,
    Record,
    WorkflowProducer,
)
from temporalio.streams._ref import StreamRef


class CloseStatus(IntEnum):
    """How a stream was closed."""

    COMPLETED = 1
    FAILED = 2
    CANCELED = 3
    TERMINATED = 4
    TIMED_OUT = 5


class StreamMode(IntEnum):
    """How a stream's owner appends its own records, which decides how workflows read it."""

    UNKNOWN = 0
    """Not yet known: the owner is a workflow whose first workflow task has not completed, or no
    read has told the mode yet."""
    SINK = 1
    """The owner's records are appended as it runs, so a failed workflow task attempt's records
    can be read, and are then withdrawn. Always the mode of a Standalone Activity's stream, and of
    every stream on a backend other than native."""
    COMMIT = 2
    """The owning workflow's records become visible when its workflow task commits. Workflows
    read the stream with recorded Read operations."""


@dataclass(frozen=True)
class StreamClose:
    """The close of a stream: written by its owner, or by the native server when the owner ends."""

    status: CloseStatus
    failure: temporalio.api.failure.v1.Failure | None = None
    reason: str = ""


@dataclass(frozen=True)
class ReadResponse:
    """The result of a read.

    ``cursor`` is where the next read resumes, past records a topic filter skipped. ``close`` is
    set once the stream is closed and the reader has every record. ``mode`` is the stream's mode
    for the owner's current run. On native, ``owner_close_time`` is set once the owner has closed,
    and ``retention`` is then the namespace's retention, after which the stream is removed; on
    Redis ``retention`` is the backend's, which applies from the stream's last write.
    """

    records: Sequence[Record]
    cursor: bytes | None
    close: StreamClose | None
    mode: StreamMode = StreamMode.UNKNOWN
    owner_close_time: datetime | None = None
    retention: timedelta | None = None


@dataclass(frozen=True)
class WorkflowEnding:
    """How a run ends, from the terminal command of its completion.

    ``cron`` and ``retry_policy`` say whether the run chain may continue: a cron run that completes
    or fails is followed by another, and a failed run with a retry policy may be retried. The worker
    cannot learn whether the server retries a failed run, so a backend does not close the stream for
    either, even when the retries are exhausted. The native server knows, and closes the stream of a
    failed run whose retries are exhausted.
    """

    close: StreamClose
    cron: bool
    retry_policy: bool


@dataclass(frozen=True)
class WorkflowActivationCompleted:
    """The records a successful activation appended to its workflow's own stream.

    ``commit_mode`` is set when the workflow's definition declares commit mode, and
    ``declare_mode`` for the activations of the run's first workflow task while not replaying, in
    which a commit-mode run declares its mode. ``close`` is set when the workflow closed its own
    stream in the activation, after the records. ``ending`` is set when the activation's completion
    ends the run.
    """

    ref: StreamRef
    producer: WorkflowProducer
    records: Sequence[AppendRecord]
    ending: WorkflowEnding | None
    commit_mode: bool = False
    close: StreamClose | None = None
    declare_mode: bool = False


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

        The first append creates the stream; on native, only while its owner exists. An epoch's
        first append has seq 0 and each later one the next seq. A retry of the producer's latest seq
        with the same content stores nothing and returns the original cursors, even after the
        stream is closed. Content is compared as encoded, so a retry must encode to the same bytes.
        ``finish`` records that the producer will write no more; it does not close the stream.
        """

    @abstractmethod
    async def close(
        self,
        ref: StreamRef,
        owner: WorkflowProducer | ActivityProducer,
        seq: int,
        close: StreamClose,
    ) -> None:
        """Close the stream. Only the stream's owner may close it: a workflow task of the owning
        run, or an attempt of the owning Standalone Activity.

        On native the server also closes the stream in the transaction that ends its owner. A
        backend the server cannot write to has no such close. For a workflow owner the worker
        writes it (see :py:meth:`workflow_evicted`). For a Standalone Activity owner nothing writes
        it: the worker cannot learn whether the server accepted the activity's completion (Core
        sends the respond call and discards its result), or whether a failed attempt will be
        retried (the failed response does not say). Such an activity must close its own stream, or
        the stream stays open until its retention passes.
        """

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
        until: bytes | None = None,
        max_bytes: int = 0,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ReadResponse:
        """Read records after a cursor, or from the beginning.

        ``topics`` filters by topic; empty means all. ``wait`` is how long the read may wait for a
        matching record. ``last``, if positive, reads the last N matching records instead. Markers
        match every topic filter, and count towards ``max_records`` and ``last``. ``until`` stops
        the read after the record at that cursor. ``max_bytes``, if positive, stops the read before
        the record that would take its payloads past it; a record that alone exceeds it raises
        :py:class:`temporalio.streams.StreamTooLargeError`. A backend that makes RPCs uses
        ``rpc_metadata`` and ``rpc_timeout`` for each; others ignore them.

        A read from the beginning of a stream that does not exist yet waits, and returns no
        records when ``wait`` passes. A read after a cursor of a stream that does not exist fails.
        """

    @abstractmethod
    async def workflow_read(self, request: WorkflowReadRequest) -> WorkflowReadResponse:
        """A workflow's read, made by the SDK's read local activity and again by replay: a read
        that does not wait, with the SHA-256 digest of its records. On a backend whose readers wait
        on the owner's notifier, with ``request.notifier`` set, a read that finds nothing reads the
        notifier's position and then reads again. The messages are the temporalio-streams crate's.
        """

    @abstractmethod
    async def workflow_activation_completed(
        self, completed: WorkflowActivationCompleted
    ) -> Sequence[temporalio.api.protocol.v1.Message]:
        """Append an activation's records, then write the workflow's own close if it made one.
        Called by the worker after each successful activation, before its completion is sent.

        The worker does not call it for a failed activation: the retried workflow task appends
        those records again.

        In commit mode, returns the records as protocol messages instead, which the worker sends in
        the activation's completion; a backend other than native raises. If the completion ends the
        run and the workflow has not closed its stream, a backend that the server cannot write to
        keeps the close until :py:meth:`workflow_evicted` says the server accepted the completion,
        and keeps none for a completed cron run or a failed run with a retry policy.
        """

    @abstractmethod
    async def workflow_evicted(self, run_id: str, *, execution_ending: bool) -> None:
        """Forget an evicted run. Called by the worker on every eviction.

        ``execution_ending`` is set when the eviction follows the server accepting the run's
        terminal completion. Only then does a backend that the server cannot write to write the
        close it kept. If this call fails, or the worker stops first, the close is never written.
        """
