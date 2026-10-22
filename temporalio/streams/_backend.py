"""The interface a stream store implements, and the configuration of a client's stores."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import IntEnum
from typing import TYPE_CHECKING

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

if TYPE_CHECKING:
    import temporalio.client

NATIVE = "native"
"""The name of the backend that stores streams in Temporal, which every client has."""


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
    can be read, and are then withdrawn. Always the mode of a Standalone Activity's stream, a
    standalone stream, and every stream on a backend other than native."""
    COMMIT = 2
    """The owning workflow's records become visible when its workflow task commits. Workflows
    read the stream with recorded Read operations."""


@dataclass(frozen=True)
class StreamClose:
    """The close of a stream: written by its owner, by any caller for a standalone stream, or by
    the native server when the owner ends or a standalone stream reaches its idle timeout.
    """

    status: CloseStatus
    failure: temporalio.api.failure.v1.Failure | None = None
    reason: str = ""


@dataclass(frozen=True)
class ReadResponse:
    """The result of a read.

    ``cursor`` is where the next read resumes, past records a topic filter skipped. ``close`` is
    set once the stream is closed and the reader has every record. ``exists`` is set once the
    stream has been created, by a create or its first append. ``mode`` is the stream's mode for
    the owner's current run. On native, ``owner_close_time`` is set once the execution that holds
    the stream has closed, and ``retention`` is then the namespace's retention, after which the
    stream is removed; on Redis ``retention`` is the backend's, which applies from the stream's
    last write.
    """

    records: Sequence[Record]
    cursor: bytes | None
    close: StreamClose | None
    exists: bool = False
    mode: StreamMode = StreamMode.UNKNOWN
    owner_close_time: datetime | None = None
    retention: timedelta | None = None


@dataclass(frozen=True)
class WorkflowEnding:
    """How a run ends, from the terminal command of its completion.

    ``cron`` and ``retry_policy`` say whether the run chain may continue: a cron run that completes
    or fails is followed by another, and a failed run with a retry policy may be retried. The worker
    cannot learn whether the server retries a failed run, so a backend does not close the streams
    for either, even when the retries are exhausted. The native server knows, and closes the
    streams of a failed run whose retries are exhausted.
    """

    close: StreamClose
    cron: bool
    retry_policy: bool


@dataclass(frozen=True)
class WorkflowStreamWrites:
    """An activation's writes to one stream: one of the workflow's own streams, by ``name``, or a
    standalone stream, by ``stream_id``. ``close`` is set when the workflow closed its own stream,
    after the records; a workflow cannot close a standalone stream.
    """

    records: Sequence[AppendRecord]
    name: str = ""
    stream_id: str = ""
    close: StreamClose | None = None


@dataclass(frozen=True)
class WorkflowActivationCompleted:
    """What a workflow wrote to the streams of one backend since the previous call, in an
    activation that has not failed.

    ``workflow_id`` and ``first_run_id`` are the workflow's run chain, whose streams ``streams``
    names. ``commit_mode`` is set when the workflow's definition declares commit mode, and
    ``declare_mode`` in the activations of the run's first workflow task while not replaying, in
    which a commit-mode run declares the mode of its streams for their readers. ``ending`` is set
    in the last call for an activation whose completion ends the run.
    """

    workflow_id: str
    first_run_id: str
    producer: WorkflowProducer
    streams: Sequence[WorkflowStreamWrites]
    ending: WorkflowEnding | None
    commit_mode: bool = False
    declare_mode: bool = False


class StreamBackend(ABC):
    """Stores streams. A producer must not have more than one append in flight within an epoch.

    Each backend of a client has a unique :py:attr:`name`, which stream handles record.
    """

    name: str
    """The backend's name among its client's backends."""

    def bind(  # type:ignore[reportUnusedParameter]
        self,
        client: temporalio.client.Client,  # type:ignore[reportUnusedParameter]
        *,
        workflow_reads: bool,  # type:ignore[reportUnusedParameter]
    ) -> None:
        """Called with the client whose backends include this one, and whether its workflows read
        streams. A backend that needs the server, such as for the notifiers of streams in an
        external store, keeps it.
        """

    @abstractmethod
    async def create(
        self,
        ref: StreamRef,
        *,
        request_id: str = "",
        idle_timeout: timedelta | None = None,
    ) -> bool:
        """Create the stream, and return whether this create, by its request ID, created it.

        A create of a stream that exists succeeds and returns false, unless the stream is closed
        or has another idle timeout. ``idle_timeout`` is a standalone stream's; unset means 7 days.
        A stream of an owner closes with its owner and does not accept one. An append creates the
        stream too, with default settings.
        """

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

        An epoch's first append has seq 0 and each later one the next seq. A retry of the
        producer's latest seq with the same content stores nothing and returns the original
        cursors, even after the stream is closed. Content is compared as encoded, so a retry must
        encode to the same bytes. ``finish`` records that the producer will write no more; it does
        not close the stream.
        """

    @abstractmethod
    async def close(
        self,
        ref: StreamRef,
        owner: WorkflowProducer | ActivityProducer | None,
        seq: int,
        close: StreamClose,
    ) -> None:
        """Close the stream. Only the stream's owner may close it: a workflow task of the owning
        run, or an attempt of the owning Standalone Activity. Any caller may close a standalone
        stream, with no owner; a close of a closed standalone stream succeeds if it is the same
        close.

        On native the server also closes an owner's streams in the transaction that ends the
        owner. A backend the server cannot write to has no such close. For a workflow owner the
        worker writes it (see :py:meth:`workflow_evicted`). For a Standalone Activity owner nothing
        writes it: the worker cannot learn whether the server accepted the activity's completion
        (Core sends the respond call and discards its result), or whether a failed attempt will be
        retried (the failed response does not say). Such an activity must close its own streams,
        or they stay open until their retention passes.
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
        the read after the record at that cursor, without waiting. ``max_bytes``, if positive,
        stops the read before the record that would take its payloads past it; a record that alone
        exceeds it raises :py:class:`temporalio.streams.StreamTooLargeError`. A backend that makes
        RPCs uses ``rpc_metadata`` and ``rpc_timeout`` for each; others ignore them.

        A read from the beginning of a stream that does not exist yet waits, and returns no
        records when ``wait`` passes. A read after a cursor of a stream that does not exist fails.
        """

    @abstractmethod
    async def workflow_read(self, request: WorkflowReadRequest) -> WorkflowReadResponse:
        """A workflow's read, made by the SDK's read local activity and again by replay: a read
        that does not wait, with the SHA-256 digest of its records. On a backend whose readers wait
        on a notifier, with ``request.notifier`` set, a read that finds nothing reads the stream's
        notifier position and then reads again. The messages are the temporalio-streams crate's.
        """

    @abstractmethod
    async def workflow_activation_completed(
        self, completed: WorkflowActivationCompleted
    ) -> Sequence[temporalio.api.protocol.v1.Message]:
        """Append what a workflow wrote to this backend's streams, then write the closes it made.
        Called by the worker while an activation runs, whenever workflow code awaits a write and
        nothing else can run, and once more at the end of the activation, before its completion
        is sent.

        The worker does not call it for a failed activation: the retried workflow task appends
        those records again.

        In commit mode, returns the records for the workflow's own streams as protocol messages
        instead, which the worker sends in the activation's completion; a backend other than
        native raises. If the completion ends the run, a backend that the server cannot write to
        keeps the close until :py:meth:`workflow_evicted` says the server accepted the completion,
        and keeps none for a completed cron run or a failed run with a retry policy.
        """

    @abstractmethod
    async def workflow_evicted(self, run_id: str, *, execution_ending: bool) -> None:
        """Forget an evicted run. Called by the worker on every eviction.

        ``execution_ending`` is set when the eviction follows the server accepting the run's
        terminal completion. Only then does a backend that the server cannot write to write the
        close it kept, to every one of the run's streams that is open. If this call fails, or the
        worker stops first, the close is never written.
        """


@dataclass(frozen=True)
class StreamBackends:
    """The stream backends of a client and its workers, besides native, which every client has.

    A stream handle records the name of its stream's backend, and every call on it goes to that
    backend. ``default`` names the backend that streams are created on, and that handles got by
    name refer to, when the caller names none.

    ``native`` and ``workflow_reads`` switch off native streams and workflow reads of streams. With
    native off, the client and its workers have no native backend, so the default must be another,
    and a worker refuses workflows that declare commit mode. With workflow reads off, a workflow's
    read raises, workers register no read local activity, and backends whose readers wait on
    notifiers in the server, such as Redis, send no Notify. The server has matching switches,
    history.enableNativeStreams and history.enableStreamWorkflowReads. Both off is the Brook
    configuration.

    .. warning::
        This API is a prototype and will change.
    """

    backends: Sequence[StreamBackend] = ()
    default: str = NATIVE
    native: bool = True
    workflow_reads: bool = True

    def __post_init__(self) -> None:
        """Check that the names are unique and the default is one of them."""
        names = [*([NATIVE] if self.native else []), *(b.name for b in self.backends)]
        if len(set(names)) != len(names):
            raise ValueError(f"stream backend names must be unique: {names}")
        if self.default not in names:
            raise ValueError(
                f"default stream backend {self.default!r} is not in {names}"
            )
