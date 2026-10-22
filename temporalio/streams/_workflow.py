"""The stream handles of workflow code, what workflow code writes for the worker to store, and how
workflow code reads and waits.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Generic, TypeVar

import temporalio.exceptions
import temporalio.workflow
from temporalio.api.nexusservices.streamservice.v1 import (
    NotifierStateResponse,
    ReadRequest,
    ReadResponse,
    WaitRequest,
    WaitResponse,
    WakeReason,
)
from temporalio.api.nexusservices.streamservice.v1 import (
    StreamRef as StreamRefProto,
)
from temporalio.bridge.proto.streams import ReadRequest as CrateReadRequest
from temporalio.bridge.proto.streams import WorkflowReadRequest, WorkflowReadResponse
from temporalio.common import RawValue, RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, NexusOperationError
from temporalio.streams import _errors
from temporalio.streams._backend import CloseStatus, StreamClose, StreamMode
from temporalio.streams._crate import read_response, ref_proto, stream_error
from temporalio.streams._errors import (
    StreamError,
    StreamModeUnknownError,
    StreamNotFoundError,
    StreamUnavailableError,
)
from temporalio.streams._handles import StreamHandleBase
from temporalio.streams._iterator import (
    StreamCursor,
    StreamRecord,
    StreamRecordAsyncIterator,
    decode_decoded,
)
from temporalio.streams._record import AppendRecord
from temporalio.streams._workflow_reads import READ_ACTIVITY, read_request

T = TypeVar("T")

_NO_TAIL_READS = "Tail reads of streams in workflow code are not implemented"
_STREAM_SERVICE = "temporal.api.nexusservices.streamservice.v1.StreamService"
_MAX_WAIT = timedelta(days=7)
# A recorded read's result must fit the namespace's payload size limit (limit.blobSize.error,
# default 2 MiB), which the SDK cannot read: it assumes the default, less a margin for the
# response's envelope.
_RECORDED_READ_MAX_BYTES = 2 * 1024 * 1024 - 64 * 1024
# Replay reads every range again, so a run that has read this much should continue as new.
_CONTINUE_AS_NEW_BYTES = 50 * 1024 * 1024
_NOT_FOUND_BACKOFF = (timedelta(seconds=1), timedelta(minutes=10))
_WAIT_FAILED_BACKOFF = (timedelta(seconds=1), timedelta(minutes=1))
_CLOSE_POLL_BACKOFF = (timedelta(seconds=1), timedelta(seconds=10))
_READ_TIMEOUT = timedelta(minutes=1)


@dataclass(frozen=True)
class WorkflowStreamHandle(StreamHandleBase):
    """Workflow code's handle to a stream: one of its own streams, from
    :py:func:`temporalio.workflow.create_stream`, or a stream it was given, from
    :py:func:`temporalio.workflow.get_stream_handle`. Workflow code writes to its own streams and to
    standalone streams.

    ``await publish(...)`` hands the records to the worker. Once nothing else in the workflow can
    run, the worker appends everything that workflow code is waiting on, resolves the waits, and
    runs workflow code on, in the same activation, so a publish adds no workflow task. If the
    store fails the append, the workflow task fails and is retried; workflow code never sees the
    error, because replay would not see it. Replay does no I/O: its publishes resolve when nothing
    else can run, as they did when the code first ran.

    Reading (``fetch_records``) follows the stream's mode, which each read reports. A read of a
    sink-mode stream (every stream on a backend other than native, a Standalone Activity's, a
    standalone stream, and a workflow's unless it declares commit mode) is a local activity that the
    SDK registers on every worker, which records in History only the read's range and a digest;
    replay reads the range again and checks it, so each replayed read costs a store read. If the
    range is no longer in the store, the workflow task fails with an application failure of type
    ``STREAM_RANGE_GONE``, and keeps failing until the run is reset to a point after the read, or
    terminated: a reader must continue as new before the store removes what it read (see
    ``continue_as_new_suggested`` on the iterator). A read of a commit-mode stream is a recorded
    Read operation on the System Nexus Endpoint, whose records are in History, so replay reads no
    store; each costs about 5 events and a workflow task round trip, and up to 2 MiB of History.
    While the stream's mode is unknown (its owner's first workflow task has not completed), the
    reader waits for it, for at most ``owner_wait``.

    At the stream's end a reader waits with an asynchronous Wait operation on the System Nexus
    Endpoint, which completes on the next matching record (native) or the stream's next Notify
    (other backends), on the stream's creation if it does not exist yet, or on the close, and costs
    a few events and a workflow task. It has at most one Wait pending, and does not cancel it. A
    workflow task spends at most 10 reads or one second on reads; the next task continues. Reads
    raise if the client's stream backends configuration switches workflow reads off.

    In a workflow that declares commit mode (``@workflow.defn(stream_commit_mode=True)``, native
    only), the records for its own streams travel in the workflow task's completion and become
    visible when the task commits, so a failed task attempt's records are never delivered; a
    publish resolves without waiting for the store. All of a task's records go in that one
    completion, which the server's request size limit bounds. Otherwise a failed task attempt may
    already have delivered records, which readers then withdraw (on native, using the server's
    commit records). In both modes the append is not a command and adds nothing to history.

    .. warning::
        This API is a prototype and will change.
    """

    async def publish(self, *values: Any, topic: str = "") -> None:
        """Convert the values and append them to the topic."""
        await temporalio.workflow._Runtime.current().workflow_stream_publish(
            self, topic, values
        )

    def topic(self, name: str, *, type: type[T]) -> WorkflowTopicHandle[T]:
        """A handle that publishes values of ``type`` to one topic."""
        return WorkflowTopicHandle(self, name, type)

    async def close(
        self, *, failure: BaseException | None = None, reason: str = ""
    ) -> None:
        """Close one of the workflow's own streams, as failed if ``failure`` is given and as
        completed otherwise, after the records published before it. Later publishes raise. Without
        it, on native the server closes the workflow's streams when the run chain ends, and on
        other backends the worker closes them after the server accepts the run's terminal
        completion.

        The close is written before the workflow task completes. If the task attempt then fails,
        its retry appends and closes again, which fail because the stream is closed, so the run's
        workflow tasks keep failing. Not supported in commit mode yet.
        """
        await temporalio.workflow._Runtime.current().workflow_stream_close(
            self, failure, reason
        )

    def fetch_records(
        self,
        *,
        after: StreamCursor | None = None,
        topics: Sequence[str] | None = None,
        page_size: int | None = None,
        wait_new_records: bool = False,
        hold_provisional: bool | None = None,
        owner_wait: timedelta = timedelta(hours=1),
        close_wait: timedelta = timedelta(minutes=1),
        hold_timeout: timedelta = timedelta(minutes=5),
        redis_wait_lifetime: timedelta = _MAX_WAIT,
        type_hint: type | None = None,
    ) -> StreamRecordAsyncIterator[Any]:
        """Iterate over the stream's records after ``after``, or from the beginning.

        Args:
            after: A record's cursor, or an iterator's, to resume after.
            topics: The topics to read; all if unset.
            page_size: The most records each read returns, markers included.
            wait_new_records: Wait for new records at the stream's end, and end only at its close,
                when the owner ended without closing it, or when continue-as-new is suggested.
                Otherwise the iteration ends at the stream's current end.
            hold_provisional: Hold back a provisional record, and every later one, until its
                workflow task commits (see :py:class:`StreamRecordAsyncIterator`). Native only,
                and the default there; on another backend, which writes no commit records, it
                raises ``ValueError``.
            owner_wait: How long to wait for an owner, or a standalone stream, that does not exist,
                before raising :py:class:`temporalio.streams.StreamNotFoundError`, or for the
                stream's mode, before raising
                :py:class:`temporalio.streams.StreamModeUnknownError`. A missing owner is read
                again after a timer, with backoff from 1 s to 10 min.
            close_wait: After the owner ended without the stream's close (a backend whose closes
                the owner's worker writes), how long to keep reading for the close before ending
                with ``owner_end`` set.
            hold_timeout: How long a record may be held. The hold uses no timer of its own: it
                limits the lifetime of the Wait the reader makes at the stream's end.
            redis_wait_lifetime: The lifetime of a Wait on a stream in an external store, such as
                Redis, at most 7 days. A reader is woken by a producer's Notify, which is lost if
                the producer stops between its write and the Notify, or the Notify fails; the
                reader then reads again when the Wait expires. A shorter lifetime bounds that delay
                at the cost of a Wait, and a workflow task, per lifetime while the stream is idle.
            type_hint: The type to decode values as. Without it, values decode as the payload
                converter's default.
        """
        return WorkflowRecordIterator(
            self,
            after=after,
            topics=topics,
            page_size=page_size,
            wait_new_records=wait_new_records,
            hold_provisional=hold_provisional,
            owner_wait=owner_wait,
            close_wait=close_wait,
            hold_timeout=hold_timeout,
            redis_wait_lifetime=redis_wait_lifetime,
            type_hint=type_hint,
        )

    async def fetch_last_records(
        self,
        n: int,  # type:ignore[reportUnusedParameter]
        **kwargs: Any,  # type:ignore[reportUnusedParameter]
    ) -> Sequence[StreamRecord[Any]]:
        """Not implemented: a tail read's range is not one that replay can read again."""
        raise NotImplementedError(_NO_TAIL_READS)


class WorkflowTopicHandle(Generic[T]):
    """Workflow code's handle to one topic of a stream."""

    def __init__(self, stream: WorkflowStreamHandle, name: str, type: type[T]) -> None:
        """Create a topic handle. Use :py:meth:`WorkflowStreamHandle.topic` instead."""
        self._stream = stream
        self.name = name
        self.type = type

    async def publish(self, *values: T) -> None:
        """See :py:meth:`WorkflowStreamHandle.publish`."""
        await self._stream.publish(*values, topic=self.name)

    def fetch_records(self, **kwargs: Any) -> StreamRecordAsyncIterator[T]:
        """See :py:meth:`WorkflowStreamHandle.fetch_records`, reading this topic, decoded as its
        type.
        """
        return self._stream.fetch_records(
            topics=[self.name], type_hint=self.type, **kwargs
        )


class WorkflowRecordIterator(StreamRecordAsyncIterator[T]):
    """The iterator of workflow code, which reads with the read local activity or recorded Read
    operations, by the stream's mode, and waits with Wait operations.
    """

    def __init__(
        self,
        handle: WorkflowStreamHandle,
        *,
        after: StreamCursor | None,
        topics: Sequence[str] | None,
        page_size: int | None,
        wait_new_records: bool,
        hold_provisional: bool | None,
        owner_wait: timedelta,
        close_wait: timedelta,
        hold_timeout: timedelta,
        redis_wait_lifetime: timedelta,
        type_hint: type | None,
    ) -> None:
        """Create an iterator. Use ``fetch_records`` on a stream handle instead."""
        runtime = temporalio.workflow._Runtime.current()
        if not runtime.workflow_stream_reads_enabled():
            raise RuntimeError(
                "Workflow reads of streams are switched off in the client's stream backends "
                "configuration"
            )
        native = runtime.workflow_stream_backend_native(handle.backend)
        if hold_provisional and not native:
            raise ValueError(
                "hold_provisional needs the native backend, which writes commit records"
            )
        super().__init__(
            after=after,
            hold_provisional=native if hold_provisional is None else hold_provisional,
        )
        self._handle = handle
        self._native = native
        self._read_after = after
        self._topics = list(topics or ())
        self._page_size = page_size or 0
        self._wait_new_records = wait_new_records
        self._owner_wait = owner_wait
        self._close_wait = close_wait
        self._hold_timeout = hold_timeout
        self._redis_wait_lifetime = redis_wait_lifetime
        self._type_hint = type_hint
        self._hold_deadline: tuple[tuple[str, int], datetime] | None = None
        self._missing_since: datetime | None = None
        self._unknown_since: datetime | None = None
        self._owner_ended: tuple[CloseStatus, datetime] | None = None
        self._backoff: timedelta | None = None
        self._bytes_read = 0
        self._range_deadline: datetime | None = None

    async def _read(self) -> None:
        if self._continue_as_new_advisable():
            self.continue_as_new_suggested = True
            self._ended = True
            return
        if self.mode == StreamMode.COMMIT:
            recorded = await self._recorded_read()
            self._take(recorded)
            if not recorded.records and not self._ended:
                await self._at_end(None)
            return
        read = await self._local_read()
        if read.owner_not_found:
            await self._owner_missing()
            return
        self.mode = StreamMode(read.read.mode)
        if self.mode == StreamMode.COMMIT:
            return
        if self.mode == StreamMode.UNKNOWN:
            await self._wait_for_mode()
            return
        self._missing_since = self._unknown_since = self._backoff = None
        self._count(read.read)
        self._take(read.read)
        if not read.read.records and not self._ended:
            await self._at_end(
                read.notifier if read.HasField("notifier") else None, read
            )

    def _take(self, resp: ReadResponse) -> None:
        records = read_response(resp)
        if resp.cursor:
            self._read_after = StreamCursor(resp.cursor)
        judged = list(self._rule.judge(records.records))
        now = temporalio.workflow.now()
        held = self._rule.held()
        if held and (not self._hold_deadline or self._hold_deadline[0] != held):
            self._hold_deadline = (held, now + self._hold_timeout)
        if records.close:
            while self._rule.held():
                judged.extend(self._rule.release_held())
        elif held and self._hold_deadline and now >= self._hold_deadline[1]:
            judged.extend(self._rule.release_held())
        self._items.extend(
            decode_decoded(
                temporalio.workflow.payload_converter(), judged, self._type_hint
            )
        )
        if records.close:
            self.close = records.close
            self._ended = True

    async def _at_end(
        self,
        notifier: NotifierStateResponse | None,
        read: WorkflowReadResponse | None = None,
    ) -> None:
        """At the stream's end: end the iteration, or wait."""
        if self._owner_ended:
            status, deadline = self._owner_ended
            if temporalio.workflow.now() >= deadline:
                self.owner_end = status
                self._ended = True
            else:
                await self._sleep(_CLOSE_POLL_BACKOFF)
            return
        if not self._wait_new_records and not self._rule.held():
            self._ended = True
            return
        if notifier and notifier.owner_end:
            self._end_of_owner(CloseStatus(notifier.owner_end))
            return
        req = WaitRequest(stream=self._service_ref())
        if notifier and read:
            req.stream.CopyFrom(self._service_ref(read.owner_run_id))
            req.notifier.CopyFrom(notifier.position)
            await self._wait(req, self._redis_wait_lifetime)
        else:
            req.after = self._read_after or b""
            req.topics.extend(self._topics)
            await self._wait(req)

    async def _wait_for_mode(self) -> None:
        now = temporalio.workflow.now()
        self._unknown_since = self._unknown_since or now
        remaining = self._unknown_since + self._owner_wait - now
        if remaining <= timedelta(0):
            raise StreamModeUnknownError(
                f"The stream's mode is still unknown after {self._owner_wait}: its owner's first "
                "workflow task has not completed"
            )
        req = WaitRequest(stream=self._service_ref(), after=self._read_after or b"")
        await self._wait(req, remaining)

    async def _wait(self, req: WaitRequest, limit: timedelta = _MAX_WAIT) -> None:
        """Wait once, for at most the least of ``limit``, what is left of the hold, and the time
        until continue-as-new becomes advisable.
        """
        now = temporalio.workflow.now()
        lifetime = min(limit, _MAX_WAIT)
        if self._rule.held() and self._hold_deadline:
            lifetime = min(lifetime, self._hold_deadline[1] - now)
        if self._range_deadline:
            lifetime = min(lifetime, self._range_deadline - now)
        lifetime = max(lifetime, timedelta(seconds=1))
        req.lifetime.FromTimedelta(lifetime)
        try:
            resp = await self._operation(
                "Wait", req, WaitResponse, lifetime + timedelta(minutes=1)
            )
        except NexusOperationError:
            await self._sleep(_WAIT_FAILED_BACKOFF)
            return
        self._backoff = None
        if resp.reason == WakeReason.WAKE_REASON_OWNER_NOT_FOUND:
            await self._owner_missing()
        elif resp.reason == WakeReason.WAKE_REASON_OWNER_ENDED and resp.owner_end:
            self._end_of_owner(CloseStatus(resp.owner_end))

    def _end_of_owner(self, status: CloseStatus) -> None:
        self._owner_ended = (status, temporalio.workflow.now() + self._close_wait)

    async def _owner_missing(self) -> None:
        now = temporalio.workflow.now()
        self._unknown_since = None
        self._missing_since = self._missing_since or now
        if now - self._missing_since >= self._owner_wait:
            raise StreamNotFoundError(
                f"The stream's owner did not appear within {self._owner_wait}"
            )
        await self._sleep(
            _NOT_FOUND_BACKOFF, self._missing_since + self._owner_wait - now
        )

    async def _sleep(
        self, backoff: tuple[timedelta, timedelta], limit: timedelta = _MAX_WAIT
    ) -> None:
        """Sleep on a durable timer, doubling from the backoff's first to its second."""
        first, most = backoff
        self._backoff = first if self._backoff is None else min(self._backoff * 2, most)
        await temporalio.workflow.sleep(min(self._backoff, limit))

    async def _local_read(self) -> WorkflowReadResponse:
        req = WorkflowReadRequest(
            read=CrateReadRequest(
                stream=ref_proto(self._handle.ref),
                after=self._read_after or b"",
                topics=self._topics,
                max_records=self._page_size,
            ),
            notifier=not self._native and not self._owner_ended,
        )
        try:
            result = await temporalio.workflow.execute_local_activity(
                READ_ACTIVITY,
                read_request(self._handle.backend, req),
                result_type=RawValue,
                start_to_close_timeout=_READ_TIMEOUT,
                retry_policy=RetryPolicy(
                    non_retryable_error_types=[
                        name
                        for name, cls in vars(_errors).items()
                        if isinstance(cls, type)
                        and issubclass(cls, StreamError)
                        and cls is not StreamUnavailableError
                    ]
                ),
            )
        except ActivityError as err:
            raise _stream_error(err) from err
        return WorkflowReadResponse.FromString(result.payload.data)

    async def _recorded_read(self) -> ReadResponse:
        req = ReadRequest(
            stream=self._service_ref(),
            after=self._read_after or b"",
            topics=self._topics,
            max_records=self._page_size,
            max_bytes=_RECORDED_READ_MAX_BYTES,
        )
        try:
            return await self._operation("Read", req, ReadResponse, _READ_TIMEOUT)
        except NexusOperationError as err:
            raise _stream_error(err) from err

    async def _operation(
        self, name: str, input: Any, output_type: type[Any], timeout: timedelta
    ) -> Any:
        client = temporalio.workflow.create_nexus_client(
            endpoint="__temporal_system", service=_STREAM_SERVICE
        )
        handle = await client.start_operation(
            name,
            input,
            output_type=output_type,
            schedule_to_close_timeout=timeout,
        )
        return await handle

    def _service_ref(self, run_id: str = "") -> StreamRefProto:
        h = self._handle
        ref = StreamRefProto(
            workflow_id=h.workflow_id,
            first_run_id=h.first_run_id,
            activity_id=h.activity_id,
            activity_run_id=h.activity_run_id,
            name=h.name,
            stream_id=h.stream_id,
            namespace=temporalio.workflow.info().namespace,
        )
        if run_id and ref.activity_id:
            ref.activity_run_id = run_id
        elif run_id:
            ref.first_run_id = run_id
        return ref

    def _count(self, resp: ReadResponse) -> None:
        """Account a sink-mode read that replay must read again."""
        if not resp.records:
            return
        self._bytes_read += sum(r.payload.ByteSize() for r in resp.records)
        records = read_response(resp)
        if not records.retention:
            return
        start = records.owner_close_time or temporalio.workflow.now()
        deadline = start + records.retention / 2
        if not self._range_deadline or deadline < self._range_deadline:
            self._range_deadline = deadline

    def _continue_as_new_advisable(self) -> bool:
        return (
            temporalio.workflow.info().is_continue_as_new_suggested()
            or self._bytes_read > _CONTINUE_AS_NEW_BYTES
            or (
                self._range_deadline is not None
                and temporalio.workflow.now() >= self._range_deadline
            )
        )


def _stream_error(err: ActivityError | NexusOperationError) -> BaseException:
    """The stream error in a failed read's cause chain, or the failure itself."""
    cause = err.cause
    while cause:
        if isinstance(cause, ApplicationError) and cause.type:
            cls = getattr(_errors, cause.type, None)
            if isinstance(cls, type) and issubclass(cls, StreamError):
                return cls(cause.message)
            if cause.type.startswith("STREAM_FAILURE_TYPE_"):
                return stream_error(cause.type, cause.message)
        cause = (
            cause.cause
            if isinstance(cause, temporalio.exceptions.FailureError)
            else None
        )
    return err


@dataclass
class PendingStreamWrites:
    """What workflow code wrote to one stream that the worker has not yet passed to the backend:
    one of its own streams, by ``name``, or a standalone stream, by ``stream_id``. ``create`` is set
    when the worker must create the stream first.
    """

    backend: str
    name: str = ""
    stream_id: str = ""
    create: bool = False
    records: list[AppendRecord] = field(default_factory=list)
    close: StreamClose | None = None
