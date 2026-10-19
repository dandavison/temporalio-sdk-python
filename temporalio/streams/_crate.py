"""A backend implemented by the temporalio-streams crate, through the bridge."""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Mapping, Sequence
from datetime import timedelta, timezone
from typing import TypeVar

from google.protobuf.message import Message

import temporalio.api.protocol.v1
import temporalio.bridge.temporal_sdk_bridge
from temporalio.api.nexusservices.streamservice.v1 import (
    AppendRecord as AppendRecordProto,
)
from temporalio.api.nexusservices.streamservice.v1 import (
    ReadResponse as ReadResponseProto,
)
from temporalio.api.nexusservices.streamservice.v1 import (
    Record as RecordProto,
)
from temporalio.api.nexusservices.streamservice.v1 import (
    StreamClose as StreamCloseProto,
)
from temporalio.bridge.proto.streams import (
    ActivityProducer as ActivityProducerProto,
)
from temporalio.bridge.proto.streams import (
    AppendRequest,
    AppendResponse,
    CloseRequest,
    CloseResponse,
    LocalActivityProducer,
    ReadRequest,
    WorkflowActivationCompletedRequest,
    WorkflowActivationCompletedResponse,
    WorkflowEvictedRequest,
    WorkflowEvictedResponse,
    WorkflowReadRequest,
    WorkflowReadResponse,
)
from temporalio.bridge.proto.streams import (
    ClientProducer as ClientProducerProto,
)
from temporalio.bridge.proto.streams import (
    Producer as ProducerProto,
)
from temporalio.bridge.proto.streams import (
    StreamRef as StreamRefProto,
)
from temporalio.bridge.proto.streams import (
    WorkflowEnding as WorkflowEndingProto,
)
from temporalio.bridge.proto.streams import (
    WorkflowProducer as WorkflowProducerProto,
)
from temporalio.streams._backend import (
    CloseStatus,
    ReadResponse,
    StreamBackend,
    StreamClose,
    StreamMode,
    WorkflowActivationCompleted,
)
from temporalio.streams._errors import (
    EpochFencedError,
    SeqDivergentError,
    SeqGapError,
    SeqStaleError,
    SpeculativeWorkflowTaskError,
    StreamClosedError,
    StreamCursorError,
    StreamError,
    StreamExpiredError,
    StreamNotFoundError,
    StreamNotOwnerError,
    StreamTooLargeError,
    StreamUnavailableError,
    WorkflowAppendModeMismatchError,
)
from temporalio.streams._record import (
    ActivityProducer,
    AppendRecord,
    ClientProducer,
    Producer,
    ProducerIdentity,
    Record,
    StreamCreated,
    WorkflowAppendMode,
    WorkflowAppendModeFixed,
    WorkflowProducer,
    WorkflowTaskCommit,
)
from temporalio.streams._ref import StreamRef

_ERRORS: dict[str, type[StreamError]] = {
    "STREAM_FAILURE_TYPE_CLOSED": StreamClosedError,
    "STREAM_FAILURE_TYPE_NOT_FOUND": StreamNotFoundError,
    "STREAM_FAILURE_TYPE_EXPIRED": StreamExpiredError,
    "STREAM_FAILURE_TYPE_NOT_OWNER": StreamNotOwnerError,
    "STREAM_FAILURE_TYPE_CURSOR_MISMATCH": StreamCursorError,
    "STREAM_FAILURE_TYPE_SEQ_DIVERGENT": SeqDivergentError,
    "STREAM_FAILURE_TYPE_SEQ_STALE": SeqStaleError,
    "STREAM_FAILURE_TYPE_EPOCH_FENCED": EpochFencedError,
    "STREAM_FAILURE_TYPE_SPECULATIVE_WORKFLOW_TASK": SpeculativeWorkflowTaskError,
    "STREAM_FAILURE_TYPE_SEQ_GAP": SeqGapError,
    "STREAM_FAILURE_TYPE_TOO_LARGE": StreamTooLargeError,
    "STREAM_FAILURE_TYPE_WORKFLOW_APPEND_MODE_MISMATCH": WorkflowAppendModeMismatchError,
    "UNAVAILABLE": StreamUnavailableError,
}

_M = TypeVar("_M", bound=Message)


class CrateStreamBackend(StreamBackend):
    """A backend whose every call is made by the temporalio-streams crate."""

    @abstractmethod
    async def _streams(self) -> temporalio.bridge.temporal_sdk_bridge.StreamsRef:
        """The bridge streams object, created on first use."""

    async def append(
        self,
        ref: StreamRef,
        producer: Producer,
        seq: int,
        records: Sequence[AppendRecord],
        *,
        finish: bool = False,
    ) -> Sequence[bytes]:
        """See :py:meth:`StreamBackend.append`."""
        req = AppendRequest(
            stream=ref_proto(ref),
            producer=_producer(producer),
            seq=seq,
            records=_records(records),
            finish=finish,
        )
        return list((await self._call("Append", req, AppendResponse)).cursors)

    async def close(
        self,
        ref: StreamRef,
        owner: WorkflowProducer | ActivityProducer,
        seq: int,
        close: StreamClose,
    ) -> None:
        """See :py:meth:`StreamBackend.close`."""
        req = CloseRequest(stream=ref_proto(ref), seq=seq, close=_close_proto(close))
        if isinstance(owner, ActivityProducer):
            req.activity_owner.CopyFrom(_activity_producer(owner).activity)
        # The crate's workflow producer has no workflow ID, so the owner's workflow is checked here.
        elif owner.workflow_id == ref.workflow_id:
            req.owner.CopyFrom(_workflow_producer(owner))
        else:
            raise StreamNotOwnerError("only the stream's owner may close it")
        await self._call("Close", req, CloseResponse)

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
        """See :py:meth:`StreamBackend.read`."""
        req = ReadRequest(
            stream=ref_proto(ref),
            after=after or b"",
            topics=topics,
            max_records=max_records,
            last=last,
            until=until or b"",
            max_bytes=max_bytes,
            rpc_metadata={
                k: v.encode() if isinstance(v, str) else v
                for k, v in rpc_metadata.items()
            },
        )
        if wait > timedelta(0):
            req.wait.FromTimedelta(wait)
        if rpc_timeout is not None:
            req.rpc_timeout.FromTimedelta(rpc_timeout)
        return read_response(await self._call("Read", req, ReadResponseProto))

    async def workflow_read(self, request: WorkflowReadRequest) -> WorkflowReadResponse:
        """See :py:meth:`StreamBackend.workflow_read`."""
        return await self._call("WorkflowRead", request, WorkflowReadResponse)

    async def workflow_activation_completed(
        self, completed: WorkflowActivationCompleted
    ) -> Sequence[temporalio.api.protocol.v1.Message]:
        """See :py:meth:`StreamBackend.workflow_activation_completed`."""
        p = completed.producer
        req = WorkflowActivationCompletedRequest(
            workflow_id=completed.ref.workflow_id,
            run_id=p.run_id,
            first_run_id=completed.ref.first_run_id,
            previous_started_event_id=p.previous_started_event_id,
            attempt=p.attempt,
            records=_records(completed.records),
            commit_mode=completed.commit_mode,
            declare_mode=completed.declare_mode,
        )
        if completed.close:
            req.close.CopyFrom(_close_proto(completed.close))
        if e := completed.ending:
            req.ending.CopyFrom(
                WorkflowEndingProto(
                    close=_close_proto(e.close),
                    cron=e.cron,
                    retry_policy=e.retry_policy,
                )
            )
        resp = await self._call(
            "WorkflowActivationCompleted", req, WorkflowActivationCompletedResponse
        )
        return list(resp.protocol_messages)

    async def workflow_evicted(self, run_id: str, *, execution_ending: bool) -> None:
        """See :py:meth:`StreamBackend.workflow_evicted`."""
        req = WorkflowEvictedRequest(run_id=run_id, execution_ending=execution_ending)
        await self._call("WorkflowEvicted", req, WorkflowEvictedResponse)

    async def _call(self, name: str, req: Message, response_type: type[_M]) -> _M:
        try:
            streams = await self._streams()
            data = await streams.call(name, req.SerializeToString())
        except temporalio.bridge.temporal_sdk_bridge.StreamError as err:
            type_name, message = err.args
            raise stream_error(type_name, message) from None
        return response_type.FromString(bytes(data))


def read_response(resp: ReadResponseProto) -> ReadResponse:
    """A read response of the stream service, as the backend interface's."""
    return ReadResponse(
        records=[_record(r) for r in resp.records],
        cursor=resp.cursor or None,
        close=_close(resp.close) if resp.HasField("close") else None,
        mode=StreamMode(resp.mode),
        owner_close_time=resp.owner_close_time.ToDatetime(timezone.utc)
        if resp.HasField("owner_close_time")
        else None,
        retention=resp.retention.ToTimedelta() if resp.HasField("retention") else None,
    )


def stream_error(type_name: str, message: str) -> StreamError:
    """The stream error for a StreamFailureType value's name."""
    return _ERRORS.get(type_name, StreamError)(message)


def ref_proto(ref: StreamRef) -> StreamRefProto:
    """A stream reference, as the crate's."""
    return StreamRefProto(
        workflow_id=ref.workflow_id,
        first_run_id=ref.first_run_id,
        activity_id=ref.activity_id,
        activity_run_id=ref.activity_run_id,
    )


def _records(records: Sequence[AppendRecord]) -> list[AppendRecordProto]:
    return [AppendRecordProto(topic=r.topic, payload=r.payload) for r in records]


def _producer(p: Producer) -> ProducerProto:
    if isinstance(p, ClientProducer):
        return ProducerProto(client=ClientProducerProto(id=p.id, epoch=p.epoch))
    if isinstance(p, ActivityProducer):
        return _activity_producer(p)
    return ProducerProto(workflow=_workflow_producer(p))


def _activity_producer(p: ActivityProducer) -> ProducerProto:
    info = p.info
    if info.is_local:
        return ProducerProto(
            local_activity=LocalActivityProducer(
                run_id=info.workflow_run_id or "",
                previous_started_event_id=info.workflow_task_previous_started_event_id,
                workflow_task_attempt=info.workflow_task_attempt,
                activity_id=info.activity_id,
                attempt=info.attempt,
            )
        )
    return ProducerProto(
        activity=ActivityProducerProto(
            task_token=info.task_token,
            workflow_run_id=info.workflow_run_id or "",
            activity_id=info.activity_id,
            activity_run_id=info.activity_run_id or "",
            attempt_stamp=info.attempt_stamp,
        )
    )


def _workflow_producer(p: WorkflowProducer) -> WorkflowProducerProto:
    return WorkflowProducerProto(
        run_id=p.run_id,
        previous_started_event_id=p.previous_started_event_id,
        attempt=p.attempt,
    )


def _record(r: RecordProto) -> Record:
    commit = r.workflow_task_commit if r.HasField("workflow_task_commit") else None
    created = r.stream_created if r.HasField("stream_created") else None
    mode = r.workflow_append_mode if r.HasField("workflow_append_mode") else None
    return Record(
        cursor=r.cursor,
        producer=ProducerIdentity(
            producer=r.producer.producer,
            workflow_run_id=r.producer.workflow_run_id,
            workflow_task=r.producer.workflow_task,
            workflow_task_attempt=r.producer.workflow_task_attempt,
            epoch=r.producer.epoch,
        ),
        seq=r.producer.seq,
        topic=r.topic,
        payload=r.payload if r.HasField("payload") else None,
        finish=r.finish,
        workflow_task_commit=WorkflowTaskCommit(
            workflow_run_id=commit.workflow_run_id,
            workflow_task=commit.workflow_task,
            attempt=commit.attempt,
        )
        if commit
        else None,
        stream_created=StreamCreated(
            workflow_run_id=created.workflow_run_id,
            next_workflow_task=created.next_workflow_task,
        )
        if created
        else None,
        workflow_append_mode=WorkflowAppendModeFixed(
            workflow_run_id=mode.workflow_run_id,
            mode=WorkflowAppendMode(mode.mode),
        )
        if mode
        else None,
    )


def _close_proto(close: StreamClose) -> StreamCloseProto:
    return StreamCloseProto(
        state=int(close.status),  # type: ignore[arg-type]
        failure=close.failure,
        reason=close.reason,
    )


def _close(c: StreamCloseProto) -> StreamClose:
    return StreamClose(
        status=CloseStatus(c.state),
        failure=c.failure if c.HasField("failure") else None,
        reason=c.reason,
    )
