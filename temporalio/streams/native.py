"""A :py:class:`temporalio.streams.StreamBackend` that stores streams in Temporal.

.. warning::
    This API is a prototype and will change.

The stream is embedded in its owning workflow. Every operation is a direct Nexus call to the stream
service on the System Nexus Endpoint, which the server runs in a transaction on the owning
workflow. The server checks activity and workflow producers against the workflow's state, records
which attempt of each workflow task committed, and closes the stream when the workflow ends.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from typing import TypeVar

from google.protobuf.message import Message

import temporalio.api.common.v1
import temporalio.api.failure.v1
import temporalio.api.workflowservice.v1
import temporalio.client
from temporalio.api.nexusservices.streamservice.v1 import (
    AppendRecord as AppendRecordProto,
)
from temporalio.api.nexusservices.streamservice.v1 import (
    AppendRequest,
    AppendResponse,
    CloseRequest,
    CloseResponse,
    ReadRequest,
    StreamFailureType,
)
from temporalio.api.nexusservices.streamservice.v1 import (
    ClientProducer as ClientProducerProto,
)
from temporalio.api.nexusservices.streamservice.v1 import (
    Producer as ProducerProto,
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
from temporalio.api.nexusservices.streamservice.v1 import (
    StreamRef as StreamRefProto,
)
from temporalio.api.nexusservices.streamservice.v1 import (
    WorkflowTaskProducer as WorkflowTaskProducerProto,
)
from temporalio.streams import (
    ActivityProducer,
    AppendRecord,
    ClientProducer,
    CloseStatus,
    EpochFencedError,
    Producer,
    ProducerIdentity,
    ReadResponse,
    Record,
    SeqDivergentError,
    SeqStaleError,
    SpeculativeWorkflowTaskError,
    StreamBackend,
    StreamClose,
    StreamClosedError,
    StreamCursorError,
    StreamError,
    StreamExpiredError,
    StreamNotFoundError,
    StreamNotOwnerError,
    StreamRef,
    WorkflowProducer,
    WorkflowTaskCommit,
)

_SYSTEM_ENDPOINT = "__temporal_system"
_SERVICE = "temporal.api.nexusservices.streamservice.v1.StreamService"

_ERRORS: dict[int, type[StreamError]] = {
    StreamFailureType.STREAM_FAILURE_TYPE_CLOSED: StreamClosedError,
    StreamFailureType.STREAM_FAILURE_TYPE_NOT_FOUND: StreamNotFoundError,
    StreamFailureType.STREAM_FAILURE_TYPE_EXPIRED: StreamExpiredError,
    StreamFailureType.STREAM_FAILURE_TYPE_NOT_OWNER: StreamNotOwnerError,
    StreamFailureType.STREAM_FAILURE_TYPE_CURSOR_MISMATCH: StreamCursorError,
    StreamFailureType.STREAM_FAILURE_TYPE_SEQ_DIVERGENT: SeqDivergentError,
    StreamFailureType.STREAM_FAILURE_TYPE_SEQ_STALE: SeqStaleError,
    StreamFailureType.STREAM_FAILURE_TYPE_EPOCH_FENCED: EpochFencedError,
    StreamFailureType.STREAM_FAILURE_TYPE_SPECULATIVE_WORKFLOW_TASK: SpeculativeWorkflowTaskError,
}

_M = TypeVar("_M", bound=Message)


class NativeStreamBackend(StreamBackend):
    """Stores streams in Temporal, embedded in their owning workflows."""

    def __init__(self, client: temporalio.client.Client) -> None:
        """Create a backend that calls the server through the client."""
        self._client = client

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
            stream=_ref(ref),
            producer=_producer(producer),
            seq=seq,
            records=[
                AppendRecordProto(topic=r.topic, payload=r.payload) for r in records
            ],
            finish=finish,
        )
        resp = await self._call("Append", req, AppendResponse)
        return list(resp.cursors)

    async def close(
        self, ref: StreamRef, owner: WorkflowProducer, seq: int, close: StreamClose
    ) -> None:
        """See :py:meth:`StreamBackend.close`."""
        if owner.workflow_id != ref.workflow_id:
            raise StreamNotOwnerError("only the stream's owner may close it")
        req = CloseRequest(
            stream=_ref(ref),
            owner=_workflow_task_producer(owner),
            seq=seq,
            close=StreamCloseProto(
                state=int(close.status),  # type: ignore[arg-type]
                failure=close.failure,
                reason=close.reason,
            ),
        )
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
    ) -> ReadResponse:
        """See :py:meth:`StreamBackend.read`."""
        req = ReadRequest(
            stream=_ref(ref),
            after=after or b"",
            topics=topics,
            max_records=max_records,
            last=last,
        )
        if wait > timedelta(0):
            req.wait.FromTimedelta(wait)
        resp = await self._call("Read", req, ReadResponseProto)
        return ReadResponse(
            records=[_record(r) for r in resp.records],
            cursor=resp.cursor or None,
            close=_close(resp.close) if resp.HasField("close") else None,
        )

    async def _call(self, operation: str, req: Message, response_type: type[_M]) -> _M:
        resp = await self._client.workflow_service.start_nexus_operation(
            temporalio.api.workflowservice.v1.StartNexusOperationRequest(
                namespace=self._client.namespace,
                endpoint=_SYSTEM_ENDPOINT,
                service=_SERVICE,
                operation=operation,
                input=temporalio.api.common.v1.Payload(
                    metadata={
                        "encoding": b"binary/protobuf",
                        "messageType": req.DESCRIPTOR.full_name.encode(),
                    },
                    data=req.SerializeToString(),
                ),
            )
        )
        if resp.HasField("operation_failure"):
            raise _error(resp.operation_failure.failure)
        out = response_type()
        out.ParseFromString(resp.sync_success.payload.data)
        return out


def _ref(ref: StreamRef) -> StreamRefProto:
    return StreamRefProto(workflow_id=ref.workflow_id, first_run_id=ref.first_run_id)


def _producer(p: Producer) -> ProducerProto:
    if isinstance(p, ClientProducer):
        return ProducerProto(client=ClientProducerProto(id=p.id, epoch=p.epoch))
    if isinstance(p, ActivityProducer):
        return ProducerProto(activity_task_token=p.info.task_token)
    return ProducerProto(workflow_task=_workflow_task_producer(p))


def _workflow_task_producer(p: WorkflowProducer) -> WorkflowTaskProducerProto:
    return WorkflowTaskProducerProto(
        run_id=p.run_id,
        previous_started_event_id=p.previous_started_event_id,
        attempt=p.attempt,
    )


def _record(r: RecordProto) -> Record:
    commit = r.workflow_task_commit if r.HasField("workflow_task_commit") else None
    return Record(
        cursor=r.cursor,
        producer=ProducerIdentity(
            producer=r.producer.producer,
            workflow_task=r.producer.workflow_task,
            epoch=r.producer.epoch,
            seq=r.producer.seq,
        ),
        topic=r.topic,
        payload=r.payload if r.HasField("payload") else None,
        finish=r.finish,
        workflow_task_commit=WorkflowTaskCommit(
            producer=commit.producer,
            workflow_task=commit.workflow_task,
            attempt=commit.attempt,
        )
        if commit
        else None,
    )


def _close(c: StreamCloseProto) -> StreamClose:
    return StreamClose(
        status=CloseStatus(c.state),
        failure=c.failure if c.HasField("failure") else None,
        reason=c.reason,
    )


def _error(failure: temporalio.api.failure.v1.Failure) -> StreamError:
    """Map an operation failure to the stream error named by an ApplicationFailureInfo type in its
    cause chain.
    """
    f: temporalio.api.failure.v1.Failure | None = failure
    while f is not None:
        error_type = f.application_failure_info.type
        if error_type in StreamFailureType.keys():
            cls = _ERRORS.get(StreamFailureType.Value(error_type))
            if cls:
                return cls(f.message)
        f = f.cause if f.HasField("cause") else None
    return StreamError(failure.message)
