"""The worker's side of workflow reads: the read local activity, the swap of a read's records into
its result before workflow code sees it, the replay check, and the read budget per workflow task.

A read of a sink-mode stream records in History only its range and a digest, as the result of the
SDK's read local activity (a WorkflowReadMarker, with the name of the stream's backend in the
payload's metadata). The activity leaves the records in this process, keyed by run and digest; when
its result reaches the workflow, the worker puts them back. On replay they are not there, so the
worker reads the range again, on the backend the marker names, and checks it against the digest. Records
are stored as the producers' codec encoded them, so the worker decodes them before workflow code,
which cannot run the codec, sees them. (The records of a recorded Read operation, a system Nexus
payload, are decoded with the rest of the activation.)
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import temporalio.activity
import temporalio.api.common.v1
import temporalio.bridge.proto.workflow_activation
import temporalio.bridge.proto.workflow_completion
import temporalio.converter
import temporalio.workflow
from temporalio.api.nexusservices.streamservice.v1 import (
    ReadResponse,
    Record,
    StreamMode,
)
from temporalio.bridge.proto.streams import (
    WorkflowReadMarker,
    WorkflowReadRequest,
)
from temporalio.common import RawValue
from temporalio.exceptions import ApplicationError
from temporalio.streams._backend import StreamBackend
from temporalio.streams._errors import (
    StreamError,
    StreamExpiredError,
    StreamNotFoundError,
    StreamUnavailableError,
)

READ_ACTIVITY = "__temporal_stream_read"
"""The name of the read local activity, which every worker with workflows registers."""

_MARKER_METADATA = "__temporal_stream_read"
_BACKEND_METADATA = "__temporal_stream_backend"

# Records a read local activity read, until its result reaches the workflow: by run ID and the
# read's digest.
_pending: dict[tuple[str, bytes], Sequence[Record]] = {}

READ_BUDGET_READS = 10
READ_BUDGET_SECONDS = 1.0


def read_request(backend: str, request: WorkflowReadRequest) -> RawValue:
    """The read local activity's input: a read of a stream on the backend named ``backend``."""
    return RawValue(
        temporalio.api.common.v1.Payload(
            metadata={"encoding": b"binary/plain", _BACKEND_METADATA: backend.encode()},
            data=request.SerializeToString(),
        )
    )


@temporalio.activity.defn(name=READ_ACTIVITY)
async def read_stream(request: RawValue) -> RawValue:
    """Read a stream for workflow code, returning a WorkflowReadMarker: the request, and the
    response without its records, which stay in this process. When the stream's mode is commit or
    unknown, the read keeps no records and does not advance the cursor: workflow code then reads
    with recorded Read operations, or waits.
    """
    req = WorkflowReadRequest.FromString(request.payload.data)
    backend_name = request.payload.metadata[_BACKEND_METADATA].decode()
    backend = _backend(backend_name)
    try:
        resp = await backend.workflow_read(req)
    except StreamError as err:
        raise ApplicationError(
            str(err),
            type=type(err).__name__,
            non_retryable=not isinstance(err, StreamUnavailableError),
        ) from err
    records = list(resp.read.records)
    del resp.read.records[:]
    if resp.read.mode != StreamMode.STREAM_MODE_SINK:
        records = []
        resp.read.cursor = req.read.after
    if records:
        _pending[temporalio.activity.info().workflow_run_id or "", resp.digest] = (
            records
        )
    marker = WorkflowReadMarker(request=req, response=resp, record_count=len(records))
    return RawValue(
        temporalio.api.common.v1.Payload(
            metadata={
                "encoding": b"binary/plain",
                _MARKER_METADATA: b"1",
                _BACKEND_METADATA: backend_name.encode(),
            },
            data=marker.SerializeToString(),
        )
    )


def _backend(name: str) -> StreamBackend:
    from temporalio.streams.backend import client_backend

    return client_backend(temporalio.activity.client(), name)


async def resolve_reads(
    act: temporalio.bridge.proto.workflow_activation.WorkflowActivation,
    backends: Mapping[str, StreamBackend] | None,
    data_converter: temporalio.converter.DataConverter,
) -> None:
    """Put the records of the activation's resolved read local activities into their results,
    codec-decoded: from this process, or on replay from the store, checked against the recorded
    digest.

    Raises an application failure of type STREAM_RANGE_GONE if a range to read again is no longer
    in the store, and a nondeterminism error if it reads differently.
    """
    for job in act.jobs:
        if job.HasField("resolve_activity"):
            result = job.resolve_activity.result
            if not result.HasField("completed"):
                continue
            payload = result.completed.result
            if payload.metadata.get(_MARKER_METADATA) != b"1":
                continue
            marker = WorkflowReadMarker.FromString(payload.data)
            backend_name = payload.metadata[_BACKEND_METADATA].decode()
            backend = (backends or {}).get(backend_name)
            records = await _records(act.run_id, marker, backend)
            response = marker.response
            response.read.records.extend(records)
            await _decode_records(response.read, data_converter)
            payload.CopyFrom(
                temporalio.api.common.v1.Payload(
                    metadata={"encoding": b"binary/plain"},
                    data=response.SerializeToString(),
                )
            )


def forget_run(run_id: str) -> None:
    """Drop the records left for an evicted run."""
    for key in [k for k in _pending if k[0] == run_id]:
        del _pending[key]


async def _records(
    run_id: str, marker: WorkflowReadMarker, backend: StreamBackend | None
) -> Sequence[Record]:
    if not marker.record_count:
        return []
    records = _pending.pop((run_id, marker.response.digest), None)
    if records is not None:
        return records
    if backend is None:
        raise RuntimeError(
            "Replaying a workflow read of a sink-mode stream needs the stream's backend, which "
            "this worker does not have"
        )
    req = WorkflowReadRequest()
    req.CopyFrom(marker.request)
    req.read.until = marker.response.read.cursor
    req.notifier = False
    try:
        resp = await backend.workflow_read(req)
    except (StreamExpiredError, StreamNotFoundError) as err:
        raise _range_gone(str(err)) from err
    if resp.digest == marker.response.digest:
        return list(resp.read.records)
    if len(resp.read.records) < marker.record_count:
        raise _range_gone(
            f"the recorded read had {marker.record_count} records; the store now has "
            f"{len(resp.read.records)}"
        )
    raise temporalio.workflow.NondeterminismError(
        "A recorded stream read reads different records on replay"
    )


def _range_gone(detail: str) -> ApplicationError:
    return ApplicationError(
        "A recorded stream read's range is no longer in the store, so the workflow cannot "
        f"replay: {detail}. Reset the run to a point after the read, or terminate it.",
        type="STREAM_RANGE_GONE",
    )


async def _decode_records(
    read: ReadResponse, data_converter: temporalio.converter.DataConverter
) -> None:
    with_payloads = [r for r in read.records if r.HasField("payload")]
    decoded = await data_converter._decode_payload_sequence(  # type: ignore[reportPrivateUsage]
        [r.payload for r in with_payloads]
    )
    for r, payload in zip(with_payloads, decoded):
        r.payload.CopyFrom(payload)


@dataclass
class _Budget:
    workflow_task: tuple[int, int]
    reads: int
    started: float
    # Reads scheduled in this task that complete in the next, which ended this one.
    carried: int = 0


class ReadBudgets:
    """The read budget of each run's current workflow task: at most READ_BUDGET_READS reads, or
    READ_BUDGET_SECONDS, after which the worker asks Core to end the task, so the next read runs in
    the next task. Time is the worker's clock; the budget changes no command, so replay need not
    repeat it.
    """

    def __init__(self) -> None:
        """Create budgets for no runs."""
        self._runs: dict[str, _Budget] = {}

    def spend(
        self,
        act: temporalio.bridge.proto.workflow_activation.WorkflowActivation,
        completion: temporalio.bridge.proto.workflow_completion.WorkflowActivationCompletion,
    ) -> None:
        """Count the reads the completion schedules, and end the task if they exceed the budget."""
        if act.is_replaying or not completion.HasField("successful"):
            return
        reads = sum(
            c.schedule_local_activity.activity_type == READ_ACTIVITY
            for c in completion.successful.commands
            if c.HasField("schedule_local_activity")
        )
        if not reads:
            return
        task = (
            act.workflow_task.previous_started_event_id,
            act.workflow_task.attempt,
        )
        budget = self._runs.get(act.run_id)
        if not budget or budget.workflow_task != task:
            carried = budget.carried if budget else 0
            budget = self._runs[act.run_id] = _Budget(task, carried, time.monotonic())
        budget.reads += reads
        if (
            budget.reads > READ_BUDGET_READS
            or time.monotonic() - budget.started > READ_BUDGET_SECONDS
        ):
            completion.successful.force_new_workflow_task = True
            budget.carried = reads

    def forget(self, run_id: str) -> None:
        """Drop an evicted run's budget."""
        self._runs.pop(run_id, None)
