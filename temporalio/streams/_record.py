"""Producers and records."""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum

import temporalio.activity
import temporalio.api.common.v1


@dataclass(frozen=True)
class ClientProducer:
    """A client appending under an identity and epoch that it chooses."""

    id: str
    epoch: int


@dataclass(frozen=True)
class ActivityProducer:
    """An activity attempt. Its epoch is the attempt stamp, which a reset increases, and a retry
    increases only when the server enables ``system.enableActivityRetryStampIncrement``; otherwise
    a retry has its predecessor's epoch and does not fence it.

    A local activity's epoch is (the workflow task it was scheduled in, that task's attempt, its own
    attempt). The server cannot verify a local activity.
    """

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

    The epoch is (``workflow_task``, ``workflow_task_attempt``, ``epoch``), compared in that order.
    ``workflow_run_id`` and the workflow task fields are set only for workflow and local activity
    producers. For a workflow producer ``epoch`` is the workflow task attempt.
    """

    producer: str
    workflow_run_id: str
    workflow_task: int
    workflow_task_attempt: int
    epoch: int
    seq: int


@dataclass(frozen=True)
class AppendRecord:
    """A record to append."""

    topic: str
    payload: temporalio.api.common.v1.Payload


@dataclass(frozen=True)
class WorkflowTaskCommit:
    """Records which attempt of a workflow task committed. Only the native backend writes it.

    It is written for every completed workflow task of a run whose stream exists, and not for a
    rejected speculative task. See :py:class:`Record` for how a reader uses it.
    """

    workflow_run_id: str
    workflow_task: int
    attempt: int


@dataclass(frozen=True)
class StreamCreated:
    """Written when an append creates a native stream.

    The run's workflow tasks whose identity (``previous_started_event_id``) is lower than
    ``next_workflow_task`` completed before the stream existed, so they have no commit record; a
    reader treats a workflow or local activity record of such a task as committed. This is wrong for
    one case: a local activity from a failed attempt of such a task, still running after the
    rerun, has its records treated as committed.
    """

    workflow_run_id: str
    next_workflow_task: int


class WorkflowAppendMode(IntEnum):
    """How a run's workflow appends to its stream. A run's first workflow append fixes it."""

    SINK = 1
    """The worker appends at the end of each activation, before sending its completion."""
    COMMIT = 2
    """The records travel in the workflow task completion, and only the committed attempt's are
    delivered. Native only."""


@dataclass(frozen=True)
class WorkflowAppendModeFixed:
    """Records the append mode a run's first workflow append fixed. Only the native backend writes
    it.
    """

    workflow_run_id: str
    mode: WorkflowAppendMode


@dataclass(frozen=True)
class Record:
    """A stored record. ``cursor`` marks the boundary after it.

    A record is data, a producer's finish, or a marker: a workflow task commit, the stream's
    creation, or a run's append mode. Only the native backend writes markers.

    On native, a workflow or local activity record of workflow task T is final when T's commit
    record has the record's workflow task attempt, withdrawn when it has another attempt, final
    when T has no commit record and T is lower than the stream creation record's
    ``next_workflow_task``, and provisional otherwise. On other backends nothing withdraws a failed
    attempt's records if the attempt that succeeds appends nothing.
    """

    cursor: bytes
    producer: ProducerIdentity
    topic: str
    payload: temporalio.api.common.v1.Payload | None
    finish: bool
    workflow_task_commit: WorkflowTaskCommit | None = None
    stream_created: StreamCreated | None = None
    workflow_append_mode: WorkflowAppendModeFixed | None = None

    @property
    def is_marker(self) -> bool:
        """Whether this record is a marker rather than data or a finish."""
        return (
            self.workflow_task_commit is not None
            or self.stream_created is not None
            or self.workflow_append_mode is not None
        )
