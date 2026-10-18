"""Producers and records."""

from __future__ import annotations

from dataclasses import dataclass

import temporalio.activity
import temporalio.api.common.v1


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
class AppendRecord:
    """A record to append."""

    topic: str
    payload: temporalio.api.common.v1.Payload


@dataclass(frozen=True)
class WorkflowTaskCommit:
    """Records which attempt of a workflow task committed.

    Readers discard records of other attempts of that task. Only the native backend writes it.
    """

    producer: str
    workflow_task: int
    attempt: int


@dataclass(frozen=True)
class Record:
    """A stored record. ``cursor`` marks the boundary after it.

    A record is data, a producer's finish, or, when ``workflow_task_commit`` is set, a commit marker.
    """

    cursor: bytes
    producer: ProducerIdentity
    topic: str
    payload: temporalio.api.common.v1.Payload | None
    finish: bool
    workflow_task_commit: WorkflowTaskCommit | None = None
