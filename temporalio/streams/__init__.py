"""Streams owned by a workflow run chain or a Standalone Activity: ordered records that workflows,
activities and clients append, and that readers read from a cursor.

.. warning::
    This API is a prototype and will change.

A workflow appends to its own stream with :py:func:`workflow_append`. By default the worker
appends the records at the end of each activation, before sending its completion, using the
worker's ``stream_backend``; a failed workflow task attempt may already have delivered records.
With the native backend's commit mode, the records travel in the workflow task completion instead,
and only the committed attempt's records are delivered. In both modes the append is not a command
and adds nothing to history, and replay does not send the records again.

Every record carries the (producer, epoch, seq) of its append. A reader may receive records from a
lower epoch of a producer before records from a higher one, for example from a failed activity
attempt and then from its retry; the epoch tells the reader which to discard. A final failed
activity attempt has no successor, so its records are never superseded; a reader learns of it from
the activity's outcome.
"""

from __future__ import annotations

from temporalio.streams._backend import (
    CloseStatus,
    ReadResponse,
    StreamBackend,
    StreamClose,
    WorkflowActivationCompleted,
    WorkflowEnding,
)
from temporalio.streams._body import (
    decode,
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
from temporalio.streams._handles import (
    StreamWriter,
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
from temporalio.streams._ref import (
    StreamRef,
)
from temporalio.streams._workflow import (
    workflow_append,
)

__all__ = [
    "WorkflowAppendModeMismatchError",
    "WorkflowAppendModeFixed",
    "WorkflowAppendMode",
    "StreamCreated",
    "WorkflowEnding",
    "WorkflowActivationCompleted",
    "StreamUnavailableError",
    "StreamTooLargeError",
    "SeqGapError",
    "ActivityProducer",
    "AppendRecord",
    "ClientProducer",
    "CloseStatus",
    "EpochFencedError",
    "Producer",
    "ProducerIdentity",
    "ReadResponse",
    "Record",
    "SeqDivergentError",
    "SeqStaleError",
    "SpeculativeWorkflowTaskError",
    "StreamBackend",
    "StreamClose",
    "StreamClosedError",
    "StreamCursorError",
    "StreamError",
    "StreamExpiredError",
    "StreamNotFoundError",
    "StreamNotOwnerError",
    "StreamRef",
    "StreamWriter",
    "WorkflowProducer",
    "WorkflowTaskCommit",
    "decode",
    "workflow_append",
]
