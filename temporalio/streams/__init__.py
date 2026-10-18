"""Streams owned by workflows: ordered records that workflows, activities and clients append, and
that readers read from a cursor.

.. warning::
    This API is a prototype and will change.

A workflow appends to its own stream with :py:func:`workflow_append`. The worker appends the
records at the end of the activation, before completing it, using the worker's
``stream_backend``. The append is not a command and adds nothing to history, so replay does not
repeat it.

Every record carries the (producer, epoch, seq) of its append. A reader may receive records from a
lower epoch of a producer before records from a higher one, for example from a failed activity
attempt and then from its retry; the epoch tells the reader which to discard.
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
