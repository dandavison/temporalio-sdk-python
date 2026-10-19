"""Streams owned by a workflow run chain or a Standalone Activity: ordered records that workflows,
activities and clients publish, and that readers read from a cursor.

.. warning::
    This API is a prototype and will change.

Handles by context:

* A client gets a :py:class:`StreamHandle` from ``stream()`` on a workflow or activity handle, or
  from :py:meth:`temporalio.client.Client.get_stream_handle`. It reads, and publishes through
  named producers (:py:meth:`StreamHandle.producer`).
* Workflow code gets its own stream's :py:class:`WorkflowStreamHandle` from
  :py:func:`temporalio.workflow.stream`. Reading in workflow code is not implemented yet.
* Activity code gets an :py:class:`ActivityStreamHandle` from :py:func:`temporalio.activity.stream`:
  its workflow's stream for a workflow or local activity, and its own for a Standalone Activity.

Handles carry no type. A value's type is given per topic, with ``topic(name, type=T)``, or per
read, with ``type_hint=``; otherwise values decode as the payload converter's default.

A client and its workers store streams in Temporal (native) unless a plugin, such as
:py:class:`temporalio.contrib.redis_streams.RedisStreamsPlugin`, gives the client another backend.

Every record carries the producer and epoch of its append. A failed workflow task attempt, or an
activity attempt or client incarnation superseded by a higher epoch, may already have delivered
records. A reader receives each record with its status, and a :py:class:`Withdrawal` notice if a
record it received is withdrawn later (see :py:class:`StreamRecordAsyncIterator`). A final failed
activity attempt has no successor, so its records are never withdrawn; a reader learns of it from
the activity's outcome.
"""

from __future__ import annotations

from temporalio.streams._backend import (
    CloseStatus,
    StreamClose,
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
    ActivityStreamHandle,
    PublishingTopicHandle,
    StreamHandle,
    StreamProducer,
    StreamReader,
    TopicReader,
)
from temporalio.streams._iterator import (
    RecordStatus,
    StreamCursor,
    StreamMode,
    StreamRecord,
    StreamRecordAsyncIterator,
    Withdrawal,
)
from temporalio.streams._record import (
    ProducerIdentity,
)
from temporalio.streams._workflow import (
    WorkflowStreamHandle,
    WorkflowTopicHandle,
)

__all__ = [
    "ActivityStreamHandle",
    "CloseStatus",
    "EpochFencedError",
    "ProducerIdentity",
    "PublishingTopicHandle",
    "RecordStatus",
    "SeqDivergentError",
    "SeqGapError",
    "SeqStaleError",
    "SpeculativeWorkflowTaskError",
    "StreamClose",
    "StreamClosedError",
    "StreamCursor",
    "StreamCursorError",
    "StreamError",
    "StreamExpiredError",
    "StreamHandle",
    "StreamMode",
    "StreamNotFoundError",
    "StreamNotOwnerError",
    "StreamProducer",
    "StreamReader",
    "StreamRecord",
    "StreamRecordAsyncIterator",
    "StreamTooLargeError",
    "StreamUnavailableError",
    "TopicReader",
    "Withdrawal",
    "WorkflowAppendModeMismatchError",
    "WorkflowStreamHandle",
    "WorkflowTopicHandle",
]
