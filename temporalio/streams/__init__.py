"""Streams: ordered records that workflows, activities and clients publish, and that readers read
from a cursor. A stream is one of the named streams of a workflow run chain or a Standalone
Activity, its owner, or a standalone stream, which has no owner.

.. warning::
    This API is a prototype and will change.

There are no implicit streams: a writer gets a handle by creating the stream, and a reader gets one
by name or from a handle passed to it. Handles are values: pass them in workflow and activity
inputs, and through continue-as-new. Each context has its own kind of handle, made from another
with that context's ``get_stream_handle``:

* A client creates a standalone stream with :py:meth:`temporalio.client.Client.create_stream`, and
  gets a :py:class:`StreamHandle` by name from ``stream(name)`` on a workflow or activity handle, or
  from :py:meth:`temporalio.client.Client.get_stream_handle`. It reads, publishes through named
  producers (:py:meth:`StreamHandle.producer`), and closes standalone streams.
* Workflow code creates its own streams with :py:func:`temporalio.workflow.create_stream`, and
  gets a :py:class:`WorkflowStreamHandle` to others with
  :py:func:`temporalio.workflow.get_stream_handle`. It publishes to its own streams and to
  standalone streams. Reading in workflow code is not implemented yet.
* Activity code gets an :py:class:`ActivityStreamHandle` with
  :py:func:`temporalio.activity.get_stream_handle`, for example from a handle its workflow passed
  it, and a Standalone Activity creates its own streams with
  :py:func:`temporalio.activity.create_stream`. A synchronous activity in a thread pool uses
  :py:class:`SyncActivityStreamHandle`.

Publishing is awaited: ``await publish(*values, topic=...)`` returns once the store has the
records (in workflow code, see :py:class:`WorkflowStreamHandle`). ``buffered()`` on a client
producer or an activity handle gives a :py:class:`BufferedStreamWriter`, which batches. A created
stream's ``create_stream`` result is also an async context manager, which closes the stream on exit.

Handles carry no type. A value's type is given per topic, with ``topic(name, type=T)``, or per
read, with ``type_hint=``; otherwise values decode as the payload converter's default.

A client has the native backend, which stores streams in Temporal, and the backends its
:py:class:`StreamBackends` configuration lists, such as
:py:class:`temporalio.contrib.redis_streams.RedisStreamBackend`; its workers use the client's. A
handle records its stream's backend.

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
    StreamBackends,
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
    StreamSettingsMismatchError,
    StreamTooLargeError,
    StreamUnavailableError,
    WorkflowAppendModeMismatchError,
)
from temporalio.streams._handles import (
    ActivityStreamHandle,
    Creating,
    PublishingTopicHandle,
    StreamHandle,
    StreamHandleBase,
    StreamProducer,
    StreamReader,
    SyncActivityStreamHandle,
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
from temporalio.streams._publisher import BufferedStreamWriter
from temporalio.streams._record import (
    ProducerIdentity,
)
from temporalio.streams._workflow import (
    WorkflowStreamHandle,
    WorkflowTopicHandle,
)

__all__ = [
    "ActivityStreamHandle",
    "BufferedStreamWriter",
    "CloseStatus",
    "Creating",
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
    "StreamBackends",
    "StreamHandle",
    "StreamHandleBase",
    "StreamMode",
    "StreamNotFoundError",
    "StreamNotOwnerError",
    "StreamProducer",
    "StreamReader",
    "StreamRecord",
    "StreamRecordAsyncIterator",
    "StreamSettingsMismatchError",
    "StreamTooLargeError",
    "StreamUnavailableError",
    "SyncActivityStreamHandle",
    "TopicReader",
    "Withdrawal",
    "WorkflowAppendModeMismatchError",
    "WorkflowStreamHandle",
    "WorkflowTopicHandle",
]
