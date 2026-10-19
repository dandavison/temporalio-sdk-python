"""The interface a stream store implements, for plugins that provide one, and the records it
stores. Not application API: applications use the handles in :py:mod:`temporalio.streams`.

.. warning::
    This API is a prototype and will change.

A plugin gives a client a backend by setting ``stream_backend`` in its
:py:class:`temporalio.client.ClientConfig`; the client's workers use the client's backend. A
client with none uses :py:class:`temporalio.streams.native.NativeStreamBackend`.
"""

from __future__ import annotations

import weakref

import temporalio.client
from temporalio.streams._backend import (
    CloseStatus,
    ReadResponse,
    StreamBackend,
    StreamClose,
    WorkflowActivationCompleted,
    WorkflowEnding,
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
from temporalio.streams.native import NativeStreamBackend

__all__ = [
    "ActivityProducer",
    "AppendRecord",
    "ClientProducer",
    "CloseStatus",
    "Producer",
    "ProducerIdentity",
    "ReadResponse",
    "Record",
    "StreamBackend",
    "StreamClose",
    "StreamCreated",
    "StreamRef",
    "WorkflowActivationCompleted",
    "WorkflowAppendMode",
    "WorkflowAppendModeFixed",
    "WorkflowEnding",
    "WorkflowProducer",
    "WorkflowTaskCommit",
    "client_backend",
]

_native_backends: weakref.WeakKeyDictionary[
    temporalio.client.Client, NativeStreamBackend
] = weakref.WeakKeyDictionary()


def client_backend(client: temporalio.client.Client) -> StreamBackend:
    """The backend a client and its workers use: the one a plugin set, or else native."""
    backend = client.config(active_config=True).get("stream_backend")
    if backend:
        return backend
    native = _native_backends.get(client)
    if not native:
        native = _native_backends[client] = NativeStreamBackend(client)
    return native
