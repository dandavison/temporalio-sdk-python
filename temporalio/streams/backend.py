"""The interface a stream store implements, and the records it stores. Not application API:
applications configure backends with :py:class:`temporalio.streams.StreamBackends` and use the
handles in :py:mod:`temporalio.streams`.

.. warning::
    This API is a prototype and will change.

A client has the native backend, :py:class:`temporalio.streams.native.NativeStreamBackend`, and the
backends that ``stream_backends`` in its :py:class:`temporalio.client.ClientConfig` lists; its
workers use the client's.
"""

from __future__ import annotations

import weakref

import temporalio.client
from temporalio.streams._backend import (
    NATIVE,
    CloseStatus,
    ReadResponse,
    StreamBackend,
    StreamBackends,
    StreamClose,
    WorkflowActivationCompleted,
    WorkflowEnding,
    WorkflowStreamWrites,
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
from temporalio.streams._workflow import PendingStreamWrites
from temporalio.streams.native import NativeStreamBackend

__all__ = [
    "NATIVE",
    "ActivityProducer",
    "PendingStreamWrites",
    "AppendRecord",
    "ClientProducer",
    "CloseStatus",
    "Producer",
    "ProducerIdentity",
    "ReadResponse",
    "Record",
    "StreamBackend",
    "StreamBackends",
    "StreamClose",
    "StreamCreated",
    "StreamRef",
    "WorkflowActivationCompleted",
    "WorkflowAppendMode",
    "WorkflowAppendModeFixed",
    "WorkflowEnding",
    "WorkflowProducer",
    "WorkflowStreamWrites",
    "WorkflowTaskCommit",
    "client_backend",
    "client_backends",
    "default_backend",
    "workflow_reads",
]

_native_backends: weakref.WeakKeyDictionary[
    temporalio.client.Client, NativeStreamBackend
] = weakref.WeakKeyDictionary()


def client_backends(client: temporalio.client.Client) -> dict[str, StreamBackend]:
    """The backends of a client and its workers, by name: native, unless its config switches it
    off, and those its config lists.
    """
    config = _config(client)
    backends: dict[str, StreamBackend] = {}
    if config.native:
        native = _native_backends.get(client)
        if not native:
            native = _native_backends[client] = NativeStreamBackend(client)
        backends[NATIVE] = native
    for b in config.backends:
        b.bind(client, workflow_reads=config.workflow_reads)
        backends[b.name] = b
    return backends


def workflow_reads(client: temporalio.client.Client) -> bool:
    """Whether the client's workflows read streams: its config does not switch them off."""
    return _config(client).workflow_reads


def client_backend(
    client: temporalio.client.Client, name: str | None = None
) -> StreamBackend:
    """The client's backend named ``name``, or its default backend."""
    name = name or default_backend(client)
    backend = client_backends(client).get(name)
    if not backend:
        raise ValueError(f"The client has no stream backend named {name!r}")
    return backend


def default_backend(client: temporalio.client.Client) -> str:
    """The name of the client's default backend."""
    return _config(client).default


def _config(client: temporalio.client.Client) -> StreamBackends:
    return client.config(active_config=True).get("stream_backends") or StreamBackends()
