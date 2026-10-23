"""The interface a stream store implements, and the records it stores. Not application API:
applications configure backends with :py:class:`temporalio.streams.StreamBackends` and use the
handles in :py:mod:`temporalio.streams`.

.. warning::
    This API is a prototype and will change.

A client has the native backend, :py:class:`temporalio.streams.native.NativeStreamBackend`, unless
``TEMPORAL_STREAMS_ENABLE_NATIVE=false``, and the backends that ``stream_backends`` in its
:py:class:`temporalio.client.ClientConfig` lists; its workers use the client's.
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
from temporalio.streams._flags import NATIVE_DISABLED, native_enabled
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
    "configured_default_backend",
    "default_backend",
]

_native_backends: weakref.WeakKeyDictionary[
    temporalio.client.Client, NativeStreamBackend
] = weakref.WeakKeyDictionary()


def client_backends(client: temporalio.client.Client) -> dict[str, StreamBackend]:
    """The backends of a client and its workers, by name: native, unless switched off, and those
    its config lists.
    """
    backends: dict[str, StreamBackend] = {}
    if native_enabled():
        native = _native_backends.get(client)
        if not native:
            native = _native_backends[client] = NativeStreamBackend(client)
        backends[NATIVE] = native
    for b in _config(client).backends:
        b.bind(client)
        backends[b.name] = b
    return backends


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
    """The name of the client's default backend. Raises if it is native and native streams are
    switched off.
    """
    name = configured_default_backend(client)
    if name == NATIVE and not native_enabled():
        raise ValueError(NATIVE_DISABLED)
    return name


def configured_default_backend(client: temporalio.client.Client) -> str:
    """The name of the default backend that the client's config gives: the one it names, or the
    first it lists, or native. Unlike :py:func:`default_backend`, it does not check that the client
    has it.
    """
    config = _config(client)
    return config.default or next((b.name for b in config.backends), NATIVE)


def _config(client: temporalio.client.Client) -> StreamBackends:
    return client.config(active_config=True).get("stream_backends") or StreamBackends()
