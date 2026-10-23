"""Prototype switches for native streams and workflow reads of streams, read from the environment
at each use. They match the server's dynamic config, history.enableNativeStreams and
history.enableStreamWorkflowReads. Both false is the Brook configuration.

``TEMPORAL_STREAMS_ENABLE_NATIVE=false``: the client and its workers have no native backend, so
streams use the backends listed in the client's ``stream_backends``, and a worker refuses workflows
that declare commit mode.

``TEMPORAL_STREAMS_ENABLE_WORKFLOW_READS=false``: a workflow's read raises, workers register no read
local activity, and backends whose readers wait on notifiers in the server, such as Redis, send no
Notify.

Unset means true.
"""

from __future__ import annotations

import os

NATIVE_DISABLED = (
    "native streams are disabled (TEMPORAL_STREAMS_ENABLE_NATIVE=false); configure a stream "
    "backend, such as RedisStreamBackend, in the client's stream_backends"
)


def native_enabled() -> bool:
    """Whether clients and their workers have the native backend."""
    return _enabled("TEMPORAL_STREAMS_ENABLE_NATIVE")


def workflow_reads_enabled() -> bool:
    """Whether workflows read streams."""
    return _enabled("TEMPORAL_STREAMS_ENABLE_WORKFLOW_READS")


def _enabled(variable: str) -> bool:
    value = os.environ.get(variable, "true")
    if value not in ("true", "false"):
        raise ValueError(f"{variable} must be true or false, not {value!r}")
    return value == "true"
