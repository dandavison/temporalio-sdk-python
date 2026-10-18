"""Appending from workflow code."""

from __future__ import annotations

from typing import Any

import temporalio.workflow


def workflow_append(value: Any, *, topic: str = "") -> None:
    """Append a value to the calling workflow's own stream.

    The worker appends it at the end of the current activation. Must be called from workflow code.
    """
    temporalio.workflow._Runtime.current().workflow_stream_append(topic, value)
