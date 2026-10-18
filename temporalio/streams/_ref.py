"""Stream references."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StreamRef:
    """A stream owned by a workflow run chain.

    ``first_run_id`` is optional; when empty, the stream of the workflow ID's current chain is
    used.
    """

    workflow_id: str
    first_run_id: str = ""
