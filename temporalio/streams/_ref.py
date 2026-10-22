"""Stream references."""

from __future__ import annotations

from dataclasses import dataclass

MAX_NAME_BYTES = 255
"""The longest name of an owner's stream, in UTF-8 bytes."""


@dataclass(frozen=True)
class StreamRef:
    """A stream: one of the named streams of an owner, a workflow run chain (``workflow_id`` and
    ``first_run_id``) or a Standalone Activity execution (``activity_id`` and ``activity_run_id``),
    or a standalone stream (``stream_id``).

    Exactly one of ``workflow_id``, ``activity_id`` and ``stream_id`` is set. ``name``, of 1 to 255
    bytes, is required for an owner's stream and empty for a standalone stream. The run ID is
    optional for reads: a read without it reads the owner ID's latest chain or execution. A Redis
    stream of an owner needs it for writes.
    """

    workflow_id: str = ""
    first_run_id: str = ""
    activity_id: str = ""
    activity_run_id: str = ""
    name: str = ""
    stream_id: str = ""

    def __post_init__(self) -> None:
        """Check that the reference names exactly one stream."""
        if sum(map(bool, (self.workflow_id, self.activity_id, self.stream_id))) != 1:
            raise ValueError(
                "a stream is named by exactly one of a workflow ID, an activity ID and a stream ID"
            )
        if self.stream_id and self.name:
            raise ValueError("a standalone stream has no name")
        if not self.stream_id and not 0 < len(self.name.encode()) <= MAX_NAME_BYTES:
            raise ValueError(
                f"a stream of a workflow or activity needs a name of 1 to {MAX_NAME_BYTES} bytes"
            )
