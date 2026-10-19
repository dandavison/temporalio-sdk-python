"""Stream references."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StreamRef:
    """A stream, named by its owner: a workflow run chain, by ``workflow_id`` and ``first_run_id``,
    or a Standalone Activity execution, by ``activity_id`` and ``activity_run_id``.

    Exactly one of ``workflow_id`` and ``activity_id`` is set. The run ID is optional for reads: a
    read without it reads the owner ID's latest chain or execution. A Redis stream needs it for
    appends.
    """

    workflow_id: str = ""
    first_run_id: str = ""
    activity_id: str = ""
    activity_run_id: str = ""

    def __post_init__(self) -> None:
        """Check that the reference names exactly one owner."""
        if bool(self.workflow_id) == bool(self.activity_id):
            raise ValueError(
                "a stream is named by exactly one of a workflow ID and an activity ID"
            )
