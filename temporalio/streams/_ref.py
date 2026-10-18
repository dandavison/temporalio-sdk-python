"""Stream references."""

from __future__ import annotations

from dataclasses import dataclass

import temporalio.activity
import temporalio.client
import temporalio.workflow


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

    @staticmethod
    def for_handle(
        handle: temporalio.client.WorkflowHandle | temporalio.client.ActivityHandle,
    ) -> StreamRef:
        """The stream of the workflow run chain or Standalone Activity a handle from starting it
        belongs to.
        """
        if isinstance(handle, temporalio.client.ActivityHandle):
            return StreamRef(activity_id=handle.id, activity_run_id=handle.run_id or "")
        return StreamRef(handle.id, handle.first_execution_run_id or "")

    @staticmethod
    def for_workflow() -> StreamRef:
        """The stream of the calling workflow's run chain. Must be called from workflow code."""
        info = temporalio.workflow.info()
        return StreamRef(info.workflow_id, info.first_execution_run_id)

    @staticmethod
    def for_activity() -> StreamRef:
        """The stream of the calling Standalone Activity. Must be called from activity code."""
        info = temporalio.activity.info()
        if info.activity_run_id is None:
            raise RuntimeError("only a Standalone Activity owns a stream")
        return StreamRef(
            activity_id=info.activity_id, activity_run_id=info.activity_run_id
        )
