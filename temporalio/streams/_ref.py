"""Stream references."""

from __future__ import annotations

from dataclasses import dataclass

import temporalio.client
import temporalio.workflow


@dataclass(frozen=True)
class StreamRef:
    """A stream owned by a workflow run chain.

    ``first_run_id`` is the run ID of the chain's first run. A read without it reads the workflow
    ID's latest chain. A Redis stream needs it for appends.
    """

    workflow_id: str
    first_run_id: str = ""

    @staticmethod
    def for_handle(handle: temporalio.client.WorkflowHandle) -> StreamRef:
        """The stream of the run chain a handle from starting a workflow belongs to."""
        return StreamRef(handle.id, handle.first_execution_run_id or "")

    @staticmethod
    def for_workflow() -> StreamRef:
        """The stream of the calling workflow's run chain. Must be called from workflow code."""
        info = temporalio.workflow.info()
        return StreamRef(info.workflow_id, info.first_execution_run_id)
