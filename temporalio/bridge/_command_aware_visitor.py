# Command-aware visitor that tracks command context during traversal

import contextvars
from dataclasses import dataclass
from typing import Optional

import temporalio.api.enums.v1.command_type_pb2
from temporalio.bridge._visitor import PayloadVisitor, VisitorFunctions


@dataclass(frozen=True)
class CommandInfo:
    """Information identifying a specific command instance."""

    command_type: temporalio.api.enums.v1.command_type_pb2.CommandType
    command_seq: int


current_command_info: contextvars.ContextVar[Optional[CommandInfo]] = (
    contextvars.ContextVar("current_command_info", default=None)
)


class CommandAwarePayloadVisitor(PayloadVisitor):
    """A payload visitor that tracks command context during traversal.

    This extends the base PayloadVisitor to set command context when
    visiting workflow commands and activation jobs that have sequence numbers.
    """

    # Map field names to command types
    _FIELD_TO_COMMAND_TYPE = {
        # Commands
        "schedule_activity": "COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK",
        "schedule_local_activity": "COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK",
        "start_child_workflow_execution": "COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION",
        "signal_external_workflow_execution": "COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION",
        "schedule_nexus_operation": "COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION",
        # Resolutions
        "resolve_activity": "COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK",
        "resolve_child_workflow_execution_start": "COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION",
        "resolve_child_workflow_execution": "COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION",
        "resolve_signal_external_workflow": "COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION",
        "resolve_request_cancel_external_workflow": "COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION",
        "resolve_nexus_operation_start": "COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION",
        "resolve_nexus_operation": "COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION",
    }

    async def _visit_with_command_context(
        self,
        fs: VisitorFunctions,
        obj: object,
        field_name: str,
        visit_method: str,
    ) -> None:
        """Visit an object while setting command context if it has a seq field."""
        field_obj = getattr(obj, field_name)

        # Check if this field type has a seq attribute
        if hasattr(field_obj, "seq") and field_name in self._FIELD_TO_COMMAND_TYPE:
            command_type_name = self._FIELD_TO_COMMAND_TYPE[field_name]
            command_type = getattr(
                temporalio.api.enums.v1.command_type_pb2.CommandType,
                command_type_name,
            )

            token = current_command_info.set(
                CommandInfo(
                    command_type=command_type,
                    command_seq=field_obj.seq,
                )
            )
            try:
                await getattr(super(), visit_method)(fs, field_obj)
            finally:
                current_command_info.reset(token)
        else:
            # No command context needed
            await getattr(super(), visit_method)(fs, field_obj)

    # Override methods for workflow commands
    async def _visit_coresdk_workflow_commands_WorkflowCommand(self, fs, o):
        """Visit workflow command, setting context for commands with seq."""
        # Handle user metadata first (no context needed)
        if o.HasField("user_metadata"):
            await self._visit_temporal_api_sdk_v1_UserMetadata(fs, o.user_metadata)

        # Check each possible command type
        for field_name in [
            "schedule_activity",
            "respond_to_query",
            "complete_workflow_execution",
            "fail_workflow_execution",
            "continue_as_new_workflow_execution",
            "start_child_workflow_execution",
            "signal_external_workflow_execution",
            "schedule_local_activity",
            "upsert_workflow_search_attributes",
            "modify_workflow_properties",
            "update_response",
            "schedule_nexus_operation",
        ]:
            if o.HasField(field_name):
                # Use the appropriate visit method
                visit_method = f"_visit_coresdk_workflow_commands_{field_name.title().replace('_', '')}"
                if field_name == "respond_to_query":
                    visit_method = "_visit_coresdk_workflow_commands_QueryResult"
                await self._visit_with_command_context(fs, o, field_name, visit_method)
                break

    # Override methods for workflow activation jobs
    async def _visit_coresdk_workflow_activation_WorkflowActivationJob(self, fs, o):
        """Visit workflow activation job, setting context for resolutions with seq."""
        for field_name in [
            "initialize_workflow",
            "query_workflow",
            "signal_workflow",
            "resolve_activity",
            "resolve_child_workflow_execution_start",
            "resolve_child_workflow_execution",
            "resolve_signal_external_workflow",
            "resolve_request_cancel_external_workflow",
            "do_update",
            "resolve_nexus_operation_start",
            "resolve_nexus_operation",
        ]:
            if o.HasField(field_name):
                # Use the appropriate visit method
                visit_method = f"_visit_coresdk_workflow_activation_{field_name.title().replace('_', '')}"
                await self._visit_with_command_context(fs, o, field_name, visit_method)
                break
