"""Command-aware visitor that tracks command context during payload traversal."""

import contextvars
from dataclasses import dataclass
from typing import Optional

import temporalio.api.enums.v1.command_type_pb2
from temporalio.bridge._visitor import PayloadVisitor


@dataclass(frozen=True)
class CommandInfo:
    """Information identifying a specific command instance."""

    command_type: temporalio.api.enums.v1.command_type_pb2.CommandType
    command_seq: int


# Current workflow command info context variable
current_command_info: contextvars.ContextVar[Optional[CommandInfo]] = (
    contextvars.ContextVar("current_command_info", default=None)
)


class CommandAwarePayloadVisitor(PayloadVisitor):
    """Payload visitor that tracks command context during traversal."""

    # Maps field names to their command types
    _COMMAND_TYPES = {
        # Workflow commands
        "schedule_activity": temporalio.api.enums.v1.command_type_pb2.CommandType.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
        "schedule_local_activity": temporalio.api.enums.v1.command_type_pb2.CommandType.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
        "start_child_workflow_execution": temporalio.api.enums.v1.command_type_pb2.CommandType.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
        "signal_external_workflow_execution": temporalio.api.enums.v1.command_type_pb2.CommandType.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
        "schedule_nexus_operation": temporalio.api.enums.v1.command_type_pb2.CommandType.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
        # Activation job resolutions
        "resolve_activity": temporalio.api.enums.v1.command_type_pb2.CommandType.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
        "resolve_child_workflow_execution_start": temporalio.api.enums.v1.command_type_pb2.CommandType.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
        "resolve_child_workflow_execution": temporalio.api.enums.v1.command_type_pb2.CommandType.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
        "resolve_signal_external_workflow": temporalio.api.enums.v1.command_type_pb2.CommandType.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
        "resolve_request_cancel_external_workflow": temporalio.api.enums.v1.command_type_pb2.CommandType.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION,
        "resolve_nexus_operation_start": temporalio.api.enums.v1.command_type_pb2.CommandType.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
        "resolve_nexus_operation": temporalio.api.enums.v1.command_type_pb2.CommandType.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
    }

    def _set_command_context(self, field_name: str, seq: int) -> Optional[object]:
        """Set command context if field is tracked."""
        command_type = self._COMMAND_TYPES.get(field_name)
        if command_type is not None:
            return current_command_info.set(
                CommandInfo(command_type=command_type, command_seq=seq)
            )
        return None

    # Override workflow command visitor
    async def _visit_coresdk_workflow_commands_ScheduleActivity(self, fs, o):
        token = self._set_command_context("schedule_activity", o.seq)
        try:
            await super()._visit_coresdk_workflow_commands_ScheduleActivity(fs, o)
        finally:
            if token:
                current_command_info.reset(token)

    async def _visit_coresdk_workflow_commands_ScheduleLocalActivity(self, fs, o):
        token = self._set_command_context("schedule_local_activity", o.seq)
        try:
            await super()._visit_coresdk_workflow_commands_ScheduleLocalActivity(fs, o)
        finally:
            if token:
                current_command_info.reset(token)

    async def _visit_coresdk_workflow_commands_StartChildWorkflowExecution(self, fs, o):
        token = self._set_command_context("start_child_workflow_execution", o.seq)
        try:
            await super()._visit_coresdk_workflow_commands_StartChildWorkflowExecution(
                fs, o
            )
        finally:
            if token:
                current_command_info.reset(token)

    async def _visit_coresdk_workflow_commands_SignalExternalWorkflowExecution(
        self, fs, o
    ):
        token = self._set_command_context("signal_external_workflow_execution", o.seq)
        try:
            await super()._visit_coresdk_workflow_commands_SignalExternalWorkflowExecution(
                fs, o
            )
        finally:
            if token:
                current_command_info.reset(token)

    async def _visit_coresdk_workflow_commands_ScheduleNexusOperation(self, fs, o):
        token = self._set_command_context("schedule_nexus_operation", o.seq)
        try:
            await super()._visit_coresdk_workflow_commands_ScheduleNexusOperation(fs, o)
        finally:
            if token:
                current_command_info.reset(token)

    # Override activation job visitors
    async def _visit_coresdk_workflow_activation_ResolveActivity(self, fs, o):
        token = self._set_command_context("resolve_activity", o.seq)
        try:
            await super()._visit_coresdk_workflow_activation_ResolveActivity(fs, o)
        finally:
            if token:
                current_command_info.reset(token)

    async def _visit_coresdk_workflow_activation_ResolveChildWorkflowExecutionStart(
        self, fs, o
    ):
        token = self._set_command_context(
            "resolve_child_workflow_execution_start", o.seq
        )
        try:
            await super()._visit_coresdk_workflow_activation_ResolveChildWorkflowExecutionStart(
                fs, o
            )
        finally:
            if token:
                current_command_info.reset(token)

    async def _visit_coresdk_workflow_activation_ResolveChildWorkflowExecution(
        self, fs, o
    ):
        token = self._set_command_context("resolve_child_workflow_execution", o.seq)
        try:
            await super()._visit_coresdk_workflow_activation_ResolveChildWorkflowExecution(
                fs, o
            )
        finally:
            if token:
                current_command_info.reset(token)

    async def _visit_coresdk_workflow_activation_ResolveSignalExternalWorkflow(
        self, fs, o
    ):
        token = self._set_command_context("resolve_signal_external_workflow", o.seq)
        try:
            await super()._visit_coresdk_workflow_activation_ResolveSignalExternalWorkflow(
                fs, o
            )
        finally:
            if token:
                current_command_info.reset(token)

    async def _visit_coresdk_workflow_activation_ResolveRequestCancelExternalWorkflow(
        self, fs, o
    ):
        token = self._set_command_context(
            "resolve_request_cancel_external_workflow", o.seq
        )
        try:
            await super()._visit_coresdk_workflow_activation_ResolveRequestCancelExternalWorkflow(
                fs, o
            )
        finally:
            if token:
                current_command_info.reset(token)

    async def _visit_coresdk_workflow_activation_ResolveNexusOperationStart(
        self, fs, o
    ):
        token = self._set_command_context("resolve_nexus_operation_start", o.seq)
        try:
            await super()._visit_coresdk_workflow_activation_ResolveNexusOperationStart(
                fs, o
            )
        finally:
            if token:
                current_command_info.reset(token)

    async def _visit_coresdk_workflow_activation_ResolveNexusOperation(self, fs, o):
        token = self._set_command_context("resolve_nexus_operation", o.seq)
        try:
            await super()._visit_coresdk_workflow_activation_ResolveNexusOperation(
                fs, o
            )
        finally:
            if token:
                current_command_info.reset(token)
