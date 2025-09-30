"""Visitor that sets command context during payload traversal."""

import contextvars
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator, Optional

import temporalio.api.enums.v1.command_type_pb2
from temporalio.bridge._visitor import PayloadVisitor


@dataclass(frozen=True)
class CommandInfo:
    """Information identifying a specific command instance."""

    command_type: temporalio.api.enums.v1.command_type_pb2.CommandType.ValueType
    command_seq: int


# Current workflow command info context variable
current_command_info: contextvars.ContextVar[Optional[CommandInfo]] = (
    contextvars.ContextVar("current_command_info", default=None)
)


class CommandAwarePayloadVisitor(PayloadVisitor):
    """Payload visitor that tracks command context during traversal."""

    # Override workflow command visitor
    async def _visit_coresdk_workflow_commands_ScheduleActivity(self, fs, o):
        with current_command(
            temporalio.api.enums.v1.command_type_pb2.CommandType.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
            o.seq,
        ):
            await super()._visit_coresdk_workflow_commands_ScheduleActivity(fs, o)

    async def _visit_coresdk_workflow_commands_ScheduleLocalActivity(self, fs, o):
        with current_command(
            temporalio.api.enums.v1.command_type_pb2.CommandType.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
            o.seq,
        ):
            await super()._visit_coresdk_workflow_commands_ScheduleLocalActivity(fs, o)

    async def _visit_coresdk_workflow_commands_StartChildWorkflowExecution(self, fs, o):
        with current_command(
            temporalio.api.enums.v1.command_type_pb2.CommandType.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
            o.seq,
        ):
            await super()._visit_coresdk_workflow_commands_StartChildWorkflowExecution(
                fs, o
            )

    async def _visit_coresdk_workflow_commands_SignalExternalWorkflowExecution(
        self, fs, o
    ):
        with current_command(
            temporalio.api.enums.v1.command_type_pb2.CommandType.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
            o.seq,
        ):
            await super()._visit_coresdk_workflow_commands_SignalExternalWorkflowExecution(
                fs, o
            )

    async def _visit_coresdk_workflow_commands_ScheduleNexusOperation(self, fs, o):
        with current_command(
            temporalio.api.enums.v1.command_type_pb2.CommandType.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
            o.seq,
        ):
            await super()._visit_coresdk_workflow_commands_ScheduleNexusOperation(fs, o)

    # Override activation job visitors
    async def _visit_coresdk_workflow_activation_ResolveActivity(self, fs, o):
        with current_command(
            temporalio.api.enums.v1.command_type_pb2.CommandType.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
            o.seq,
        ):
            await super()._visit_coresdk_workflow_activation_ResolveActivity(fs, o)

    async def _visit_coresdk_workflow_activation_ResolveChildWorkflowExecutionStart(
        self, fs, o
    ):
        with current_command(
            temporalio.api.enums.v1.command_type_pb2.CommandType.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
            o.seq,
        ):
            await super()._visit_coresdk_workflow_activation_ResolveChildWorkflowExecutionStart(
                fs, o
            )

    async def _visit_coresdk_workflow_activation_ResolveChildWorkflowExecution(
        self, fs, o
    ):
        with current_command(
            temporalio.api.enums.v1.command_type_pb2.CommandType.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
            o.seq,
        ):
            await super()._visit_coresdk_workflow_activation_ResolveChildWorkflowExecution(
                fs, o
            )

    async def _visit_coresdk_workflow_activation_ResolveSignalExternalWorkflow(
        self, fs, o
    ):
        with current_command(
            temporalio.api.enums.v1.command_type_pb2.CommandType.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
            o.seq,
        ):
            await super()._visit_coresdk_workflow_activation_ResolveSignalExternalWorkflow(
                fs, o
            )

    async def _visit_coresdk_workflow_activation_ResolveRequestCancelExternalWorkflow(
        self, fs, o
    ):
        with current_command(
            temporalio.api.enums.v1.command_type_pb2.CommandType.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION,
            o.seq,
        ):
            await super()._visit_coresdk_workflow_activation_ResolveRequestCancelExternalWorkflow(
                fs, o
            )

    async def _visit_coresdk_workflow_activation_ResolveNexusOperationStart(
        self, fs, o
    ):
        with current_command(
            temporalio.api.enums.v1.command_type_pb2.CommandType.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
            o.seq,
        ):
            await super()._visit_coresdk_workflow_activation_ResolveNexusOperationStart(
                fs, o
            )

    async def _visit_coresdk_workflow_activation_ResolveNexusOperation(self, fs, o):
        with current_command(
            temporalio.api.enums.v1.command_type_pb2.CommandType.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
            o.seq,
        ):
            await super()._visit_coresdk_workflow_activation_ResolveNexusOperation(
                fs, o
            )


@contextmanager
def current_command(
    command_type: temporalio.api.enums.v1.command_type_pb2.CommandType.ValueType,
    command_seq: int,
) -> Iterator[None]:
    """Context manager for setting command info."""
    token = current_command_info.set(
        CommandInfo(command_type=command_type, command_seq=command_seq)
    )
    try:
        yield
    finally:
        if token:
            current_command_info.reset(token)
