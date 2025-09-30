"""Visitor that sets command context during payload traversal."""

import contextvars
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator, Optional

from temporalio.api.enums.v1.command_type_pb2 import CommandType
from temporalio.bridge._visitor import PayloadVisitor, VisitorFunctions
from temporalio.bridge.proto.workflow_activation import workflow_activation_pb2
from temporalio.bridge.proto.workflow_commands import workflow_commands_pb2


@dataclass(frozen=True)
class CommandInfo:
    """Information identifying a specific command instance."""

    command_type: CommandType.ValueType
    command_seq: int


# Current workflow command info context variable
current_command_info: contextvars.ContextVar[Optional[CommandInfo]] = (
    contextvars.ContextVar("current_command_info", default=None)
)


class CommandAwarePayloadVisitor(PayloadVisitor):
    """Payload visitor that sets command context during traversal.

    Override  methods are created for all workflow commands and activation jobs that have a 'seq'
    field.
    """

    _COMMAND_TYPE_MAP = {
        # Commands
        "ScheduleActivity": CommandType.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
        "ScheduleLocalActivity": CommandType.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
        "StartChildWorkflowExecution": CommandType.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
        "SignalExternalWorkflowExecution": CommandType.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
        "RequestCancelExternalWorkflowExecution": CommandType.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION,
        "ScheduleNexusOperation": CommandType.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
        "RequestCancelNexusOperation": CommandType.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION,
        "StartTimer": CommandType.COMMAND_TYPE_START_TIMER,
        "CancelTimer": CommandType.COMMAND_TYPE_CANCEL_TIMER,
        "RequestCancelActivity": CommandType.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
        "RequestCancelLocalActivity": CommandType.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
        "CancelSignalWorkflow": None,
        # Resolutions (map to their corresponding command types)
        "ResolveActivity": CommandType.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
        "ResolveChildWorkflowExecutionStart": CommandType.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
        "ResolveChildWorkflowExecution": CommandType.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
        "ResolveSignalExternalWorkflow": CommandType.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
        "ResolveRequestCancelExternalWorkflow": CommandType.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION,
        "ResolveNexusOperationStart": CommandType.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
        "ResolveNexusOperation": CommandType.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
        "FireTimer": CommandType.COMMAND_TYPE_START_TIMER,
    }

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._create_override_methods()

    def _create_override_methods(self) -> None:
        """Dynamically create override methods for all protos with seq fields."""
        # Process workflow commands
        for name in dir(workflow_commands_pb2):
            if not name.startswith("_"):
                attr = getattr(workflow_commands_pb2, name)
                if (
                    hasattr(attr, "DESCRIPTOR")
                    and hasattr(attr.DESCRIPTOR, "fields_by_name")
                    and "seq" in attr.DESCRIPTOR.fields_by_name
                ):
                    command_type = self._COMMAND_TYPE_MAP[name]
                    if command_type:
                        self._add_override(
                            "coresdk_workflow_commands", name, command_type
                        )

        # Process activation jobs
        for name in dir(workflow_activation_pb2):
            if not name.startswith("_"):
                attr = getattr(workflow_activation_pb2, name)
                if (
                    hasattr(attr, "DESCRIPTOR")
                    and hasattr(attr.DESCRIPTOR, "fields_by_name")
                    and "seq" in attr.DESCRIPTOR.fields_by_name
                ):
                    command_type = self._COMMAND_TYPE_MAP[name]
                    if command_type:
                        self._add_override(
                            "coresdk_workflow_activation", name, command_type
                        )

    def _add_override(
        self, module: str, name: str, command_type: CommandType.ValueType
    ) -> None:
        """Add an override method that sets command context."""
        method_name = f"_visit_{module}_{name}"

        # Create the override method
        async def override_method(self: Any, fs: VisitorFunctions, o: Any) -> None:
            with current_command(command_type, o.seq):
                # Call the parent class's method
                parent_method = getattr(PayloadVisitor, method_name, None)
                if parent_method:
                    await parent_method(self, fs, o)

        # Bind the method to this instance
        setattr(
            self, method_name, override_method.__get__(self, CommandAwarePayloadVisitor)
        )


@contextmanager
def current_command(
    command_type: CommandType.ValueType, command_seq: int
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
