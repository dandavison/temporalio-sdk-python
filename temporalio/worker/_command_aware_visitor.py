"""Visitor that sets command context during payload traversal."""

import contextvars
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator, Optional, Type

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
        for proto_class in _get_workflow_command_protos_with_seq():
            command_type = self._COMMAND_TYPE_MAP.get(proto_class.__name__)
            if command_type:
                self._add_override(
                    proto_class, "coresdk_workflow_commands", command_type
                )

        # Process activation jobs
        for proto_class in _get_workflow_activation_protos_with_seq():
            command_type = self._COMMAND_TYPE_MAP.get(proto_class.__name__)
            if command_type:
                self._add_override(
                    proto_class, "coresdk_workflow_activation", command_type
                )

    def _add_override(
        self, proto_class: Type[Any], module: str, command_type: CommandType.ValueType
    ) -> None:
        """Add an override method that sets command context.

        Only creates overrides for commands that have payload fields.
        Commands without payloads (e.g., CancelTimer) don't need context
        because they don't serialize anything.
        """
        method_name = f"_visit_{module}_{proto_class.__name__}"
        parent_method = getattr(PayloadVisitor, method_name, None)

        if not parent_method:
            # No visitor method means no payload fields to visit
            return

        # Create override as a simple closure that captures self
        async def override_method(fs: VisitorFunctions, o: Any) -> None:
            with current_command(command_type, o.seq):
                await parent_method(self, fs, o)

        setattr(self, method_name, override_method)


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


def _get_workflow_command_protos_with_seq() -> Iterator[Type[Any]]:
    """Get concrete classes of all workflow command protos with a seq field."""
    for descriptor in workflow_commands_pb2.DESCRIPTOR.message_types_by_name.values():
        if "seq" in descriptor.fields_by_name:
            yield descriptor._concrete_class


def _get_workflow_activation_protos_with_seq() -> Iterator[Type[Any]]:
    """Get concrete classes of all workflow activation protos with a seq field."""
    for descriptor in workflow_activation_pb2.DESCRIPTOR.message_types_by_name.values():
        if "seq" in descriptor.fields_by_name:
            yield descriptor._concrete_class
