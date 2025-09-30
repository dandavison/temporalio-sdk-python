"""Test that CommandAwarePayloadVisitor handles all commands with seq fields.

This module contains two complementary tests:

1. test_command_aware_visitor_handles_all_seq_commands:
   - Uses pattern matching to detect potential commands with seq
   - Good for catching unexpected visitor methods

2. test_command_aware_visitor_covers_all_protos_with_seq:
   - Directly inspects protobuf definitions to find seq fields
   - Definitive test that all protos with seq are handled
"""

from temporalio.bridge.proto.workflow_activation import workflow_activation_pb2
from temporalio.bridge.proto.workflow_commands import workflow_commands_pb2
from temporalio.worker._command_aware_visitor import CommandAwarePayloadVisitor


def test_command_aware_visitor_covers_all_protos_with_seq():
    """Verify CommandAwarePayloadVisitor handles all command/activation protos with seq fields.

    This test directly inspects the protobuf definitions to find which messages
    have 'seq' fields and ensures they're properly handled.
    """

    # Find all workflow command message types that have a seq field
    commands_with_seq = set()
    for field_name in dir(workflow_commands_pb2):
        if not field_name.startswith("_"):
            attr = getattr(workflow_commands_pb2, field_name)
            if hasattr(attr, "DESCRIPTOR") and hasattr(
                attr.DESCRIPTOR, "fields_by_name"
            ):
                if "seq" in attr.DESCRIPTOR.fields_by_name:
                    commands_with_seq.add(field_name)

    # Find all workflow activation job message types that have a seq field
    activation_jobs_with_seq = set()
    for field_name in dir(workflow_activation_pb2):
        if not field_name.startswith("_"):
            attr = getattr(workflow_activation_pb2, field_name)
            if hasattr(attr, "DESCRIPTOR") and hasattr(
                attr.DESCRIPTOR, "fields_by_name"
            ):
                if "seq" in attr.DESCRIPTOR.fields_by_name:
                    activation_jobs_with_seq.add(field_name)

    # Commands that need context tracking (ones we care about)
    commands_needing_context = {
        "ScheduleActivity",
        "ScheduleLocalActivity",
        "StartChildWorkflowExecution",
        "SignalExternalWorkflowExecution",
        "ScheduleNexusOperation",
    }

    # Activation jobs that need context tracking
    activation_jobs_needing_context = {
        "ResolveActivity",
        "ResolveChildWorkflowExecutionStart",
        "ResolveChildWorkflowExecution",
        "ResolveSignalExternalWorkflow",
        "ResolveRequestCancelExternalWorkflow",
        "ResolveNexusOperationStart",
        "ResolveNexusOperation",
    }

    # Commands with seq that don't need context (cancellations, timers)
    commands_seq_no_context = {
        "CancelTimer",
        "CancelSignalWorkflow",
        "RequestCancelActivity",
        "RequestCancelLocalActivity",
        "RequestCancelExternalWorkflowExecution",
        "RequestCancelNexusOperation",
        "StartTimer",
    }

    # Activation jobs with seq that don't need context
    activation_jobs_seq_no_context = {
        "FireTimer",
    }

    # Check that all commands with seq are accounted for
    all_known_commands = commands_needing_context | commands_seq_no_context
    unaccounted_commands = commands_with_seq - all_known_commands
    assert not unaccounted_commands, (
        f"Found command protos with seq field that aren't categorized: {unaccounted_commands}\n"
        f"Add them to either commands_needing_context or commands_seq_no_context"
    )

    # Check that all activation jobs with seq are accounted for
    all_known_activation_jobs = (
        activation_jobs_needing_context | activation_jobs_seq_no_context
    )
    unaccounted_activation_jobs = activation_jobs_with_seq - all_known_activation_jobs
    assert not unaccounted_activation_jobs, (
        f"Found activation job protos with seq field that aren't categorized: {unaccounted_activation_jobs}\n"
        f"Add them to either activation_jobs_needing_context or activation_jobs_seq_no_context"
    )

    # Verify CommandAwarePayloadVisitor has overrides for all that need context
    visitor_overrides = set(CommandAwarePayloadVisitor.__dict__.keys())

    # Check commands
    for command in commands_needing_context:
        method_name = f"_visit_coresdk_workflow_commands_{command}"
        assert (
            method_name in visitor_overrides
        ), f"CommandAwarePayloadVisitor missing override for command with seq: {command}"

    # Check activation jobs
    for job in activation_jobs_needing_context:
        method_name = f"_visit_coresdk_workflow_activation_{job}"
        assert (
            method_name in visitor_overrides
        ), f"CommandAwarePayloadVisitor missing override for activation job with seq: {job}"

    # Verify we're not overriding things that don't have seq
    for override_name in visitor_overrides:
        if override_name.startswith("_visit_coresdk_workflow_commands_"):
            command_name = override_name.replace(
                "_visit_coresdk_workflow_commands_", ""
            )
            if command_name not in commands_with_seq:
                # Special case: might be a valid override for other reasons
                assert False, (
                    f"CommandAwarePayloadVisitor has override for command without seq: {command_name}\n"
                    f"Remove the override or verify this is intentional"
                )
        elif override_name.startswith("_visit_coresdk_workflow_activation_"):
            job_name = override_name.replace("_visit_coresdk_workflow_activation_", "")
            if job_name not in activation_jobs_with_seq:
                # Special case: might be a valid override for other reasons
                assert False, (
                    f"CommandAwarePayloadVisitor has override for activation job without seq: {job_name}\n"
                    f"Remove the override or verify this is intentional"
                )
