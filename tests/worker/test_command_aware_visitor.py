"""Test that CommandAwarePayloadVisitor handles all commands with seq fields.

This module contains two complementary tests:

1. test_command_aware_visitor_handles_all_seq_commands:
   - Uses pattern matching to detect potential commands with seq
   - Good for catching unexpected visitor methods

2. test_command_aware_visitor_covers_all_protos_with_seq:
   - Directly inspects protobuf definitions to find seq fields
   - Definitive test that all protos with seq are handled
"""

import inspect

from temporalio.bridge._visitor import PayloadVisitor
from temporalio.bridge.proto.workflow_activation import workflow_activation_pb2
from temporalio.bridge.proto.workflow_commands import workflow_commands_pb2
from temporalio.worker._command_aware_visitor import CommandAwarePayloadVisitor


def test_command_aware_visitor_handles_all_seq_commands():
    """Ensure CommandAwarePayloadVisitor overrides all visitor methods for messages with seq fields.

    This test will fail if:
    1. We remove an expected override from CommandAwarePayloadVisitor
    2. New commands/resolutions are added that might need handling
    """
    # Methods we expect to be overridden for context tracking
    expected_overrides = {
        # Workflow commands that need context
        "_visit_coresdk_workflow_commands_ScheduleActivity",
        "_visit_coresdk_workflow_commands_ScheduleLocalActivity",
        "_visit_coresdk_workflow_commands_StartChildWorkflowExecution",
        "_visit_coresdk_workflow_commands_SignalExternalWorkflowExecution",
        "_visit_coresdk_workflow_commands_ScheduleNexusOperation",
        # Activation resolutions that need context
        "_visit_coresdk_workflow_activation_ResolveActivity",
        "_visit_coresdk_workflow_activation_ResolveChildWorkflowExecutionStart",
        "_visit_coresdk_workflow_activation_ResolveChildWorkflowExecution",
        "_visit_coresdk_workflow_activation_ResolveSignalExternalWorkflow",
        "_visit_coresdk_workflow_activation_ResolveRequestCancelExternalWorkflow",
        "_visit_coresdk_workflow_activation_ResolveNexusOperationStart",
        "_visit_coresdk_workflow_activation_ResolveNexusOperation",
    }

    # Commands/resolutions we know have seq but don't need context tracking
    known_seq_no_context = {
        # Cancel/timer commands - have seq but don't need context
        "_visit_coresdk_workflow_commands_CancelTimer",
        "_visit_coresdk_workflow_commands_CancelSignalWorkflow",
        "_visit_coresdk_workflow_commands_RequestCancelActivity",
        "_visit_coresdk_workflow_commands_RequestCancelLocalActivity",
        "_visit_coresdk_workflow_commands_RequestCancelExternalWorkflowExecution",
        "_visit_coresdk_workflow_commands_RequestCancelNexusOperation",
        "_visit_coresdk_workflow_commands_StartTimer",
        # Timer resolution - has seq but doesn't need context
        "_visit_coresdk_workflow_activation_FireTimer",
    }

    # Methods that don't have seq fields (false positives from pattern matching)
    known_no_seq = {
        # SignalWorkflow is an incoming signal, no seq
        "_visit_coresdk_workflow_activation_SignalWorkflow",
        # Child workflow start cancelled doesn't have seq
        "_visit_coresdk_workflow_activation_ResolveChildWorkflowExecutionStartCancelled",
    }

    # Get actual overridden methods in CommandAwarePayloadVisitor
    actual_overrides = {
        name
        for name in CommandAwarePayloadVisitor.__dict__
        if name.startswith("_visit_coresdk_workflow_")
    }

    # Check we have all expected overrides
    missing = expected_overrides - actual_overrides
    assert not missing, (
        f"CommandAwarePayloadVisitor is missing expected overrides: {missing}"
    )

    # Check for unexpected overrides (might be OK, but worth reviewing)
    unexpected = actual_overrides - expected_overrides
    assert not unexpected, (
        f"CommandAwarePayloadVisitor has unexpected overrides: {unexpected}\n"
        f"If these are correct, add them to expected_overrides in this test."
    )

    # Find all visitor methods in base class that look like they handle seq
    base_methods = {
        name
        for name, method in inspect.getmembers(PayloadVisitor)
        if name.startswith("_visit_coresdk_workflow_") and callable(method)
    }

    # Methods that likely have seq based on naming patterns
    potential_seq_methods = {
        name
        for name in base_methods
        if any(
            pattern in name
            for pattern in [
                "Schedule",
                "Start",
                "Signal",
                "Cancel",
                "Request",
                "Resolve",
                "Fire",
                "Nexus",
            ]
        )
    }

    # Check for new methods that might need handling
    all_known = expected_overrides | known_seq_no_context | known_no_seq
    potentially_unhandled = potential_seq_methods - all_known

    assert not potentially_unhandled, (
        f"New visitor methods detected that might have seq fields: {potentially_unhandled}\n"
        f"If they need context tracking, add overrides to CommandAwarePayloadVisitor.\n"
        f"If they have seq but don't need context, add to known_seq_no_context in this test.\n"
        f"If they don't have seq at all, update the pattern matching in this test."
    )


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
        assert method_name in visitor_overrides, (
            f"CommandAwarePayloadVisitor missing override for command with seq: {command}"
        )

    # Check activation jobs
    for job in activation_jobs_needing_context:
        method_name = f"_visit_coresdk_workflow_activation_{job}"
        assert method_name in visitor_overrides, (
            f"CommandAwarePayloadVisitor missing override for activation job with seq: {job}"
        )

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
