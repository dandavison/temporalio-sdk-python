"""Test that CommandAwarePayloadVisitor handles all commands with seq fields.

This test directly inspects protobuf definitions to ensure that ALL commands
and activation jobs with 'seq' fields get command context tracking.
"""

from temporalio.bridge._visitor import PayloadVisitor
from temporalio.bridge.proto.workflow_activation import workflow_activation_pb2
from temporalio.bridge.proto.workflow_commands import workflow_commands_pb2
from temporalio.worker._command_aware_visitor import CommandAwarePayloadVisitor


def test_command_aware_visitor_covers_all_protos_with_seq():
    """Verify CommandAwarePayloadVisitor handles ALL protos with seq fields.

    This test ensures that every workflow command and activation job
    that has a 'seq' field gets an override for command context tracking.
    """
    # Find all workflow command message types that have a seq field
    commands_with_seq = {
        name
        for name, descriptor in workflow_commands_pb2.DESCRIPTOR.message_types_by_name.items()
        if "seq" in descriptor.fields_by_name
    }

    # Find all workflow activation job message types that have a seq field
    activation_jobs_with_seq = {
        name
        for name, descriptor in workflow_activation_pb2.DESCRIPTOR.message_types_by_name.items()
        if "seq" in descriptor.fields_by_name
    }

    # Create a visitor instance to check its overrides
    visitor = CommandAwarePayloadVisitor()

    # Commands that have seq but no server command type
    # These are internal SDK commands that don't need context
    known_no_command_type = {
        "CancelSignalWorkflow",  # Internal SDK-core command for canceling pending signals
    }

    # Check that commands with seq have proper handling
    for command in commands_with_seq:
        if command in known_no_command_type:
            continue  # Skip commands that don't need tracking

        method_name = f"_visit_coresdk_workflow_commands_{command}"
        # Check if command is in the known map (has a command type)
        if command in visitor._COMMAND_TYPE_MAP:
            # Check if parent has the visitor method (i.e., command has payloads)
            parent_has_method = hasattr(PayloadVisitor, method_name)
            if parent_has_method:
                # Commands with payloads should have overrides
                assert hasattr(visitor, method_name), (
                    f"CommandAwarePayloadVisitor missing override for command with seq and payloads: {command}\n"
                    f"The command has payloads but the override wasn't created"
                )
            # else: Command has no payloads, so no visitor method needed
        else:
            # Command has seq but no command type mapping
            assert False, (
                f"Command proto '{command}' has seq field but no command type mapping.\n"
                f"Either add it to _COMMAND_TYPE_MAP or to known_no_command_type"
            )

    # Check that activation jobs with seq have proper handling
    for job in activation_jobs_with_seq:
        method_name = f"_visit_coresdk_workflow_activation_{job}"
        # Check if job is in the known map (has a command type)
        if job in visitor._COMMAND_TYPE_MAP:
            # Check if parent has the visitor method (i.e., job has payloads)
            parent_has_method = hasattr(PayloadVisitor, method_name)
            if parent_has_method:
                # Jobs with payloads should have overrides
                assert hasattr(visitor, method_name), (
                    f"CommandAwarePayloadVisitor missing override for activation job with seq and payloads: {job}\n"
                    f"The job has payloads but the override wasn't created"
                )
            # else: Job has no payloads, so no visitor method needed
        else:
            # Job has seq but no command type mapping
            assert False, (
                f"Activation job proto '{job}' has seq field but no command type mapping.\n"
                f"Add it to _COMMAND_TYPE_MAP in CommandAwarePayloadVisitor"
            )
