from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import List, Optional, Sequence

import temporalio.api.common.v1
from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.converter import (
    ActivitySerializationContext,
    DataConverter,
    DefaultPayloadConverter,
    PayloadCodec,
    SerializationContext,
    WorkflowSerializationContext,
)
from temporalio.worker import Worker


class WorkflowIdSignatureCodec(PayloadCodec):
    """Codec that signs payloads with workflow ID to prevent replay attacks."""

    def __init__(self, context: Optional[SerializationContext] = None):
        self.context = context
        self.encoding_key = b"signed"

    def with_context(self, context: SerializationContext) -> PayloadCodec:
        """Return a new codec with the given context."""
        return WorkflowIdSignatureCodec(context)

    async def encode(
        self, payloads: Sequence[temporalio.api.common.v1.Payload]
    ) -> List[temporalio.api.common.v1.Payload]:
        """Encode payloads with workflow ID signature."""
        result = []
        for payload in payloads:
            # Get workflow ID from context if available
            workflow_id = None
            activity_type = None

            if isinstance(self.context, WorkflowSerializationContext):
                workflow_id = self.context.workflow_id
            elif isinstance(self.context, ActivitySerializationContext):
                workflow_id = self.context.workflow_id
                activity_type = self.context.activity_type

            if workflow_id:
                # Create signature from workflow ID (and activity type if present)
                signature_data = workflow_id
                if activity_type:
                    signature_data += f":{activity_type}"
                signature = hashlib.sha256(signature_data.encode()).digest()[
                    :8
                ]  # Use first 8 bytes

                # Append signature to payload data
                new_data = payload.data + signature

                # Create new payload with signature
                new_payload = temporalio.api.common.v1.Payload()
                new_payload.CopyFrom(payload)
                new_payload.data = new_data
                new_payload.metadata["encoding"] = self.encoding_key
                result.append(new_payload)
            else:
                # No context, return payload as-is
                result.append(payload)

        return result

    async def decode(
        self, payloads: Sequence[temporalio.api.common.v1.Payload]
    ) -> List[temporalio.api.common.v1.Payload]:
        """Decode payloads and verify workflow ID signature."""
        result = []
        for payload in payloads:
            # Check if this payload was encoded by us
            if payload.metadata.get("encoding") == self.encoding_key:
                # Get workflow ID from context
                workflow_id = None
                activity_type = None

                if isinstance(self.context, WorkflowSerializationContext):
                    workflow_id = self.context.workflow_id
                elif isinstance(self.context, ActivitySerializationContext):
                    workflow_id = self.context.workflow_id
                    activity_type = self.context.activity_type

                if not workflow_id:
                    raise ValueError(
                        "Cannot decode signed payload without workflow ID context"
                    )

                # Calculate expected signature
                signature_data = workflow_id
                if activity_type:
                    signature_data += f":{activity_type}"
                expected_signature = hashlib.sha256(signature_data.encode()).digest()[
                    :8
                ]

                # Extract actual signature from payload
                if len(payload.data) < 8:
                    raise ValueError("Payload too short to contain signature")
                actual_signature = payload.data[-8:]
                original_data = payload.data[:-8]

                # Verify signature
                if actual_signature != expected_signature:
                    raise ValueError(
                        f"Signature mismatch! This payload was signed for a different workflow. "
                        f"Expected workflow: {workflow_id}"
                    )

                # Create new payload without signature
                new_payload = temporalio.api.common.v1.Payload()
                new_payload.CopyFrom(payload)
                new_payload.data = original_data
                del new_payload.metadata["encoding"]
                result.append(new_payload)
            else:
                # Not encoded by us, return as-is
                result.append(payload)

        return result


@dataclass
class TestInput:
    """Input data for test workflow."""

    message: str
    value: int


@dataclass
class TestOutput:
    """Output data from test workflow."""

    result: str
    processed_value: int


@activity.defn
async def test_activity(input: TestInput) -> TestOutput:
    """Simple test activity that processes input."""
    return TestOutput(
        result=f"Processed: {input.message}", processed_value=input.value * 2
    )


@workflow.defn
class WorkflowWithSignedPayloads:
    """Test workflow that uses activities and returns data."""

    @workflow.run
    async def run(self, input: TestInput) -> TestOutput:
        """Run the workflow."""
        # Execute an activity
        activity_result = await workflow.execute_activity(
            test_activity,
            input,
            start_to_close_timeout=timedelta(seconds=10),
        )

        # Return combined result
        return TestOutput(
            result=f"Workflow: {activity_result.result}",
            processed_value=activity_result.processed_value + 1,
        )


async def test_workflow_id_signed_payloads(client: Client):
    """Test that payloads are signed with workflow ID to prevent replay attacks."""

    # Create data converter with our signature codec
    data_converter = DataConverter(
        payload_converter_class=DefaultPayloadConverter,
        payload_codec=WorkflowIdSignatureCodec(),
    )

    config = client.config()
    config["data_converter"] = data_converter
    client = Client(**config)

    async with Worker(
        client,
        task_queue="test-signed-payloads",
        workflows=[WorkflowWithSignedPayloads],
        activities=[test_activity],
    ):
        result = await client.execute_workflow(
            WorkflowWithSignedPayloads.run,
            TestInput(message="Hello", value=42),
            id=f"test-signed-workflow-{uuid.uuid4()}",
            task_queue="test-signed-payloads",
        )

        assert result.result == "Workflow: Processed: Hello"
        assert result.processed_value == 85  # (42 * 2) + 1

        # TODO: This test should fail because the SDK doesn't actually pass
        # SerializationContext to the codec yet. Once implemented, the codec
        # will receive the workflow ID in the context and be able to sign
        # payloads properly.

        # The test will pass initially because without context, the codec
        # doesn't sign anything. Once we start passing context, we can
        # add a flag to verify that signing actually happened.
