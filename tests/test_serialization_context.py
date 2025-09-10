from __future__ import annotations

import dataclasses
import json
import uuid
from dataclasses import dataclass
from typing import Any, Optional, Type

from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.client import Client
from temporalio.converter import (
    CompositePayloadConverter,
    DataConverter,
    DefaultPayloadConverter,
    EncodingPayloadConverter,
    SerializationContext,
    WithSerializationContext,
    WorkflowSerializationContext,
)
from temporalio.worker import Worker


@dataclass
class WorkflowData:
    # Context captured during to_payload (when sending data to workflow)
    to_payload_context: Optional[WorkflowSerializationContext] = None
    # Context captured during from_payload (when receiving data from workflow)
    from_payload_context: Optional[WorkflowSerializationContext] = None


@workflow.defn
class SerializationContextTestWorkflow:
    @workflow.run
    async def run(self, input: WorkflowData) -> WorkflowData:
        return input


class SerializationContextTestEncodingPayloadConverter(
    EncodingPayloadConverter, WithSerializationContext
):
    def __init__(self, context: Optional[SerializationContext] = None):
        self.context = context

    @property
    def encoding(self) -> str:
        return "test-serialization-context"

    def with_context(
        self, context: Optional[SerializationContext]
    ) -> SerializationContextTestEncodingPayloadConverter:
        return SerializationContextTestEncodingPayloadConverter(context)

    def to_payload(self, value: Any) -> Optional[Payload]:
        # Only process WorkflowData
        if not isinstance(value, WorkflowData):
            return None
            
        # Capture the context when serializing
        if self.context:
            value.to_payload_context = self.context
            
        # Serialize the data
        data = {
            "to_payload_context": {
                "namespace": value.to_payload_context.namespace,
                "workflow_id": value.to_payload_context.workflow_id,
            } if value.to_payload_context else None,
            "from_payload_context": {
                "namespace": value.from_payload_context.namespace,
                "workflow_id": value.from_payload_context.workflow_id,
            } if value.from_payload_context else None,
        }
        
        return Payload(
            metadata={"encoding": self.encoding.encode()},
            data=json.dumps(data).encode(),
        )

    def from_payload(self, payload: Payload, type_hint: Optional[Type] = None) -> Any:
        # Check encoding
        if payload.metadata.get("encoding", b"") != self.encoding.encode():
            return None
            
        # Deserialize the data
        data = json.loads(payload.data.decode())
        result = WorkflowData()
        
        # Restore the serialized contexts
        if data.get("to_payload_context"):
            result.to_payload_context = WorkflowSerializationContext(
                namespace=data["to_payload_context"]["namespace"],
                workflow_id=data["to_payload_context"]["workflow_id"],
            )
        if data.get("from_payload_context"):
            result.from_payload_context = WorkflowSerializationContext(
                namespace=data["from_payload_context"]["namespace"],
                workflow_id=data["from_payload_context"]["workflow_id"],
            )
            
        # Capture the current context during deserialization
        if self.context and isinstance(self.context, WorkflowSerializationContext):
            result.from_payload_context = self.context
            
        return result


class SerializationContextTestPayloadConverter(CompositePayloadConverter):
    def __init__(self, *converters):
        # If no converters provided, use our defaults
        if not converters:
            converters = (
                SerializationContextTestEncodingPayloadConverter(None),
                *DefaultPayloadConverter.default_encoding_payload_converters,
            )
        super().__init__(*converters)


data_converter = DataConverter(
    payload_converter_class=SerializationContextTestPayloadConverter,
)


async def test_workflow_payload_conversion_can_be_given_access_to_serialization_context(
    client: Client,
):
    workflow_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    # Create client with custom data converter
    client_with_converter = Client(
        service_client=client.service_client,
        namespace=client.namespace,
        data_converter=data_converter,
    )
    
    async with Worker(
        client_with_converter,
        task_queue=task_queue,
        workflows=[SerializationContextTestWorkflow],
        activities=[],
    ):
        result = await client_with_converter.execute_workflow(
            SerializationContextTestWorkflow.run,
            WorkflowData(),
            id=workflow_id,
            task_queue=task_queue,
        )

        # Check that context was captured during serialization to workflow (input)
        assert result.to_payload_context == WorkflowSerializationContext(
            namespace="default",
            workflow_id=workflow_id,
        )
        
        # Check that context was captured during deserialization from workflow (result)
        assert result.from_payload_context == WorkflowSerializationContext(
            namespace="default",
            workflow_id=workflow_id,
        )
