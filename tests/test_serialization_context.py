from __future__ import annotations

import dataclasses
import uuid
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Literal, Optional, Type

from temporalio import activity, workflow
from temporalio.api.common.v1 import Payload
from temporalio.client import Client
from temporalio.converter import (
    ActivitySerializationContext,
    CompositePayloadConverter,
    DataConverter,
    DefaultPayloadConverter,
    EncodingPayloadConverter,
    JSONPlainPayloadConverter,
    SerializationContext,
    WithSerializationContext,
    WorkflowSerializationContext,
)
from temporalio.worker import Worker


@dataclass
class TraceItem:
    context_type: Literal["workflow", "activity"]
    method: Literal["to_payload", "from_payload"]
    context: WorkflowSerializationContext | ActivitySerializationContext


@dataclass
class TraceData:
    items: list[TraceItem] = field(default_factory=list)


@activity.defn
async def passthrough_activity(input: TraceData) -> TraceData:
    return input


@workflow.defn(sandboxed=False)  # we want to use isinstance
class PassThroughWorkflow:
    @workflow.run
    async def run(self, input: TraceData) -> TraceData:
        return input


@workflow.defn(sandboxed=False)
class WorkflowWithActivity:
    @workflow.run
    async def run(self, input: TraceData) -> TraceData:
        return await workflow.execute_activity(
            passthrough_activity,
            input,
            start_to_close_timeout=timedelta(seconds=10),
        )


@workflow.defn(sandboxed=False)
class ChildWorkflow:
    @workflow.run
    async def run(self, input: TraceData) -> TraceData:
        return input


@workflow.defn(sandboxed=False)
class ComprehensiveWorkflow:
    def __init__(self) -> None:
        self.signal_data: Optional[TraceData] = None
        self.update_data: Optional[TraceData] = None
        
    @workflow.run
    async def run(self, input: TraceData) -> TraceData:
        # Test activity
        activity_result = await workflow.execute_activity(
            passthrough_activity,
            input,
            start_to_close_timeout=timedelta(seconds=10),
        )
        
        # Test child workflow
        child_result = await workflow.execute_child_workflow(
            ChildWorkflow.run,
            activity_result,
            id=f"child-{workflow.info().workflow_id}",
        )
        
        # Wait for signal
        await workflow.wait_condition(lambda: self.signal_data is not None)
        
        # Wait for update
        await workflow.wait_condition(lambda: self.update_data is not None)
        
        # Combine all results
        combined = TraceData(items=child_result.items[:])
        if self.signal_data:
            combined.items.extend(self.signal_data.items)
        if self.update_data:
            combined.items.extend(self.update_data.items)
        
        return combined
    
    @workflow.signal
    def my_signal(self, data: TraceData) -> None:
        self.signal_data = data
    
    @workflow.update
    async def my_update(self, data: TraceData) -> TraceData:
        self.update_data = data
        return data
    
    @workflow.query
    def my_query(self, data: TraceData) -> TraceData:
        return data


class SerializationContextTestEncodingPayloadConverter(
    EncodingPayloadConverter, WithSerializationContext
):
    def __init__(self):
        self.context: Optional[SerializationContext] = None

    @property
    def encoding(self) -> str:
        return "test-serialization-context"

    def with_context(
        self, context: Optional[SerializationContext]
    ) -> SerializationContextTestEncodingPayloadConverter:
        print(
            f"🌈 SerializationContextTestEncodingPayloadConverter.with_context({context})"
        )
        converter = SerializationContextTestEncodingPayloadConverter()
        converter.context = context
        return converter

    def to_payload(self, value: Any) -> Optional[Payload]:
        if not isinstance(value, TraceData):
            return None
        print(
            f"🌈 SerializationContextTestEncodingPayloadConverter.to_payload({value})"
        )
        if isinstance(self.context, WorkflowSerializationContext):
            value.items.append(
                TraceItem(
                    context_type="workflow", method="to_payload", context=self.context
                )
            )
        elif isinstance(self.context, ActivitySerializationContext):
            value.items.append(
                TraceItem(
                    context_type="activity", method="to_payload", context=self.context
                )
            )
        payload = JSONPlainPayloadConverter().to_payload(value)
        assert payload
        payload.metadata["encoding"] = self.encoding.encode()
        return payload

    def from_payload(self, payload: Payload, type_hint: Optional[Type] = None) -> Any:
        print(
            f"🌈 SerializationContextTestEncodingPayloadConverter.from_payload({payload}, {type_hint})"
        )
        value = JSONPlainPayloadConverter().from_payload(payload, type_hint)
        assert isinstance(value, TraceData)
        if isinstance(self.context, WorkflowSerializationContext):
            value.items.append(
                TraceItem(
                    context_type="workflow", method="from_payload", context=self.context
                )
            )
        elif isinstance(self.context, ActivitySerializationContext):
            value.items.append(
                TraceItem(
                    context_type="activity", method="from_payload", context=self.context
                )
            )
        return value


class SerializationContextTestPayloadConverter(
    CompositePayloadConverter, WithSerializationContext
):
    def __init__(self):
        super().__init__(
            SerializationContextTestEncodingPayloadConverter(),
            *DefaultPayloadConverter.default_encoding_payload_converters,
        )


data_converter = dataclasses.replace(
    DataConverter.default,
    payload_converter_class=SerializationContextTestPayloadConverter,
)


async def test_workflow_payload_conversion_can_be_given_access_to_serialization_context(
    client: Client,
):
    print()
    workflow_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    config = client.config()
    config["data_converter"] = data_converter
    client = Client(**config)

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[PassThroughWorkflow],
        activities=[],
    ):
        result = await client.execute_workflow(
            PassThroughWorkflow.run,
            TraceData(),
            id=workflow_id,
            task_queue=task_queue,
        )

        assert len(result.items) == 4
        assert result.items[0].method == "to_payload"
        assert result.items[1].method == "from_payload"
        assert result.items[2].method == "to_payload"
        assert result.items[3].method == "from_payload"


async def test_activity_payload_conversion_has_context(client: Client):
    workflow_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    config = client.config()
    config["data_converter"] = data_converter
    client = Client(**config)

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[WorkflowWithActivity],
        activities=[passthrough_activity],
    ):
        result = await client.execute_workflow(
            WorkflowWithActivity.run,
            TraceData(),
            id=workflow_id,
            task_queue=task_queue,
        )

        assert any(item.context_type == "activity" for item in result.items)


async def test_comprehensive_serialization_context(client: Client):
    workflow_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    config = client.config()
    config["data_converter"] = data_converter
    client = Client(**config)

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[ComprehensiveWorkflow, ChildWorkflow],
        activities=[passthrough_activity],
    ):
        # Start workflow
        handle = await client.start_workflow(
            ComprehensiveWorkflow.run,
            TraceData(),
            id=workflow_id,
            task_queue=task_queue,
        )
        
        # Send signal
        await handle.signal(ComprehensiveWorkflow.my_signal, TraceData())
        
        # Send update  
        await handle.execute_update(ComprehensiveWorkflow.my_update, TraceData())
        
        # Send query
        await handle.query(ComprehensiveWorkflow.my_query, TraceData())
        
        # Get result
        result = await handle.result()
        
        # Verify we have contexts for all operations
        context_types = {item.context_type for item in result.items}
        assert "workflow" in context_types
        assert "activity" in context_types
        
        # Verify both to_payload and from_payload were called for each
        methods = {item.method for item in result.items}
        assert "to_payload" in methods
        assert "from_payload" in methods
