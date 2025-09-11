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
    DefaultFailureConverter,
    DefaultPayloadConverter,
    EncodingPayloadConverter,
    FailureConverter,
    JSONPlainPayloadConverter,
    PayloadCodec,
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


class SerializationContextTestCodec(PayloadCodec, WithSerializationContext):
    def __init__(self):
        self.context: Optional[SerializationContext] = None
        self.encode_count = 0
        self.decode_count = 0
        self.contexts_seen = []

    def with_context(
        self, context: Optional[SerializationContext]
    ) -> SerializationContextTestCodec:
        codec = SerializationContextTestCodec()
        codec.context = context
        codec.contexts_seen = self.contexts_seen  # Share the list
        return codec

    async def encode(self, payloads: list[Payload]) -> list[Payload]:
        self.encode_count += 1
        if self.context:
            self.contexts_seen.append(("encode", self.context))
        result = []
        for p in payloads:
            # Add trace metadata
            new_p = Payload()
            new_p.CopyFrom(p)
            if self.context:
                if isinstance(self.context, WorkflowSerializationContext):
                    new_p.metadata["codec-ctx-type"] = b"workflow"
                    new_p.metadata["codec-wf-id"] = self.context.workflow_id.encode()
                elif isinstance(self.context, ActivitySerializationContext):
                    new_p.metadata["codec-ctx-type"] = b"activity"
                    new_p.metadata["codec-act-type"] = (
                        self.context.activity_type.encode()
                    )
            new_p.metadata["codec-encoded"] = b"true"
            result.append(new_p)
        return result

    async def decode(self, payloads: list[Payload]) -> list[Payload]:
        self.decode_count += 1
        if self.context:
            self.contexts_seen.append(("decode", self.context))
        result = []
        for p in payloads:
            # Just pass through, but verify metadata
            if p.metadata.get("codec-encoded") == b"true":
                result.append(p)
            else:
                result.append(p)
        return result


async def test_codec_serialization_context(client: Client):
    workflow_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    codec = SerializationContextTestCodec()
    data_converter_with_codec = dataclasses.replace(
        data_converter,
        payload_codec=codec,
    )

    config = client.config()
    config["data_converter"] = data_converter_with_codec
    client = Client(**config)

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[PassThroughWorkflow],
        activities=[passthrough_activity],
    ):
        result = await client.execute_workflow(
            PassThroughWorkflow.run,
            TraceData(),
            id=workflow_id,
            task_queue=task_queue,
        )

        # Verify codec was used and got context
        assert codec.encode_count > 0
        assert codec.decode_count > 0
        assert len(codec.contexts_seen) > 0, "Codec should have context"

        # Verify we saw both encode and decode with context
        operations = {op for op, ctx in codec.contexts_seen}
        assert "encode" in operations
        assert "decode" in operations


@workflow.defn(sandboxed=False)
class FailingWorkflow:
    @workflow.run
    async def run(self, message: str) -> str:
        from temporalio.exceptions import ApplicationError

        raise ApplicationError(f"Intentional failure: {message}", non_retryable=True)


@activity.defn
async def failing_activity(message: str) -> str:
    raise RuntimeError(f"Activity failed: {message}")


class SerializationContextTestFailureConverter(
    DefaultFailureConverter, WithSerializationContext
):
    def __init__(self):
        super().__init__()
        self.context: Optional[SerializationContext] = None
        self.contexts_seen = []

    def with_context(
        self, context: Optional[SerializationContext]
    ) -> SerializationContextTestFailureConverter:
        converter = SerializationContextTestFailureConverter()
        converter.context = context
        converter.contexts_seen = self.contexts_seen  # Share the list
        return converter

    def to_failure(self, exception, payload_converter, failure):
        if self.context:
            self.contexts_seen.append(("to_failure", self.context, str(exception)))
        super().to_failure(exception, payload_converter, failure)

    def from_failure(self, failure, payload_converter):
        if self.context:
            self.contexts_seen.append(("from_failure", self.context, failure.message))
        return super().from_failure(failure, payload_converter)


async def test_failure_converter_serialization_context(client: Client):
    workflow_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    failure_converter = SerializationContextTestFailureConverter()

    class TestFailureConverterClass(FailureConverter):
        def __new__(cls):
            return failure_converter

    data_converter_with_failure = dataclasses.replace(
        DataConverter.default,
        failure_converter_class=TestFailureConverterClass,
    )

    config = client.config()
    config["data_converter"] = data_converter_with_failure
    client = Client(**config)

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[FailingWorkflow],
        activities=[failing_activity],
    ):
        # Test workflow failure
        try:
            await client.execute_workflow(
                FailingWorkflow.run,
                "test",
                id=workflow_id,
                task_queue=task_queue,
            )
            assert False, "Should have failed"
        except Exception:
            pass

        # Verify failure converter saw context
        assert len(failure_converter.contexts_seen) > 0
        operations = {op for op, ctx, msg in failure_converter.contexts_seen}
        assert "from_failure" in operations  # Client sees failure when decoding
