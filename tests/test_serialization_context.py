from __future__ import annotations

import asyncio
import dataclasses
import inspect
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import timedelta
from itertools import zip_longest
from pprint import pformat
from typing import Any, List, Literal, Never, Optional, Sequence, Type
from warnings import warn

import pytest
from pydantic import BaseModel

from temporalio import activity, workflow
from temporalio.api.common.v1 import Payload
from temporalio.api.failure.v1 import Failure
from temporalio.client import Client, WorkflowFailureError, WorkflowUpdateFailedError
from temporalio.common import RetryPolicy
from temporalio.contrib.pydantic import PydanticJSONPlainPayloadConverter
from temporalio.converter import (
    ActivitySerializationContext,
    CompositePayloadConverter,
    DataConverter,
    DefaultFailureConverter,
    DefaultPayloadConverter,
    EncodingPayloadConverter,
    JSONPlainPayloadConverter,
    PayloadCodec,
    PayloadConverter,
    SerializationContext,
    WithSerializationContext,
    WorkflowSerializationContext,
)
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker
from temporalio.worker._workflow_instance import UnsandboxedWorkflowRunner


@dataclass
class TraceItem:
    context_type: Literal["workflow", "activity"]
    method: Literal[
        "to_payload",
        "from_payload",
        "to_failure",
        "from_failure",
    ]
    context: dict[str, Any]
    in_workflow: bool
    caller_location: list[str] = field(default_factory=list)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TraceItem):
            return False
        return (
            self.context_type == other.context_type
            and self.method == other.method
            and self.context == other.context
            and self.in_workflow == other.in_workflow
        )


@dataclass
class TraceData:
    items: list[TraceItem] = field(default_factory=list)


@activity.defn
async def passthrough_activity(input: TraceData) -> TraceData:
    activity.heartbeat(input)
    # Wait for the heartbeat to be processed so that it modifies the data before the activity returns
    await asyncio.sleep(0.2)
    return input


@workflow.defn
class EchoWorkflow:
    @workflow.run
    async def run(self, data: TraceData) -> TraceData:
        return data


@workflow.defn
class SerializationContextTestWorkflow:
    @workflow.run
    async def run(self, data: TraceData) -> TraceData:
        data = await workflow.execute_activity(
            passthrough_activity,
            data,
            start_to_close_timeout=timedelta(seconds=10),
            heartbeat_timeout=timedelta(seconds=2),
        )
        data = await workflow.execute_child_workflow(
            EchoWorkflow.run, data, id=f"{workflow.info().workflow_id}_child"
        )
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
        converter = SerializationContextTestEncodingPayloadConverter()
        converter.context = context
        return converter

    def to_payload(self, value: Any) -> Optional[Payload]:
        if not isinstance(value, TraceData):
            return None
        if not self.context:
            raise Exception("Context is None")
        if isinstance(self.context, WorkflowSerializationContext):
            value.items.append(
                TraceItem(
                    context_type="workflow",
                    in_workflow=workflow.in_workflow(),
                    method="to_payload",
                    context=dataclasses.asdict(self.context),
                    caller_location=get_caller_location(),
                )
            )
        elif isinstance(self.context, ActivitySerializationContext):
            value.items.append(
                TraceItem(
                    context_type="activity",
                    in_workflow=workflow.in_workflow(),
                    method="to_payload",
                    context=dataclasses.asdict(self.context),
                    caller_location=get_caller_location(),
                )
            )
        else:
            raise Exception(f"Unexpected context type: {type(self.context)}")
        payload = JSONPlainPayloadConverter().to_payload(value)
        assert payload
        payload.metadata["encoding"] = self.encoding.encode()
        return payload

    def from_payload(self, payload: Payload, type_hint: Optional[Type] = None) -> Any:
        # Always deserialize as TraceData since that's what this converter handles
        value = JSONPlainPayloadConverter().from_payload(payload, TraceData)
        assert isinstance(value, TraceData)
        if not self.context:
            raise Exception("Context is None")
        if isinstance(self.context, WorkflowSerializationContext):
            value.items.append(
                TraceItem(
                    context_type="workflow",
                    in_workflow=workflow.in_workflow(),
                    method="from_payload",
                    context=dataclasses.asdict(self.context),
                    caller_location=get_caller_location(),
                )
            )
        elif isinstance(self.context, ActivitySerializationContext):
            value.items.append(
                TraceItem(
                    context_type="activity",
                    in_workflow=workflow.in_workflow(),
                    method="from_payload",
                    context=dataclasses.asdict(self.context),
                    caller_location=get_caller_location(),
                )
            )
        else:
            raise Exception(f"Unexpected context type: {type(self.context)}")
        return value


class SerializationContextTestPayloadConverter(
    CompositePayloadConverter, WithSerializationContext
):
    def __init__(self):
        super().__init__(
            SerializationContextTestEncodingPayloadConverter(),
            *DefaultPayloadConverter.default_encoding_payload_converters,
        )


async def test_workflow_payload_conversion(
    client: Client,
):
    workflow_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    config = client.config()
    config["data_converter"] = dataclasses.replace(
        DataConverter.default,
        payload_converter_class=SerializationContextTestPayloadConverter,
    )
    client = Client(**config)

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[SerializationContextTestWorkflow, EchoWorkflow],
        activities=[passthrough_activity],
        workflow_runner=UnsandboxedWorkflowRunner(),  # so that we can use isinstance
    ):
        result = await client.execute_workflow(
            SerializationContextTestWorkflow.run,
            TraceData(),
            id=workflow_id,
            task_queue=task_queue,
        )

        workflow_context = dataclasses.asdict(
            WorkflowSerializationContext(
                namespace="default",
                workflow_id=workflow_id,
            )
        )
        child_workflow_context = dataclasses.asdict(
            WorkflowSerializationContext(
                namespace="default",
                workflow_id=f"{workflow_id}_child",
            )
        )
        activity_context = dataclasses.asdict(
            ActivitySerializationContext(
                namespace="default",
                workflow_id=workflow_id,
                workflow_type="SerializationContextTestWorkflow",
                activity_type="passthrough_activity",
                activity_task_queue=task_queue,
                is_local=False,
            )
        )
        assert_trace(
            result.items,
            [
                TraceItem(
                    context_type="workflow",
                    in_workflow=False,
                    method="to_payload",
                    context=workflow_context,  # Outbound workflow input
                ),
                TraceItem(
                    context_type="workflow",
                    in_workflow=False,
                    method="from_payload",
                    context=workflow_context,  # Inbound workflow input
                ),
                TraceItem(
                    context_type="activity",
                    in_workflow=True,
                    method="to_payload",
                    context=activity_context,  # Outbound activity input
                ),
                TraceItem(
                    context_type="activity",
                    in_workflow=False,
                    method="from_payload",
                    context=activity_context,  # Inbound activity input
                ),
                TraceItem(
                    context_type="activity",
                    in_workflow=False,
                    method="to_payload",
                    context=activity_context,  # Outbound heartbeat
                ),
                TraceItem(
                    context_type="activity",
                    in_workflow=False,
                    method="to_payload",
                    context=activity_context,  # Outbound activity result
                ),
                TraceItem(
                    context_type="activity",
                    in_workflow=False,
                    method="from_payload",
                    context=activity_context,  # Inbound activity result
                ),
                TraceItem(
                    context_type="workflow",
                    in_workflow=True,
                    method="to_payload",
                    context=child_workflow_context,  # Outbound child workflow input
                ),
                TraceItem(
                    context_type="workflow",
                    in_workflow=False,
                    method="from_payload",
                    context=child_workflow_context,  # Inbound child workflow input
                ),
                TraceItem(
                    context_type="workflow",
                    in_workflow=True,
                    method="to_payload",
                    context=child_workflow_context,  # Outbound child workflow result
                ),
                TraceItem(
                    context_type="workflow",
                    in_workflow=False,
                    method="from_payload",
                    context=child_workflow_context,  # Inbound child workflow result
                ),
                TraceItem(
                    context_type="workflow",
                    in_workflow=True,
                    method="to_payload",
                    context=workflow_context,  # Outbound workflow result
                ),
                TraceItem(
                    context_type="workflow",
                    in_workflow=False,
                    method="from_payload",
                    context=workflow_context,  # Inbound workflow result
                ),
            ],
        )


# Activity with heartbeat details test


@activity.defn
async def activity_with_heartbeat_details() -> TraceData:
    """Activity that checks heartbeat details are decoded with proper context."""
    info = activity.info()

    # If we have heartbeat details, it means we're resuming from a previous attempt
    if info.heartbeat_details:
        # The heartbeat details should be a TraceData that was decoded with activity context
        assert len(info.heartbeat_details) == 1
        heartbeat_data = info.heartbeat_details[0]
        assert isinstance(heartbeat_data, TraceData)
        # Return the heartbeat data which should contain the decode trace
        return heartbeat_data

    # First attempt - heartbeat and then fail
    data = TraceData()
    activity.heartbeat(data)
    # Wait a bit to ensure heartbeat is recorded
    await asyncio.sleep(0.1)
    # Fail to trigger retry with heartbeat details
    raise Exception("Intentional failure to test heartbeat details")


@workflow.defn
class HeartbeatDetailsSerializationContextTestWorkflow:
    @workflow.run
    async def run(self) -> TraceData:
        return await workflow.execute_activity(
            activity_with_heartbeat_details,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=100),
                maximum_attempts=2,
            ),
        )


async def test_heartbeat_details_payload_conversion(client: Client):
    """Test that heartbeat details are decoded with activity context."""
    workflow_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    config = client.config()
    config["data_converter"] = dataclasses.replace(
        DataConverter.default,
        payload_converter_class=SerializationContextTestPayloadConverter,
    )

    client = Client(**config)

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[HeartbeatDetailsSerializationContextTestWorkflow],
        activities=[activity_with_heartbeat_details],
        workflow_runner=UnsandboxedWorkflowRunner(),  # so that we can use isinstance
    ):
        result = await client.execute_workflow(
            HeartbeatDetailsSerializationContextTestWorkflow.run,
            id=workflow_id,
            task_queue=task_queue,
        )

        activity_context = dataclasses.asdict(
            ActivitySerializationContext(
                namespace="default",
                workflow_id=workflow_id,
                workflow_type="HeartbeatDetailsSerializationContextTestWorkflow",
                activity_type="activity_with_heartbeat_details",
                activity_task_queue=task_queue,
                is_local=False,
            )
        )

        # The result should contain the heartbeat data that was decoded with activity context
        # We expect to see the from_payload trace item for the heartbeat details
        # This test will FAIL until the bug is fixed
        found_heartbeat_decode = False
        for item in result.items:
            if (
                item.context_type == "activity"
                and item.method == "from_payload"
                and not item.in_workflow
                and item.context == activity_context
            ):
                found_heartbeat_decode = True
                break

        assert (
            found_heartbeat_decode
        ), "Heartbeat details should be decoded with activity context"


# Async activity completion test
@activity.defn
async def async_activity() -> TraceData:
    # Signal that activity has started via heartbeat
    activity.heartbeat("started")
    activity.raise_complete_async()


@workflow.defn
class AsyncActivityCompletionSerializationContextTestWorkflow:
    @workflow.run
    async def run(self) -> TraceData:
        return await workflow.execute_activity(
            async_activity,
            start_to_close_timeout=timedelta(seconds=10),
            activity_id="async-activity-id",
        )


async def test_async_activity_completion_payload_conversion(
    client: Client,
):
    workflow_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    config = client.config()
    config["data_converter"] = dataclasses.replace(
        DataConverter.default,
        payload_converter_class=SerializationContextTestPayloadConverter,
    )

    client = Client(**config)

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[AsyncActivityCompletionSerializationContextTestWorkflow],
        activities=[async_activity],
        workflow_runner=UnsandboxedWorkflowRunner(),  # so that we can use isinstance
    ):
        wf_handle = await client.start_workflow(
            AsyncActivityCompletionSerializationContextTestWorkflow.run,
            id=workflow_id,
            task_queue=task_queue,
        )
        activity_handle = client.get_async_activity_handle(
            workflow_id=workflow_id,
            run_id=wf_handle.first_execution_run_id,
            activity_id="async-activity-id",
        )
        # Wait a bit for the activity to start
        await asyncio.sleep(0.5)
        data = TraceData()
        await activity_handle.heartbeat(data)
        await activity_handle.complete(data)
        result = await wf_handle.result()

        # project down since activity completion by a client does not have access to most activity
        # context fields
        def project(trace_item: TraceItem) -> tuple[str, bool, str]:
            return (
                trace_item.context_type,
                trace_item.in_workflow,
                trace_item.method,
            )

        assert [project(item) for item in result.items] == [
            (
                "activity",
                False,
                "to_payload",  # Outbound activity input
            ),
            (
                "activity",
                False,
                "to_payload",  # Outbound activity heartbeat data
            ),
            (
                "activity",
                False,
                "from_payload",  # Inbound activity result
            ),
            (
                "workflow",
                True,
                "to_payload",  # Outbound workflow result
            ),
            (
                "workflow",
                False,
                "from_payload",  # Inbound workflow result
            ),
        ]


# Signal test


@workflow.defn(sandboxed=False)  # so that we can use isinstance
class SignalSerializationContextTestWorkflow:
    def __init__(self) -> None:
        self.signal_received: Optional[TraceData] = None

    @workflow.run
    async def run(self) -> TraceData:
        await workflow.wait_condition(lambda: self.signal_received is not None)
        assert self.signal_received is not None
        return self.signal_received

    @workflow.signal
    async def my_signal(self, data: TraceData) -> None:
        self.signal_received = data


async def test_signal_payload_conversion(
    client: Client,
):
    workflow_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    config = client.config()
    config["data_converter"] = dataclasses.replace(
        DataConverter.default,
        payload_converter_class=SerializationContextTestPayloadConverter,
    )

    custom_client = Client(**config)

    async with Worker(
        custom_client,
        task_queue=task_queue,
        workflows=[SignalSerializationContextTestWorkflow],
        activities=[],
        workflow_runner=UnsandboxedWorkflowRunner(),  # so that we can use isinstance
    ):
        handle = await custom_client.start_workflow(
            SignalSerializationContextTestWorkflow.run,
            id=workflow_id,
            task_queue=task_queue,
        )
        await handle.signal(
            SignalSerializationContextTestWorkflow.my_signal,
            TraceData(),
        )
        result = await handle.result()

        workflow_context = dataclasses.asdict(
            WorkflowSerializationContext(
                namespace="default",
                workflow_id=workflow_id,
            )
        )
        assert_trace(
            result.items,
            [
                TraceItem(
                    context_type="workflow",
                    in_workflow=False,
                    method="to_payload",
                    context=workflow_context,  # Outbound signal input
                ),
                TraceItem(
                    context_type="workflow",
                    in_workflow=False,
                    method="from_payload",
                    context=workflow_context,  # Inbound signal input
                ),
                TraceItem(
                    context_type="workflow",
                    in_workflow=True,
                    method="to_payload",
                    context=workflow_context,  # Outbound workflow result
                ),
                TraceItem(
                    context_type="workflow",
                    in_workflow=False,
                    method="from_payload",
                    context=workflow_context,  # Inbound workflow result
                ),
            ],
        )


# Query test


@workflow.defn
class QuerySerializationContextTestWorkflow:
    @workflow.run
    async def run(self) -> None:
        await asyncio.Event().wait()

    @workflow.query
    def my_query(self, input: TraceData) -> TraceData:
        return input


async def test_query_payload_conversion(
    client: Client,
):
    workflow_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    config = client.config()
    config["data_converter"] = dataclasses.replace(
        DataConverter.default,
        payload_converter_class=SerializationContextTestPayloadConverter,
    )
    custom_client = Client(**config)

    async with Worker(
        custom_client,
        task_queue=task_queue,
        workflows=[QuerySerializationContextTestWorkflow],
        activities=[],
        workflow_runner=UnsandboxedWorkflowRunner(),  # so that we can use isinstance
    ):
        handle = await custom_client.start_workflow(
            QuerySerializationContextTestWorkflow.run,
            id=workflow_id,
            task_queue=task_queue,
        )
        result = await handle.query(
            QuerySerializationContextTestWorkflow.my_query, TraceData()
        )

        workflow_context = dataclasses.asdict(
            WorkflowSerializationContext(
                namespace="default",
                workflow_id=workflow_id,
            )
        )
        assert_trace(
            result.items,
            [
                TraceItem(
                    context_type="workflow",
                    in_workflow=False,
                    method="to_payload",
                    context=workflow_context,  # Outbound query input
                ),
                TraceItem(
                    context_type="workflow",
                    in_workflow=True,
                    method="from_payload",
                    context=workflow_context,  # Inbound query input
                ),
                TraceItem(
                    context_type="workflow",
                    in_workflow=True,
                    method="to_payload",
                    context=workflow_context,  # Outbound query result
                ),
                TraceItem(
                    context_type="workflow",
                    in_workflow=False,
                    method="from_payload",
                    context=workflow_context,  # Inbound query result
                ),
            ],
        )


# Update test


@workflow.defn
class UpdateSerializationContextTestWorkflow:
    @workflow.init
    def __init__(self, pass_validation: bool) -> None:
        self.pass_validation = pass_validation
        self.input: Optional[TraceData] = None

    @workflow.run
    async def run(self, pass_validation: bool) -> TraceData:
        await workflow.wait_condition(lambda: self.input is not None)
        assert self.input
        return self.input

    @workflow.update
    def my_update(self, input: TraceData) -> TraceData:
        return input

    @my_update.validator
    def my_update_validator(self, input: TraceData) -> None:
        self.input = input  # for test purposes; update validators should not mutate workflow state
        if not self.pass_validation:
            raise ValueError("Rejected")


@pytest.mark.parametrize("pass_validation", [True, False])
async def test_update_payload_conversion(
    client: Client,
    pass_validation: bool,
):
    workflow_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    config = client.config()
    config["data_converter"] = dataclasses.replace(
        DataConverter.default,
        payload_converter_class=SerializationContextTestPayloadConverter,
    )
    custom_client = Client(**config)

    async with Worker(
        custom_client,
        task_queue=task_queue,
        workflows=[UpdateSerializationContextTestWorkflow],
        activities=[],
        workflow_runner=UnsandboxedWorkflowRunner(),  # so that we can use isinstance
    ):
        wf_handle = await custom_client.start_workflow(
            UpdateSerializationContextTestWorkflow.run,
            pass_validation,
            id=workflow_id,
            task_queue=task_queue,
        )
        if pass_validation:
            result = await wf_handle.execute_update(
                UpdateSerializationContextTestWorkflow.my_update, TraceData()
            )
        else:
            try:
                await wf_handle.execute_update(
                    UpdateSerializationContextTestWorkflow.my_update, TraceData()
                )
                raise AssertionError("Expected WorkflowUpdateFailedError")
            except WorkflowUpdateFailedError:
                pass

            result = await wf_handle.result()

        workflow_context = dataclasses.asdict(
            WorkflowSerializationContext(
                namespace="default",
                workflow_id=workflow_id,
            )
        )
        assert_trace(
            result.items,
            [
                TraceItem(
                    context_type="workflow",
                    in_workflow=False,
                    method="to_payload",
                    context=workflow_context,  # Outbound update input
                ),
                TraceItem(
                    context_type="workflow",
                    in_workflow=True,
                    method="from_payload",
                    context=workflow_context,  # Inbound update input
                ),
                TraceItem(
                    context_type="workflow",
                    in_workflow=True,
                    method="to_payload",
                    context=workflow_context,  # Outbound update/workflow result
                ),
                TraceItem(
                    context_type="workflow",
                    in_workflow=False,
                    method="from_payload",
                    context=workflow_context,  # Inbound update/workflow result
                ),
            ],
        )


# External workflow test


@workflow.defn
class ExternalWorkflowTarget:
    def __init__(self) -> None:
        self.signal_received: Optional[TraceData] = None

    @workflow.run
    async def run(self) -> TraceData:
        try:
            # Wait for signal
            await workflow.wait_condition(lambda: self.signal_received is not None)
            return self.signal_received or TraceData()
        except asyncio.CancelledError:
            # Return empty data on cancellation
            return TraceData()

    @workflow.signal
    async def external_signal(self, data: TraceData) -> None:
        self.signal_received = data


@workflow.defn
class ExternalWorkflowSignaler:
    @workflow.run
    async def run(self, target_id: str, data: TraceData) -> TraceData:
        # Signal external workflow
        handle = workflow.get_external_workflow_handle(target_id)
        await handle.signal(ExternalWorkflowTarget.external_signal, data)
        return data


@workflow.defn
class ExternalWorkflowCanceller:
    @workflow.run
    async def run(self, target_id: str) -> TraceData:
        # Cancel external workflow
        handle = workflow.get_external_workflow_handle(target_id)
        await handle.cancel()
        return TraceData()


@pytest.mark.timeout(10)
async def test_external_workflow_signal_and_cancel_payload_conversion(
    client: Client,
):
    target_workflow_id = str(uuid.uuid4())
    signaler_workflow_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    config = client.config()
    config["data_converter"] = dataclasses.replace(
        DataConverter.default,
        payload_converter_class=SerializationContextTestPayloadConverter,
    )
    custom_client = Client(**config)

    async with Worker(
        custom_client,
        task_queue=task_queue,
        workflows=[
            ExternalWorkflowTarget,
            ExternalWorkflowSignaler,
            ExternalWorkflowCanceller,
        ],
        activities=[],
        workflow_runner=UnsandboxedWorkflowRunner(),  # so that we can use isinstance
    ):
        # Test external signal
        target_handle = await custom_client.start_workflow(
            ExternalWorkflowTarget.run,
            id=target_workflow_id,
            task_queue=task_queue,
        )

        signaler_handle = await custom_client.start_workflow(
            ExternalWorkflowSignaler.run,
            args=[target_workflow_id, TraceData()],
            id=signaler_workflow_id,
            task_queue=task_queue,
        )

        # Wait for both to complete
        signaler_result = await signaler_handle.result()
        await target_handle.result()

        # Verify signal trace
        signaler_context = dataclasses.asdict(
            WorkflowSerializationContext(
                namespace="default",
                workflow_id=signaler_workflow_id,
            )
        )
        target_context = dataclasses.asdict(
            WorkflowSerializationContext(
                namespace="default",
                workflow_id=target_workflow_id,
            )
        )

        # This test verifies that external signals SHOULD use the target workflow's context
        # This is the DESIRED behavior to match .NET
        assert_trace(
            signaler_result.items,
            [
                TraceItem(
                    context_type="workflow",
                    in_workflow=False,
                    method="to_payload",
                    context=signaler_context,  # Outbound signaler workflow input
                ),
                TraceItem(
                    context_type="workflow",
                    in_workflow=False,
                    method="from_payload",
                    context=signaler_context,  # Inbound signaler workflow input
                ),
                TraceItem(
                    context_type="workflow",
                    in_workflow=True,
                    method="to_payload",
                    context=target_context,  # Should use target workflow's context for external signal
                ),
                TraceItem(
                    context_type="workflow",
                    in_workflow=True,
                    method="to_payload",
                    context=signaler_context,  # Outbound signaler workflow result
                ),
                TraceItem(
                    context_type="workflow",
                    in_workflow=False,
                    method="from_payload",
                    context=signaler_context,  # Inbound signaler workflow result
                ),
            ],
        )

        # Note: External cancel doesn't send payloads, so we don't test it here
        # The cancel context would only be used for failure deserialization


# Failure conversion


@activity.defn
async def failing_activity() -> Never:
    raise ApplicationError("test error", dataclasses.asdict(TraceData()))


@workflow.defn
class FailureConverterTestWorkflow:
    @workflow.run
    async def run(self) -> Never:
        await workflow.execute_activity(
            failing_activity,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )
        raise Exception("Unreachable")


failure_converter_test_trace: dict[str, list[TraceItem]] = defaultdict(list)


class FailureConverterWithContext(DefaultFailureConverter, WithSerializationContext):
    def __init__(self):
        super().__init__(encode_common_attributes=False)
        self.context: Optional[SerializationContext] = None

    def with_context(
        self, context: Optional[SerializationContext]
    ) -> "FailureConverterWithContext":
        converter = FailureConverterWithContext()
        converter.context = context
        return converter

    def to_failure(
        self,
        exception: BaseException,
        payload_converter: PayloadConverter,
        failure: Failure,
    ) -> None:
        if isinstance(self.context, WorkflowSerializationContext):
            context_type = "workflow"
        elif isinstance(self.context, ActivitySerializationContext):
            context_type = "activity"
        else:
            raise TypeError(f"self.context is {type(self.context)}")

        failure_converter_test_trace[self.context.workflow_id].append(
            TraceItem(
                context_type=context_type,
                in_workflow=workflow.in_workflow(),
                method="to_failure",
                context=dataclasses.asdict(self.context),
            )
        )
        super().to_failure(exception, payload_converter, failure)

    def from_failure(
        self, failure: Failure, payload_converter: PayloadConverter
    ) -> BaseException:
        # Let the base class create the exception
        if isinstance(self.context, WorkflowSerializationContext):
            context_type = "workflow"
        elif isinstance(self.context, ActivitySerializationContext):
            context_type = "activity"
        else:
            raise TypeError(f"self.context is {type(self.context)}")

        failure_converter_test_trace[self.context.workflow_id].append(
            TraceItem(
                context_type=context_type,
                in_workflow=workflow.in_workflow(),
                method="from_failure",
                context=dataclasses.asdict(self.context),
                # caller_location=get_caller_location(),
            )
        )
        return super().from_failure(failure, payload_converter)


async def test_failure_converter_with_context(client: Client):
    workflow_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    data_converter = dataclasses.replace(
        DataConverter.default,
        failure_converter_class=FailureConverterWithContext,
    )
    test_client = Client(
        client.service_client,
        namespace=client.namespace,
        data_converter=data_converter,
    )
    async with Worker(
        test_client,
        task_queue=task_queue,
        workflows=[FailureConverterTestWorkflow],
        activities=[failing_activity],
        workflow_runner=UnsandboxedWorkflowRunner(),
    ):
        try:
            await test_client.execute_workflow(
                FailureConverterTestWorkflow.run,
                id=workflow_id,
                task_queue=task_queue,
            )
            raise AssertionError("unreachable")
        except WorkflowFailureError:
            pass

        assert isinstance(data_converter.failure_converter, FailureConverterWithContext)

        workflow_context = dataclasses.asdict(
            WorkflowSerializationContext(
                namespace="default",
                workflow_id=workflow_id,
            )
        )
        activity_context = dataclasses.asdict(
            ActivitySerializationContext(
                namespace="default",
                workflow_id=workflow_id,
                workflow_type="FailureConverterTestWorkflow",
                activity_type="failing_activity",
                activity_task_queue=task_queue,
                is_local=False,
            )
        )
        assert_trace(
            failure_converter_test_trace[workflow_id],
            [
                TraceItem(
                    context_type="activity",
                    context=activity_context,
                    in_workflow=False,
                    method="to_failure",  # outbound activity result
                )
            ]
            + (
                [
                    TraceItem(
                        context_type="activity",
                        context=activity_context,
                        in_workflow=False,
                        method="from_failure",  # inbound activity result
                    )
                ]
                * 2  # from_failure deserializes the error and error cause
            )
            + [
                TraceItem(
                    context_type="workflow",
                    context=workflow_context,
                    in_workflow=True,
                    method="to_failure",  # outbound workflow result
                )
            ]
            + (
                [
                    TraceItem(
                        context_type="workflow",
                        context=workflow_context,
                        in_workflow=False,
                        method="from_failure",  # inbound workflow result
                    )
                ]
                * 2  # from_failure deserializes the error and error cause
            ),
        )
        del failure_converter_test_trace[workflow_id]


class PayloadCodecWithContext(PayloadCodec, WithSerializationContext):
    def __init__(self):
        self.context: Optional[SerializationContext] = None
        self.encode_called_with_context = False
        self.decode_called_with_context = False

    def with_context(
        self, context: Optional[SerializationContext]
    ) -> "PayloadCodecWithContext":
        codec = PayloadCodecWithContext()
        codec.context = context
        return codec

    async def encode(self, payloads: Sequence[Payload]) -> List[Payload]:
        result = []
        for p in payloads:
            new_p = Payload()
            new_p.CopyFrom(p)
            if self.context:
                self.encode_called_with_context = True
                new_p.metadata["has_context"] = b"true"
            result.append(new_p)
        return result

    async def decode(self, payloads: Sequence[Payload]) -> List[Payload]:
        result = []
        for p in payloads:
            new_p = Payload()
            new_p.CopyFrom(p)
            if self.context and new_p.metadata.get("has_context") == b"true":
                self.decode_called_with_context = True
                del new_p.metadata["has_context"]
            result.append(new_p)
        return result


@workflow.defn
class CodecTestWorkflow:
    @workflow.run
    async def run(self, data: str) -> str:
        return data


async def test_codec_with_context(client: Client):
    wf_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())
    test_client = Client(
        client.service_client,
        namespace=client.namespace,
        data_converter=dataclasses.replace(
            DataConverter.default, payload_codec=PayloadCodecWithContext()
        ),
    )
    async with Worker(
        test_client,
        task_queue=task_queue,
        workflows=[CodecTestWorkflow],
    ):
        await test_client.execute_workflow(
            CodecTestWorkflow.run,
            "data",
            id=wf_id,
            task_queue=task_queue,
        )


# Pydantic


class PydanticData(BaseModel):
    value: str
    trace: List[str] = []


class ContextPydanticJSONConverter(
    PydanticJSONPlainPayloadConverter, WithSerializationContext
):
    def __init__(self):
        super().__init__()
        self.context: Optional[SerializationContext] = None

    def with_context(
        self, context: Optional[SerializationContext]
    ) -> "ContextPydanticJSONConverter":
        converter = ContextPydanticJSONConverter()
        converter.context = context
        return converter

    def to_payload(self, value: Any) -> Optional[Payload]:
        if isinstance(value, PydanticData) and self.context:
            if isinstance(self.context, WorkflowSerializationContext):
                value.trace.append(f"wf_{self.context.workflow_id}")
        return super().to_payload(value)


class ContextPydanticConverter(CompositePayloadConverter, WithSerializationContext):
    def __init__(self):
        self.json_converter = ContextPydanticJSONConverter()
        super().__init__(
            *(
                c
                if not isinstance(c, JSONPlainPayloadConverter)
                else self.json_converter
                for c in DefaultPayloadConverter.default_encoding_payload_converters
            )
        )
        self.context: Optional[SerializationContext] = None

    def with_context(
        self, context: Optional[SerializationContext]
    ) -> "ContextPydanticConverter":
        converter = ContextPydanticConverter()
        converter.context = context
        # Also set context on all sub-converters
        converters: list[EncodingPayloadConverter] = []
        for c in self.converters.values():
            if isinstance(c, WithSerializationContext):
                converters.append(c.with_context(context))
            else:
                converters.append(c)
        CompositePayloadConverter.__init__(converter, *converters)
        return converter


@workflow.defn
class PydanticContextWorkflow:
    @workflow.run
    async def run(self, data: PydanticData) -> PydanticData:
        data.value += "_processed"
        return data


async def test_pydantic_converter_with_context(client: Client):
    wf_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    test_client = Client(
        client.service_client,
        namespace=client.namespace,
        data_converter=DataConverter(
            payload_converter_class=ContextPydanticConverter,
        ),
    )
    async with Worker(
        test_client,
        task_queue=task_queue,
        workflows=[PydanticContextWorkflow],
    ):
        result = await test_client.execute_workflow(
            PydanticContextWorkflow.run,
            PydanticData(value="test"),
            id=wf_id,
            task_queue=task_queue,
        )
        assert result.value == "test_processed"
        assert f"wf_{wf_id}" in result.trace


# Utilities


def assert_trace(trace: list[TraceItem], expected: list[TraceItem]):
    if len(trace) != len(expected):
        warn(f"expected {len(expected)} trace items but received {len(trace)}")
    history: list[str] = []
    for item, expected_item in zip_longest(trace, expected):
        if item is None:
            raise AssertionError(
                f"Fewer items in trace than expected.\n\n History:\n{'\n'.join(history)}"
            )
        if expected_item is None:
            raise AssertionError(
                f"More items in trace than expected.\n\n History:\n{'\n'.join(history)}"
            )
        if item != expected_item:
            raise AssertionError(
                f"Item:\n{pformat(item)}\n\ndoes not match expected:\n\n {pformat(expected_item)}.\n\n History:\n{'\n'.join(history)}"
            )
        history.append(f"{item.context_type} {item.method}")


def get_caller_location() -> list[str]:
    """Get 3 stack frames starting from the first that's not in test_serialization_context.py or temporalio/converter.py."""
    frame = inspect.currentframe()
    result: list[str] = []
    found_first = False

    # Walk up the stack
    while frame and len(result) < 3:
        frame = frame.f_back
        if not frame:
            break

        file_path = frame.f_code.co_filename

        # Skip frames from test file and converter.py until we find the first one
        if not found_first:
            if "test_serialization_context.py" in file_path:
                continue
            if file_path.endswith("temporalio/converter.py"):
                continue
            found_first = True

        result.append(f"{file_path}:{frame.f_lineno}")

    # Pad with "unknown:0" if we didn't get 3 frames
    while len(result) < 3:
        result.append("unknown:0")

    return result
