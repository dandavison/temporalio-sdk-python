import asyncio
import uuid
from dataclasses import dataclass
from datetime import timedelta

import pytest

from temporalio import activity, workflow
from temporalio.client import (
    ActivityExecutionCountAggregationGroup,
    ActivityFailedError,
    Client,
    Interceptor,
    OutboundInterceptor,
)
from temporalio.common import ActivityExecutionStatus
from temporalio.exceptions import ApplicationError, CancelledError
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker import Worker
from tests.helpers import assert_eq_eventually


@activity.defn
async def increment(input: int) -> int:
    return input + 1


async def test_describe(client: Client):
    activity_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    activity_handle = await client.start_activity(
        increment,
        args=(1,),
        id=activity_id,
        task_queue=task_queue,
        start_to_close_timeout=timedelta(seconds=5),
    )
    desc = await activity_handle.describe()
    assert desc.activity_id == activity_id
    assert desc.activity_run_id == activity_handle.activity_run_id
    assert desc.activity_type == "increment"
    assert desc.task_queue == task_queue
    assert desc.status == ActivityExecutionStatus.RUNNING
    assert isinstance(desc.eager_execution_requested, bool)
    assert isinstance(desc.paused, bool)


def test_get_activity_result_input_exists():
    """GetActivityResultInput dataclass should be importable from temporalio.client."""
    from temporalio.client import GetActivityResultInput

    # Verify it has the expected fields per spec
    assert hasattr(GetActivityResultInput, "__dataclass_fields__")
    fields = GetActivityResultInput.__dataclass_fields__
    assert "activity_id" in fields
    assert "activity_run_id" in fields
    assert "result_type" in fields
    assert "rpc_metadata" in fields
    assert "rpc_timeout" in fields


def test_outbound_interceptor_has_get_activity_result_method():
    """OutboundInterceptor should have a get_activity_result method."""
    assert hasattr(OutboundInterceptor, "get_activity_result")
    # Check it's a callable method
    import inspect

    assert inspect.isfunction(
        OutboundInterceptor.get_activity_result
    ) or inspect.ismethod(OutboundInterceptor.get_activity_result)


class ActivityResultTracingInterceptor(Interceptor):
    """Test interceptor that tracks get_activity_result calls."""

    def __init__(self):
        self.get_activity_result_calls: list = []

    def intercept_client(self, next: OutboundInterceptor) -> OutboundInterceptor:
        return ActivityResultTracingOutboundInterceptor(self, next)


class ActivityResultTracingOutboundInterceptor(OutboundInterceptor):
    def __init__(
        self,
        parent: ActivityResultTracingInterceptor,
        next: OutboundInterceptor,
    ) -> None:
        super().__init__(next)
        self._parent = parent

    async def get_activity_result(self, input):
        """Track calls to get_activity_result."""
        from temporalio.client import GetActivityResultInput

        assert isinstance(input, GetActivityResultInput)
        self._parent.get_activity_result_calls.append(input)
        return await super().get_activity_result(input)


async def test_activity_result_calls_interceptor(client: Client):
    """ActivityHandle.result() should call the get_activity_result interceptor."""
    interceptor = ActivityResultTracingInterceptor()

    # Create a new client with the interceptor
    intercepted_client = Client(
        service_client=client.service_client,
        namespace=client.namespace,
        interceptors=[interceptor],
    )

    activity_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    activity_handle = await intercepted_client.start_activity(
        increment,
        args=(1,),
        id=activity_id,
        task_queue=task_queue,
        start_to_close_timeout=timedelta(seconds=5),
    )

    async with Worker(
        intercepted_client,
        task_queue=task_queue,
        activities=[increment],
    ):
        result = await activity_handle.result()
        assert result == 2

    # Verify interceptor was called
    assert len(interceptor.get_activity_result_calls) == 1
    call = interceptor.get_activity_result_calls[0]
    assert call.activity_id == activity_id


async def test_get_result(client: Client):
    activity_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    activity_handle = await client.start_activity(
        increment,
        args=(1,),
        id=activity_id,
        task_queue=task_queue,
        start_to_close_timeout=timedelta(seconds=5),
    )
    result_via_execute_activity = client.execute_activity(
        increment,
        args=(1,),
        id=activity_id,
        task_queue=task_queue,
        start_to_close_timeout=timedelta(seconds=5),
    )

    async with Worker(
        client,
        task_queue=task_queue,
        activities=[increment],
    ):
        assert await activity_handle.result() == 2
        assert await result_via_execute_activity == 2


async def test_get_activity_handle(client: Client):
    activity_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    activity_handle = await client.start_activity(
        increment,
        1,
        id=activity_id,
        task_queue=task_queue,
        start_to_close_timeout=timedelta(seconds=5),
    )

    handle_by_id = client.get_activity_handle(activity_id)
    assert handle_by_id.activity_id == activity_id
    assert handle_by_id.activity_run_id is None

    handle_by_id_and_run_id = client.get_activity_handle(
        activity_id,
        activity_run_id=activity_handle.activity_run_id,
    )
    assert handle_by_id_and_run_id.activity_id == activity_id
    assert handle_by_id_and_run_id.activity_run_id == activity_handle.activity_run_id

    handle_with_result_type = client.get_activity_handle(
        activity_id,
        result_type=int,
        activity_run_id=activity_handle.activity_run_id,
    )

    async with Worker(
        client,
        task_queue=task_queue,
        activities=[increment],
    ):
        assert await handle_by_id.result() == 2
        assert await handle_by_id_and_run_id.result() == 2
        assert await handle_with_result_type.result() == 2


async def test_list_activities(client: Client):
    activity_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    await client.start_activity(
        increment,
        1,
        id=activity_id,
        task_queue=task_queue,
        start_to_close_timeout=timedelta(seconds=5),
    )

    executions = [
        e async for e in client.list_activities(f'ActivityId = "{activity_id}"')
    ]
    assert len(executions) == 1
    execution = executions[0]
    assert execution.activity_id == activity_id
    assert execution.activity_type == "increment"
    assert execution.task_queue == task_queue
    assert execution.status == ActivityExecutionStatus.RUNNING
    # TODO: not being set by server?
    # assert isinstance(execution.state_transition_count, int)


async def test_count_activities(client: Client):
    activity_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    await client.start_activity(
        increment,
        1,
        id=activity_id,
        task_queue=task_queue,
        start_to_close_timeout=timedelta(seconds=5),
    )

    count = await client.count_activities(f'ActivityId = "{activity_id}"')
    assert count.count == 1
    assert count.groups == []


async def test_count_activities_group_by(client: Client):
    from temporalio.client import ActivityExecutionCount

    task_queue = str(uuid.uuid4())
    activity_ids = []

    for _ in range(3):
        activity_id = str(uuid.uuid4())
        activity_ids.append(activity_id)
        await client.start_activity(
            increment,
            1,
            id=activity_id,
            task_queue=task_queue,
            schedule_to_close_timeout=timedelta(seconds=60),
        )

    ids_filter = " OR ".join([f'ActivityId = "{aid}"' for aid in activity_ids])

    async def fetch_count() -> ActivityExecutionCount:
        return await client.count_activities(f"({ids_filter}) GROUP BY ExecutionStatus")

    await assert_eq_eventually(
        ActivityExecutionCount(
            count=3,
            groups=[
                ActivityExecutionCountAggregationGroup(
                    count=3, group_values=["Running"]
                ),
            ],
        ),
        fetch_count,
    )


@dataclass
class ActivityInput:
    event_workflow_id: str
    wait_for_activity_start_workflow_id: str | None = None


@activity.defn
async def async_activity(input: ActivityInput) -> int:
    # Notify test that the activity has started and is ready to be completed manually
    await (
        activity.client()
        .get_workflow_handle(input.event_workflow_id)
        .signal(EventWorkflow.set)
    )
    activity.raise_complete_async()


async def test_manual_completion(client: Client):
    activity_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())
    event_workflow_id = str(uuid.uuid4())

    activity_handle = await client.start_activity(
        async_activity,
        args=(ActivityInput(event_workflow_id=event_workflow_id),),  # TODO: overloads
        id=activity_id,
        task_queue=task_queue,
        start_to_close_timeout=timedelta(seconds=5),
    )

    async with Worker(
        client,
        task_queue=task_queue,
        activities=[async_activity],
        workflows=[EventWorkflow],
    ):
        # Wait for activity to start
        await client.execute_workflow(
            EventWorkflow.wait,
            id=event_workflow_id,
            task_queue=task_queue,
        )
        # Complete activity manually
        async_activity_handle = client.get_async_activity_handle(
            activity_id=activity_id,
            run_id=activity_handle.activity_run_id,
        )
        await async_activity_handle.complete(7)
        assert await activity_handle.result() == 7

        desc = await activity_handle.describe()
        assert desc.status == ActivityExecutionStatus.COMPLETED


async def test_manual_cancellation(client: Client):
    activity_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())
    event_workflow_id = str(uuid.uuid4())

    activity_handle = await client.start_activity(
        async_activity,
        args=(ActivityInput(event_workflow_id=event_workflow_id),),  # TODO: overloads
        id=activity_id,
        task_queue=task_queue,
        start_to_close_timeout=timedelta(seconds=5),
    )

    async with Worker(
        client,
        task_queue=task_queue,
        activities=[async_activity],
        workflows=[EventWorkflow],
    ):
        # Wait for activity to start
        await client.execute_workflow(
            EventWorkflow.wait,
            id=event_workflow_id,
            task_queue=task_queue,
        )
        async_activity_handle = client.get_async_activity_handle(
            activity_id=activity_id,
            run_id=activity_handle.activity_run_id,
        )

        # report_cancellation fails if activity is not in CANCELLATION_REQUESTED state
        with pytest.raises(RPCError) as err:
            await async_activity_handle.report_cancellation("Test cancellation")
        assert err.value.status == RPCStatusCode.FAILED_PRECONDITION
        assert "invalid transition from Started" in str(err.value)

        # Request cancellation to transition activity to CANCELLATION_REQUESTED state
        await activity_handle.cancel()

        # Now report_cancellation succeeds
        await async_activity_handle.report_cancellation("Test cancellation")

        with pytest.raises(ActivityFailedError) as exc_info:
            await activity_handle.result()
        assert isinstance(exc_info.value.cause, CancelledError)
        assert list(exc_info.value.cause.details) == ["Test cancellation"]

        desc = await activity_handle.describe()
        assert desc.status == ActivityExecutionStatus.CANCELED


async def test_manual_failure(client: Client):
    activity_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())
    event_workflow_id = str(uuid.uuid4())

    activity_handle = await client.start_activity(
        async_activity,
        args=(ActivityInput(event_workflow_id=event_workflow_id),),  # TODO: overloads
        id=activity_id,
        task_queue=task_queue,
        start_to_close_timeout=timedelta(seconds=5),
    )
    async with Worker(
        client,
        task_queue=task_queue,
        activities=[async_activity],
        workflows=[EventWorkflow],
    ):
        await client.execute_workflow(
            EventWorkflow.wait,
            id=event_workflow_id,
            task_queue=task_queue,
        )
        async_activity_handle = client.get_async_activity_handle(
            activity_id=activity_id,
            run_id=activity_handle.activity_run_id,
        )
        await async_activity_handle.fail(
            ApplicationError("Test failure", non_retryable=True)
        )
        with pytest.raises(ActivityFailedError) as err:
            await activity_handle.result()
        assert isinstance(err.value.cause, ApplicationError)
        assert str(err.value.cause) == "Test failure"

        desc = await activity_handle.describe()
        assert desc.status == ActivityExecutionStatus.FAILED


@activity.defn
async def activity_for_testing_heartbeat(input: ActivityInput) -> str:
    info = activity.info()
    if info.attempt == 1:
        # Signal that activity has started (only on first attempt)
        if input.wait_for_activity_start_workflow_id:
            await (
                activity.client()
                .get_workflow_handle(
                    workflow_id=input.wait_for_activity_start_workflow_id,
                )
                .signal(EventWorkflow.set)
            )
        wait_for_heartbeat_wf_handle = await activity.client().start_workflow(
            EventWorkflow.wait,
            id=input.event_workflow_id,
            task_queue=activity.info().task_queue,
        )
        # Wait for test to notify that it has sent heartbeat
        await wait_for_heartbeat_wf_handle.result()
        raise Exception("Intentional error to force retry")
    elif info.attempt == 2:
        [heartbeat_data] = info.heartbeat_details
        assert isinstance(heartbeat_data, str)
        return heartbeat_data
    else:
        raise AssertionError(f"Unexpected attempt number: {info.attempt}")


async def test_manual_heartbeat(client: Client):
    activity_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())
    event_workflow_id = str(uuid.uuid4())
    wait_for_activity_start_workflow_id = str(uuid.uuid4())

    activity_handle = await client.start_activity(
        activity_for_testing_heartbeat,
        args=(
            ActivityInput(
                event_workflow_id=event_workflow_id,
                wait_for_activity_start_workflow_id=wait_for_activity_start_workflow_id,
            ),
        ),  # TODO: overloads
        id=activity_id,
        task_queue=task_queue,
        start_to_close_timeout=timedelta(seconds=5),
    )
    wait_for_activity_start_wf_handle = await client.start_workflow(
        EventWorkflow.wait,
        id=wait_for_activity_start_workflow_id,
        task_queue=task_queue,
    )
    async with Worker(
        client,
        task_queue=task_queue,
        activities=[activity_for_testing_heartbeat],
        workflows=[EventWorkflow],
    ):
        async_activity_handle = client.get_async_activity_handle(
            activity_id=activity_id,
            run_id=activity_handle.activity_run_id,
        )
        await wait_for_activity_start_wf_handle.result()
        await async_activity_handle.heartbeat("Test heartbeat details")
        await client.get_workflow_handle(
            workflow_id=event_workflow_id,
        ).signal(EventWorkflow.set)
        assert await activity_handle.result() == "Test heartbeat details"


async def test_id_conflict_policy_fail(client: Client):
    activity_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())
    from temporalio.common import ActivityIDConflictPolicy

    await client.start_activity(
        increment,
        1,
        id=activity_id,
        task_queue=task_queue,
        schedule_to_close_timeout=timedelta(seconds=60),
        id_conflict_policy=ActivityIDConflictPolicy.FAIL,
    )

    with pytest.raises(RPCError) as err:
        await client.start_activity(
            increment,
            1,
            id=activity_id,
            task_queue=task_queue,
            schedule_to_close_timeout=timedelta(seconds=60),
            id_conflict_policy=ActivityIDConflictPolicy.FAIL,
        )
    assert err.value.status == RPCStatusCode.ALREADY_EXISTS


async def test_id_conflict_policy_use_existing(client: Client):
    activity_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())
    from temporalio.common import ActivityIDConflictPolicy

    handle1 = await client.start_activity(
        increment,
        1,
        id=activity_id,
        task_queue=task_queue,
        schedule_to_close_timeout=timedelta(seconds=60),
        id_conflict_policy=ActivityIDConflictPolicy.USE_EXISTING,
    )

    handle2 = await client.start_activity(
        increment,
        1,
        id=activity_id,
        task_queue=task_queue,
        schedule_to_close_timeout=timedelta(seconds=60),
        id_conflict_policy=ActivityIDConflictPolicy.USE_EXISTING,
    )

    assert handle1.activity_id == handle2.activity_id
    assert handle1.activity_run_id == handle2.activity_run_id


async def test_id_reuse_policy_reject_duplicate(client: Client):
    activity_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())
    from temporalio.common import ActivityIDReusePolicy

    handle = await client.start_activity(
        increment,
        1,
        id=activity_id,
        task_queue=task_queue,
        start_to_close_timeout=timedelta(seconds=5),
        id_reuse_policy=ActivityIDReusePolicy.REJECT_DUPLICATE,
    )

    async with Worker(
        client,
        task_queue=task_queue,
        activities=[increment],
    ):
        await handle.result()

    with pytest.raises(RPCError) as err:
        await client.start_activity(
            increment,
            1,
            id=activity_id,
            task_queue=task_queue,
            start_to_close_timeout=timedelta(seconds=5),
            id_reuse_policy=ActivityIDReusePolicy.REJECT_DUPLICATE,
        )
    assert err.value.status == RPCStatusCode.ALREADY_EXISTS


async def test_id_reuse_policy_allow_duplicate(client: Client):
    activity_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())
    from temporalio.common import ActivityIDReusePolicy

    handle1 = await client.start_activity(
        increment,
        1,
        id=activity_id,
        task_queue=task_queue,
        start_to_close_timeout=timedelta(seconds=5),
        id_reuse_policy=ActivityIDReusePolicy.ALLOW_DUPLICATE,
    )

    async with Worker(
        client,
        task_queue=task_queue,
        activities=[increment],
    ):
        await handle1.result()

    handle2 = await client.start_activity(
        increment,
        1,
        id=activity_id,
        task_queue=task_queue,
        start_to_close_timeout=timedelta(seconds=5),
        id_reuse_policy=ActivityIDReusePolicy.ALLOW_DUPLICATE,
    )

    assert handle1.activity_id == handle2.activity_id
    assert handle1.activity_run_id != handle2.activity_run_id


async def test_search_attributes(client: Client):
    from temporalio.common import (
        SearchAttributeKey,
        SearchAttributePair,
        TypedSearchAttributes,
    )

    activity_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())
    temporal_change_version_key = SearchAttributeKey.for_keyword_list(
        "TemporalChangeVersion"
    )

    handle = await client.start_activity(
        increment,
        1,
        id=activity_id,
        task_queue=task_queue,
        schedule_to_close_timeout=timedelta(seconds=60),
        search_attributes=TypedSearchAttributes(
            [SearchAttributePair(temporal_change_version_key, ["test-1", "test-2"])]
        ),
    )

    desc = await handle.describe()
    assert desc.search_attributes is not None
    assert desc.search_attributes["TemporalChangeVersion"] == ["test-1", "test-2"]


async def test_retry_policy(client: Client):
    from temporalio.common import RetryPolicy

    activity_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    handle = await client.start_activity(
        increment,
        1,
        id=activity_id,
        task_queue=task_queue,
        start_to_close_timeout=timedelta(seconds=5),
        retry_policy=RetryPolicy(
            initial_interval=timedelta(seconds=1),
            maximum_interval=timedelta(seconds=10),
            backoff_coefficient=2.0,
            maximum_attempts=3,
        ),
    )

    desc = await handle.describe()
    assert desc.retry_policy is not None
    assert desc.retry_policy.initial_interval == timedelta(seconds=1)
    assert desc.retry_policy.maximum_interval == timedelta(seconds=10)
    assert desc.retry_policy.backoff_coefficient == 2.0
    assert desc.retry_policy.maximum_attempts == 3


async def test_terminate(client: Client):
    activity_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())
    event_workflow_id = str(uuid.uuid4())

    activity_handle = await client.start_activity(
        async_activity,
        args=(ActivityInput(event_workflow_id=event_workflow_id),),
        id=activity_id,
        task_queue=task_queue,
        start_to_close_timeout=timedelta(seconds=5),
    )

    async with Worker(
        client,
        task_queue=task_queue,
        activities=[async_activity],
        workflows=[EventWorkflow],
    ):
        await client.execute_workflow(
            EventWorkflow.wait,
            id=event_workflow_id,
            task_queue=task_queue,
        )

        await activity_handle.terminate(reason="Test termination")

        with pytest.raises(ActivityFailedError):
            await activity_handle.result()

        desc = await activity_handle.describe()
        assert desc.status == ActivityExecutionStatus.TERMINATED


# Utilities


@workflow.defn
class EventWorkflow:
    """
    A workflow version of asyncio.Event()
    """

    def __init__(self) -> None:
        self.signal_received = asyncio.Event()

    @workflow.run
    async def wait(self) -> None:
        await self.signal_received.wait()

    @workflow.signal
    def set(self) -> None:
        self.signal_received.set()
