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
