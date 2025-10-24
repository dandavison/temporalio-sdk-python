import uuid
from dataclasses import dataclass
from datetime import timedelta

import pytest

from temporalio import activity
from temporalio.client import ActivityFailedError, Client
from temporalio.common import ActivityExecutionStatus
from temporalio.exceptions import ApplicationError, CancelledError
from temporalio.worker import Worker


@activity.defn
async def increment(input: int) -> int:
    return input + 1


async def test_describe(client: Client):
    activity_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    activity_handle = await client.start_activity(
        increment,
        1,
        id=activity_id,
        task_queue=task_queue,
        start_to_close_timeout=timedelta(seconds=5),
    )
    desc = await activity_handle.describe()
    assert desc.activity_id == activity_id
    # TODO: server not returning run ID yet
    # assert desc.run_id == activity_handle.run_id
    assert desc.activity_type == "increment"
    assert desc.task_queue == task_queue
    assert desc.status == ActivityExecutionStatus.RUNNING


async def test_get_result(client: Client):
    activity_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    activity_handle = await client.start_activity(
        increment,
        1,
        id=activity_id,
        task_queue=task_queue,
        start_to_close_timeout=timedelta(seconds=5),
    )
    result_via_execute_activity = client.execute_activity(
        increment,
        1,
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


@dataclass
class ActivityInput:
    pass


@activity.defn
async def async_activity(input: ActivityInput) -> int:
    activity.raise_complete_async()


async def test_manual_completion(client: Client):
    activity_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    activity_handle = await client.start_activity(
        async_activity,
        ActivityInput(),
        id=activity_id,
        task_queue=task_queue,
        start_to_close_timeout=timedelta(seconds=5),
    )

    async with Worker(
        client,
        task_queue=task_queue,
        activities=[async_activity],
    ):
        # Complete activity manually
        async_activity_handle = client.get_async_activity_handle(
            activity_id=activity_id,
            run_id=activity_handle.run_id,
        )
        await async_activity_handle.complete(7)
        assert await activity_handle.result() == 7

        desc = await activity_handle.describe()
        assert desc.status == ActivityExecutionStatus.COMPLETED


async def test_manual_cancellation(client: Client):
    activity_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    activity_handle = await client.start_activity(
        async_activity,
        ActivityInput(),
        id=activity_id,
        task_queue=task_queue,
        start_to_close_timeout=timedelta(seconds=5),
    )

    async with Worker(
        client,
        task_queue=task_queue,
        activities=[async_activity],
    ):
        async_activity_handle = client.get_async_activity_handle(
            activity_id=activity_id,
            run_id=activity_handle.run_id,
        )
        await async_activity_handle.report_cancellation("Test cancellation")
        with pytest.raises(ActivityFailedError) as err:
            await activity_handle.result()
        assert isinstance(err.value.cause, CancelledError)
        assert list(err.value.cause.details) == ["Test cancellation"]

        desc = await activity_handle.describe()
        assert desc.status == ActivityExecutionStatus.CANCELED


async def test_manual_failure(client: Client):
    activity_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    activity_handle = await client.start_activity(
        async_activity,
        ActivityInput(),
        id=activity_id,
        task_queue=task_queue,
        start_to_close_timeout=timedelta(seconds=5),
    )
    async with Worker(
        client,
        task_queue=task_queue,
        activities=[async_activity],
    ):
        async_activity_handle = client.get_async_activity_handle(
            activity_id=activity_id,
            run_id=activity_handle.run_id,
        )
        await async_activity_handle.fail(ApplicationError("Test failure"))
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
        raise Exception("Intentional error to force retry")
    elif info.attempt == 2:
        [heartbeat_data] = info.heartbeat_details
        assert isinstance(heartbeat_data, str)
        return heartbeat_data
    else:
        raise AssertionError(f"Unexpected attempt number: {info.attempt}")


@pytest.mark.skip(reason="Manual heartbeat not supported in server yet")
async def test_manual_heartbeat(client: Client):
    activity_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    activity_handle = await client.start_activity(
        activity_for_testing_heartbeat,
        ActivityInput(),
        id=activity_id,
        task_queue=task_queue,
        start_to_close_timeout=timedelta(seconds=5),
    )
    async with Worker(
        client,
        task_queue=task_queue,
        activities=[activity_for_testing_heartbeat],
    ):
        async_activity_handle = client.get_async_activity_handle(
            activity_id=activity_id,
            run_id=activity_handle.run_id,
        )
        await async_activity_handle.heartbeat("Test heartbeat details")
        assert await activity_handle.result() == "Test heartbeat details"
