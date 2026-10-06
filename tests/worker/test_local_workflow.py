"""Runs workflows against the in-process local server and compares their histories with those of
the same workflows run on a real server.

Requires TEMPORAL_LOCAL_SERVER_MODULE: the path of a precompiled local-server module (.cwasm).
"""

import asyncio
import os
import uuid
from collections.abc import Awaitable, Callable, Sequence
from datetime import timedelta
from typing import Any

import pytest
from google.protobuf.json_format import MessageToDict

from temporalio import activity, workflow
from temporalio.client import Client, WorkflowHandle
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError
from tests.helpers import assert_eventually, new_worker

# Fields that differ between two runs of the same workflow without a difference in behavior.
RUN_SPECIFIC_FIELDS = {
    "taskId",
    "identity",
    "requestId",
    "originalExecutionRunId",
    "firstExecutionRunId",
}

# Fields holding sets, whose order is arbitrary.
SET_FIELDS = {"coreUsedFlags", "langUsedFlags"}

# Fields whose server values the local server does not reproduce. historySizeBytes: the server
# counts the bytes of persisted event batches, which include task IDs; the local server counts the
# bytes of its events, which have none.
UNREPRODUCED_FIELDS = {"historySizeBytes"}


@activity.defn
async def fail_first_attempt(name: str) -> str:
    if activity.info().attempt == 1:
        raise ApplicationError("first attempt fails")
    return f"Hello, {name}!"


@activity.defn
async def time_out_first_attempt(name: str) -> str:
    if activity.info().attempt == 1:
        await asyncio.sleep(3)
    return f"Hello, {name}!"


@activity.defn
async def always_fail(name: str) -> str:
    raise ApplicationError(f"{name} fails")


@workflow.defn
class TimerThenRetriedActivityWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        await workflow.sleep(timedelta(seconds=1))
        return await workflow.execute_activity(
            fail_first_attempt,
            name,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(initial_interval=timedelta(seconds=1)),
        )


@workflow.defn
class ActivityTimeoutWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        return await workflow.execute_activity(
            time_out_first_attempt,
            name,
            start_to_close_timeout=timedelta(seconds=1),
            retry_policy=RetryPolicy(initial_interval=timedelta(seconds=1)),
        )


@workflow.defn
class RetriesExhaustedWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        try:
            return await workflow.execute_activity(
                always_fail,
                name,
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=1), maximum_attempts=2
                ),
            )
        except ActivityError as err:
            return f"caught: {err.cause}"


# Run IDs whose first workflow task has failed.
failed_workflow_task_run_ids: set[str] = set()


@workflow.defn(sandboxed=False)
class WorkflowTaskFailureWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        run_id = workflow.info().run_id
        if run_id not in failed_workflow_task_run_ids:
            failed_workflow_task_run_ids.add(run_id)
            raise RuntimeError("first workflow task fails")
        await workflow.sleep(timedelta(seconds=1))
        return f"Hello, {name}!"


@workflow.defn
class TimerWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        await workflow.sleep(timedelta(seconds=2))
        return f"Hello, {name}!"


Scenario = Callable[[Client, str, str], Awaitable[None]]


async def timer_then_retried_activity(
    client: Client, workflow_id: str, task_queue: str
) -> None:
    await run_to_completion(
        client,
        workflow_id,
        task_queue,
        TimerThenRetriedActivityWorkflow,
        [fail_first_attempt],
        "Hello, local!",
    )


async def activity_timeout(client: Client, workflow_id: str, task_queue: str) -> None:
    await run_to_completion(
        client,
        workflow_id,
        task_queue,
        ActivityTimeoutWorkflow,
        [time_out_first_attempt],
        "Hello, local!",
    )


async def retries_exhausted(client: Client, workflow_id: str, task_queue: str) -> None:
    await run_to_completion(
        client,
        workflow_id,
        task_queue,
        RetriesExhaustedWorkflow,
        [always_fail],
        "caught: local fails",
    )


async def workflow_task_failure(
    client: Client, workflow_id: str, task_queue: str
) -> None:
    await run_to_completion(
        client,
        workflow_id,
        task_queue,
        WorkflowTaskFailureWorkflow,
        [],
        "Hello, local!",
    )


async def worker_replaced(client: Client, workflow_id: str, task_queue: str) -> None:
    """The worker that ran the first workflow task shuts down while the workflow waits on a timer,
    and a new worker finishes the workflow."""
    async with new_worker(
        client,
        TimerWorkflow,
        task_queue=task_queue,
        sticky_queue_schedule_to_start_timeout=timedelta(seconds=1),
    ):
        handle = await client.start_workflow(
            TimerWorkflow.run, "local", id=workflow_id, task_queue=task_queue
        )
        await assert_eventually(lambda: assert_timer_started(handle))
    async with new_worker(client, TimerWorkflow, task_queue=task_queue):
        assert await handle.result() == "Hello, local!"


@pytest.mark.parametrize(
    "scenario",
    [
        timer_then_retried_activity,
        activity_timeout,
        retries_exhausted,
        workflow_task_failure,
        worker_replaced,
    ],
)
async def test_local_history_matches_server_history(client: Client, scenario: Scenario):
    module = os.environ["TEMPORAL_LOCAL_SERVER_MODULE"]
    local_client = await Client.connect("local", local_server_module=module)
    workflow_id = f"wf-{uuid.uuid4()}"
    task_queue = f"tq-{uuid.uuid4()}"

    await scenario(client, workflow_id, task_queue)
    await scenario(local_client, workflow_id, task_queue)

    assert await history(local_client, workflow_id) == await history(
        client, workflow_id
    )


async def run_to_completion(
    client: Client,
    workflow_id: str,
    task_queue: str,
    workflow_class: type,
    activities: Sequence[Callable],
    expected_result: str,
) -> None:
    async with new_worker(
        client, workflow_class, activities=activities, task_queue=task_queue
    ):
        result = await client.execute_workflow(
            workflow_class.run,  # type: ignore[attr-defined]
            "local",
            id=workflow_id,
            task_queue=task_queue,
        )
    assert result == expected_result


async def assert_timer_started(handle: WorkflowHandle) -> None:
    events = (await handle.fetch_history()).events
    assert any(e.HasField("timer_started_event_attributes") for e in events)


async def history(client: Client, workflow_id: str) -> list[dict[str, Any]]:
    events = (await client.get_workflow_handle(workflow_id).fetch_history()).events
    return [comparable(MessageToDict(e)) for e in events]


def comparable(value: Any) -> Any:
    """Drops run-specific and unreproduced fields, and sorts sets. The name of a sticky task queue
    is run-specific because each worker has its own."""
    if isinstance(value, dict):
        return {
            k: sorted(v) if k in SET_FIELDS else comparable(v)
            for k, v in value.items()
            if k not in RUN_SPECIFIC_FIELDS | UNREPRODUCED_FIELDS
            and not k.endswith("Time")
            and not (k == "name" and value.get("kind") == "TASK_QUEUE_KIND_STICKY")
        }
    if isinstance(value, list):
        return [comparable(v) for v in value]
    return value
