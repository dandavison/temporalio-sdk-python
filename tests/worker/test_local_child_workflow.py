"""Runs a child workflow on a worker with local execution: the worker's in-process local server
acquires the child from the server, runs it, and syncs the child's history to the server.

Requires:
- the temporalio-localserver package (``temporalio[local]``), with its module built;
- TEMPORAL_LOCAL_EXECUTION_CLI: a Temporal CLI built against a server that supports local
  execution (temporalio/temporal branch sj/local-first-execution).
"""

import asyncio
import os
import subprocess
import sys
import uuid
from collections.abc import AsyncIterator
from datetime import timedelta

import pytest

from temporalio import activity, workflow
from temporalio.api.enums.v1 import EventType, TaskQueueType
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import DescribeTaskQueueRequest
from temporalio.client import Client, WorkflowExecutionStatus, WorkflowHandle
from temporalio.service import RPCError, RPCStatusCode
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import LocalExecution, Replayer, Worker
from tests.helpers import assert_eventually

turn_may_finish = asyncio.Event()


@activity.defn
async def step(n: int) -> int:
    if n == 1:
        await turn_may_finish.wait()
    return n


@workflow.defn
class Turn:
    @workflow.run
    async def run(self, steps: int) -> int:
        total = 0
        for n in range(steps):
            total += await workflow.execute_activity(
                step, n, start_to_close_timeout=timedelta(seconds=30)
            )
        return total


@workflow.defn
class Agent:
    @workflow.run
    async def run(self, turn_task_queue: str) -> int:
        return await workflow.execute_child_workflow(
            Turn.run,
            3,
            id=f"{workflow.info().workflow_id}-turn",
            task_queue=turn_task_queue,
        )


@pytest.fixture
async def server() -> AsyncIterator[WorkflowEnvironment]:
    env = await WorkflowEnvironment.start_local(
        dev_server_existing_path=os.environ["TEMPORAL_LOCAL_EXECUTION_CLI"],
        dev_server_extra_args=[
            "--dynamic-config-value",
            "history.enableLocalExecution=true",
        ],
    )
    yield env
    await env.shutdown()


async def test_child_workflow_runs_locally_and_syncs_to_server(
    server: WorkflowEnvironment,
):
    turn_may_finish.clear()
    agent_task_queue = f"agent-{uuid.uuid4()}"
    turn_task_queue = f"turn-{uuid.uuid4()}"
    async with (
        Worker(server.client, task_queue=agent_task_queue, workflows=[Agent]),
        Worker(
            server.client,
            task_queue=turn_task_queue,
            workflows=[Turn],
            activities=[step],
            local_execution=LocalExecution(),
        ),
    ):
        agent = await start_agent(server.client, agent_task_queue, turn_task_queue)
        turn = server.client.get_workflow_handle(f"{agent.id}-turn")

        # While the turn waits in its second activity, the server shows the first one completed.
        await assert_eventually(lambda: assert_synced(turn, completed_activities=1))
        assert (await turn.describe()).status == WorkflowExecutionStatus.RUNNING

        # Only the local server polls the turn's task queue, and only for workflow tasks: the
        # server never dispatches the turn's activities.
        assert await poller_identities(
            server.client, turn_task_queue, TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW
        ) == ["local-server"]
        assert (
            await poller_identities(
                server.client, turn_task_queue, TaskQueueType.TASK_QUEUE_TYPE_ACTIVITY
            )
            == []
        )

        turn_may_finish.set()
        assert await agent.result() == 3

    history = await turn.fetch_history()
    assert history.events[-1].event_type == (
        EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
    )
    await Replayer(workflows=[Turn]).replay_workflow(history)


async def test_child_workflow_continues_on_the_server_when_the_local_worker_dies(
    server: WorkflowEnvironment,
):
    turn_may_finish.clear()
    agent_task_queue = f"agent-{uuid.uuid4()}"
    turn_task_queue = f"turn-{uuid.uuid4()}"
    local_worker = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import asyncio, sys; "
            "from tests.worker.test_local_child_workflow import run_local_worker; "
            "asyncio.run(run_local_worker(sys.argv[1], sys.argv[2]))",
            target_host(server),
            turn_task_queue,
        ]
    )
    try:
        async with Worker(
            server.client, task_queue=agent_task_queue, workflows=[Agent]
        ):
            agent = await start_agent(server.client, agent_task_queue, turn_task_queue)
            turn = server.client.get_workflow_handle(f"{agent.id}-turn")

            # The local worker dies while the turn waits in its second activity, whose scheduling
            # the server has.
            await assert_eventually(
                lambda: assert_synced(
                    turn, completed_activities=1, scheduled_activities=2
                )
            )
            local_worker.kill()

            # Once the local worker's ownership expires, a worker connected to the server
            # continues the turn from its synced history.
            turn_may_finish.set()
            async with Worker(
                server.client,
                task_queue=turn_task_queue,
                workflows=[Turn],
                activities=[step],
            ):
                assert await asyncio.wait_for(agent.result(), timeout=30) == 3
    finally:
        local_worker.kill()
        local_worker.wait()

    history = await turn.fetch_history()
    types = [event.event_type for event in history.events]
    assert types.count(EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED) == 3
    await Replayer(workflows=[Turn]).replay_workflow(history)


async def run_local_worker(target_host: str, task_queue: str) -> None:
    """Runs a worker with local execution for the turn until the process is killed."""
    client = await Client.connect(target_host)
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[Turn],
        activities=[step],
        local_execution=LocalExecution(),
    ):
        await asyncio.Event().wait()


async def poller_identities(
    client: Client, task_queue: str, task_queue_type: TaskQueueType.ValueType
) -> list[str]:
    """The identities of the pollers of a task queue, with any process-specific suffix removed."""
    response = await client.workflow_service.describe_task_queue(
        DescribeTaskQueueRequest(
            namespace=client.namespace,
            task_queue=TaskQueue(name=task_queue),
            task_queue_type=task_queue_type,
        )
    )
    return sorted({p.identity.split("@")[0] for p in response.pollers})


async def start_agent(
    client: Client, agent_task_queue: str, turn_task_queue: str
) -> WorkflowHandle:
    return await client.start_workflow(
        Agent.run,
        turn_task_queue,
        id=f"agent-{uuid.uuid4()}",
        task_queue=agent_task_queue,
    )


def target_host(server: WorkflowEnvironment) -> str:
    return server.client.service_client.config.target_host


async def assert_synced(
    turn: WorkflowHandle, completed_activities: int, scheduled_activities: int = 0
) -> None:
    types = await event_types(turn)
    assert (
        types.count(EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED)
        >= completed_activities
    )
    assert (
        types.count(EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED)
        >= scheduled_activities
    )


async def event_types(handle: WorkflowHandle) -> list[EventType.ValueType]:
    try:
        return [event.event_type async for event in handle.fetch_history_events()]
    except RPCError as err:
        assert err.status != RPCStatusCode.NOT_FOUND, "the workflow does not exist yet"
        raise
