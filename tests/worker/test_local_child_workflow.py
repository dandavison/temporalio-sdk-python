"""Runs a child workflow in-process on the local server while a real server owns it: the local
server acquires the child from the server and syncs the child's history to it.

Requires:
- TEMPORAL_LOCAL_SERVER_MODULE: the path of a precompiled local-server module (.cwasm).
- TEMPORAL_LOCAL_EXECUTION_SERVER: the address of a server that supports local execution (branch
  sj/local-first-execution) with history.enableLocalExecution enabled.
"""

import asyncio
import os
import uuid
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.api.enums.v1 import EventType
from temporalio.client import Client, WorkflowExecutionStatus, WorkflowHandle
from temporalio.service import LocalServerUpstream, RPCError, RPCStatusCode
from temporalio.worker import Replayer, Worker
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


async def test_child_workflow_runs_locally_and_syncs_to_server():
    turn_may_finish.clear()
    address = os.environ["TEMPORAL_LOCAL_EXECUTION_SERVER"]
    server_client = await Client.connect(address)
    local_client = await Client.connect(
        "local",
        local_server_module=os.environ["TEMPORAL_LOCAL_SERVER_MODULE"],
        local_server_upstream=LocalServerUpstream(target_host=address),
    )
    agent_task_queue = f"agent-{uuid.uuid4()}"
    turn_task_queue = f"turn-{uuid.uuid4()}"
    # The only worker for the turn's task queue is connected to the local server.
    async with (
        Worker(server_client, task_queue=agent_task_queue, workflows=[Agent]),
        Worker(
            local_client,
            task_queue=turn_task_queue,
            workflows=[Turn],
            activities=[step],
        ),
    ):
        agent = await server_client.start_workflow(
            Agent.run,
            turn_task_queue,
            id=f"agent-{uuid.uuid4()}",
            task_queue=agent_task_queue,
        )
        turn = server_client.get_workflow_handle(f"{agent.id}-turn")

        # While the turn waits in its second activity, the server shows the first one completed.
        async def first_activity_synced() -> None:
            assert EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED in await event_types(
                turn
            )

        await assert_eventually(first_activity_synced)
        assert (await turn.describe()).status == WorkflowExecutionStatus.RUNNING

        turn_may_finish.set()
        assert await agent.result() == 3

    history = await turn.fetch_history()
    assert history.events[-1].event_type == (
        EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
    )
    await Replayer(workflows=[Turn]).replay_workflow(history)


async def event_types(handle: WorkflowHandle) -> list[EventType.ValueType]:
    try:
        return [event.event_type async for event in handle.fetch_history_events()]
    except RPCError as err:
        assert err.status != RPCStatusCode.NOT_FOUND, "the workflow does not exist yet"
        raise
