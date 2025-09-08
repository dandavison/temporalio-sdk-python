from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta
from typing import Any, Sequence

import nexusrpc.handler
import pytest

from temporalio import workflow
from temporalio.testing import WorkflowEnvironment
from tests.helpers import new_worker
from tests.helpers.nexus import create_nexus_endpoint, make_nexus_endpoint_name


@workflow.defn
class NexusCallerWorkflow:
    """Workflow that calls a Nexus operation."""

    @workflow.run
    async def run(self, n: int) -> None:
        nexus_client = workflow.create_nexus_client(
            endpoint=make_nexus_endpoint_name(workflow.info().task_queue),
            service="MaxConcurrentTestService",
        )

        coros: list[Any] = [
            nexus_client.execute_operation(
                "op",
                i,
                schedule_to_close_timeout=timedelta(seconds=60),
            )
            for i in range(n)
        ]
        await asyncio.gather(*coros)


@pytest.mark.parametrize(
    ["num_nexus_operations", "max_concurrent_nexus_tasks", "expect_timeout"],
    [
        (1, 1, False),
        (1, 3, False),
        (3, 3, False),
        (4, 3, True),
    ],
)
async def test_max_concurrent_nexus_tasks(
    env: WorkflowEnvironment,
    max_concurrent_nexus_tasks: int,
    num_nexus_operations: int,
    expect_timeout: bool,
):
    if env.supports_time_skipping:
        pytest.skip("Nexus tests don't work with Javas test server")

    class Barrier:
        def __init__(self, size: int) -> None:
            self.size = size
            self.event = asyncio.Event()

        @property
        def waiters(self) -> Sequence[Any]:
            return getattr(self.event, "_waiters")

        async def wait(self) -> None:
            if len(self.waiters) >= self.size - 1:
                self.event.set()
            else:
                await self.event.wait()

    barrier = Barrier(num_nexus_operations)

    @nexusrpc.handler.service_handler
    class MaxConcurrentTestService:
        @nexusrpc.handler.sync_operation
        async def op(
            self, _ctx: nexusrpc.handler.StartOperationContext, id: int
        ) -> None:
            await barrier.wait()

    async with new_worker(
        env.client,
        NexusCallerWorkflow,
        nexus_service_handlers=[MaxConcurrentTestService()],
        max_concurrent_nexus_tasks=max_concurrent_nexus_tasks,
    ) as worker:
        await create_nexus_endpoint(worker.task_queue, env.client)

        execute_operations_concurrently = env.client.execute_workflow(
            NexusCallerWorkflow.run,
            num_nexus_operations,
            id=str(uuid.uuid4()),
            task_queue=worker.task_queue,
        )
        if expect_timeout:
            try:
                await asyncio.wait_for(execute_operations_concurrently, timeout=10)
            except TimeoutError:
                pass
            else:
                pytest.fail(
                    f"Expected timeout: "
                    f"max_concurrent_nexus_tasks={max_concurrent_nexus_tasks}, "
                    f"num_nexus_operations={num_nexus_operations}"
                )
        else:
            await execute_operations_concurrently
