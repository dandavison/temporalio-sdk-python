"""Test Nexus worker configuration, specifically max_concurrent_nexus_tasks."""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import nexusrpc.handler
import pytest

import temporalio.exceptions
from temporalio import workflow
from temporalio.client import Client
from tests.helpers import new_worker
from tests.helpers.nexus import create_nexus_endpoint, make_nexus_endpoint_name


@workflow.defn(sandboxed=False)
class NexusCallerWorkflow:
    """Workflow that calls a Nexus operation."""

    @workflow.run
    async def run(self, id: int) -> None:
        nexus_client = workflow.create_nexus_client(
            endpoint=make_nexus_endpoint_name(workflow.info().task_queue),
            service="MaxConcurrentTestService",
        )

        try:
            return await nexus_client.execute_operation(
                "op",
                id,
                schedule_to_close_timeout=timedelta(milliseconds=500),
            )
        except Exception as err:
            assert isinstance(err.__cause__, temporalio.exceptions.TimeoutError)


@pytest.mark.parametrize("max_concurrent_nexus_tasks", [1])
async def test_max_concurrent_nexus_tasks(
    client: Client, max_concurrent_nexus_tasks: int
):
    ids = []
    event = asyncio.Event()

    @nexusrpc.handler.service_handler
    class MaxConcurrentTestService:
        @nexusrpc.handler.sync_operation
        async def op(
            self, _ctx: nexusrpc.handler.StartOperationContext, id: int
        ) -> None:
            ids.append(id)
            await event.wait()

    async with new_worker(
        client,
        NexusCallerWorkflow,
        nexus_service_handlers=[MaxConcurrentTestService()],
        max_concurrent_nexus_tasks=max_concurrent_nexus_tasks,
    ) as worker:
        await create_nexus_endpoint(worker.task_queue, client)

        for i in range(10):
            await client.execute_workflow(
                NexusCallerWorkflow.run,
                i,
                id=str(uuid.uuid4()),
                task_queue=worker.task_queue,
            )
        event.set()
        print(ids)
