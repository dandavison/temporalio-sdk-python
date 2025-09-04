"""Test Nexus worker configuration, specifically max_concurrent_nexus_tasks."""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import nexusrpc.handler
import pytest

from temporalio import workflow
from temporalio.client import Client
from tests.helpers import new_worker
from tests.helpers.nexus import create_nexus_endpoint, make_nexus_endpoint_name


@nexusrpc.handler.service_handler
class EchoService:
    @nexusrpc.handler.sync_operation
    async def echo(
        self, _ctx: nexusrpc.handler.StartOperationContext, input: str
    ) -> str:
        return input


@workflow.defn
class NexusCallerWorkflow:
    """Workflow that calls a Nexus operation."""

    @workflow.run
    async def run(self, input: str) -> str:
        nexus_client = workflow.create_nexus_client(
            endpoint=make_nexus_endpoint_name(workflow.info().task_queue),
            service=EchoService,
        )

        return await nexus_client.execute_operation(
            EchoService.echo,
            input,
            schedule_to_close_timeout=timedelta(seconds=10),
        )


async def test_max_concurrent_nexus_tasks_zero_fails(client: Client):
    """Test that max_concurrent_nexus_tasks=0 prevents nexus tasks from executing."""

    async with new_worker(
        client,
        NexusCallerWorkflow,
        nexus_service_handlers=[EchoService()],
        max_concurrent_nexus_tasks=0,
    ) as worker:
        await create_nexus_endpoint(worker.task_queue, client)

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(
                client.execute_workflow(
                    NexusCallerWorkflow.run,
                    "input",
                    id=str(uuid.uuid4()),
                    task_queue=worker.task_queue,
                ),
                timeout=2.0,
            )


async def test_max_concurrent_nexus_tasks_one_succeeds(client: Client):
    async with new_worker(
        client,
        NexusCallerWorkflow,
        nexus_service_handlers=[EchoService()],
        max_concurrent_nexus_tasks=1,
    ) as worker:
        await create_nexus_endpoint(worker.task_queue, client)

        await client.execute_workflow(
            NexusCallerWorkflow.run,
            "input",
            id=str(uuid.uuid4()),
            task_queue=worker.task_queue,
        )
