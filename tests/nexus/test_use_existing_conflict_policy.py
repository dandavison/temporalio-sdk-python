from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from typing import Optional

import pytest
from nexusrpc.handler import service_handler

from temporalio import nexus, workflow
from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from tests.helpers.nexus import create_nexus_endpoint, make_nexus_endpoint_name


@dataclass
class OpInput:
    workflow_id: str
    conflict_policy: WorkflowIDConflictPolicy


@workflow.defn
class HandlerWorkflow:
    def __init__(self) -> None:
        self.result: Optional[str] = None

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: self.result is not None)
        assert self.result
        return self.result

    @workflow.signal
    def complete(self, result: str) -> None:
        self.result = result


@service_handler
class NexusService:
    @nexus.workflow_run_operation
    async def workflow_backed_operation(
        self, ctx: nexus.WorkflowRunOperationContext, input: OpInput
    ) -> nexus.WorkflowHandle[str]:
        return await ctx.start_workflow(
            HandlerWorkflow.run,
            id=input.workflow_id,
            id_conflict_policy=input.conflict_policy,
        )


@workflow.defn
class CallerWorkflow:
    def __init__(self) -> None:
        self._nexus_operations_have_started = asyncio.Event()

    @workflow.run
    async def run(self, workflow_id: str, task_queue: str) -> tuple[str, str]:
        nexus_client = workflow.create_nexus_client(
            service=NexusService, endpoint=make_nexus_endpoint_name(task_queue)
        )

        op_input = OpInput(
            workflow_id=workflow_id,
            conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )

        handle_1 = await nexus_client.start_operation(
            NexusService.workflow_backed_operation, op_input
        )
        handle_2 = await nexus_client.start_operation(
            NexusService.workflow_backed_operation, op_input
        )
        self._nexus_operations_have_started.set()
        return await handle_1, await handle_2

    @workflow.update
    async def nexus_operations_have_started(self) -> None:
        await self._nexus_operations_have_started.wait()


async def test_two_operation_invocations_can_connect_to_same_handler_workflow(
    client: Client, env: WorkflowEnvironment
):
    if env.supports_time_skipping:
        pytest.skip("Nexus tests don't work with time-skipping server")

    task_queue = str(uuid.uuid4())
    workflow_id = str(uuid.uuid4())

    async with Worker(
        client,
        nexus_service_handlers=[NexusService()],
        workflows=[CallerWorkflow, HandlerWorkflow],
        task_queue=task_queue,
    ):
        await create_nexus_endpoint(task_queue, client)
        caller_handle = await client.start_workflow(
            CallerWorkflow.run,
            args=[workflow_id, task_queue],
            id=str(uuid.uuid4()),
            task_queue=task_queue,
        )
        await caller_handle.execute_update(CallerWorkflow.nexus_operations_have_started)
        await client.get_workflow_handle(workflow_id).signal(
            HandlerWorkflow.complete, "test-result"
        )
        assert await caller_handle.result() == ("test-result", "test-result")


# Note: A test for FAIL policy would timeout because Nexus operations
# retry indefinitely on WorkflowExecutionAlreadyStarted errors.
# This is a known limitation that will be addressed in future updates.
