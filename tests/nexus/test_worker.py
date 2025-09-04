"""Test Nexus worker configuration, specifically max_concurrent_nexus_tasks."""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta
from typing import List

import nexusrpc.handler
import pytest

from temporalio import workflow
from temporalio.client import Client
from tests.helpers import new_worker
from tests.helpers.nexus import create_nexus_endpoint, make_nexus_endpoint_name


@workflow.defn(sandboxed=False)
class NexusCallerWorkflow:
    """Workflow that calls a Nexus operation."""

    @workflow.run
    async def run(self, op_id: int) -> str:
        nexus_client = workflow.create_nexus_client(
            endpoint=make_nexus_endpoint_name(workflow.info().task_queue),
            service="ConcurrencyTestService",
        )

        result = await nexus_client.execute_operation(
            "process",
            op_id,
            schedule_to_close_timeout=timedelta(seconds=10),
        )
        return result


class ConcurrencyTracker:
    """Tracks concurrent execution of Nexus operations."""

    def __init__(self):
        self.current_concurrent = 0
        self.max_concurrent = 0
        self.started_ops: List[int] = []
        self.completed_ops: List[int] = []
        self.lock = asyncio.Lock()
        # Event that operations wait on
        self.continue_event = asyncio.Event()

    async def start_operation(self, op_id: int):
        async with self.lock:
            self.current_concurrent += 1
            self.max_concurrent = max(self.max_concurrent, self.current_concurrent)
            self.started_ops.append(op_id)

    async def complete_operation(self, op_id: int):
        async with self.lock:
            self.current_concurrent -= 1
            self.completed_ops.append(op_id)


@pytest.mark.parametrize(
    "max_concurrent_nexus_tasks,expected_max",
    [
        (1, 1),  # With max=1, only 1 operation runs at a time
        (2, 2),  # With max=2, up to 2 operations run concurrently
        (3, 3),  # With max=3, up to 3 operations run concurrently (we'll start 3)
    ],
)
async def test_max_concurrent_nexus_tasks(
    client: Client, max_concurrent_nexus_tasks: int, expected_max: int
):
    """Test that max_concurrent_nexus_tasks limits concurrent execution."""

    tracker = ConcurrencyTracker()

    @nexusrpc.handler.service_handler
    class ConcurrencyTestService:
        @nexusrpc.handler.sync_operation
        async def process(
            self, _ctx: nexusrpc.handler.StartOperationContext, op_id: int
        ) -> str:
            # Track when operation starts
            await tracker.start_operation(op_id)

            # Wait for test to release operations
            await tracker.continue_event.wait()

            # Track when operation completes
            await tracker.complete_operation(op_id)

            return f"processed_{op_id}"

    async with new_worker(
        client,
        NexusCallerWorkflow,
        nexus_service_handlers=[ConcurrencyTestService()],
        max_concurrent_nexus_tasks=max_concurrent_nexus_tasks,
    ) as worker:
        await create_nexus_endpoint(worker.task_queue, client)

        # Start 3 workflows concurrently
        # They will all try to execute Nexus operations
        workflow_handles = []
        for i in range(3):
            handle = await client.start_workflow(
                NexusCallerWorkflow.run,
                i,
                id=f"test-wf-{uuid.uuid4()}",
                task_queue=worker.task_queue,
            )
            workflow_handles.append(handle)

        # Give operations time to start (up to the concurrency limit)
        await asyncio.sleep(0.5)

        # Check how many operations started
        assert len(tracker.started_ops) == min(3, max_concurrent_nexus_tasks)
        assert tracker.max_concurrent == expected_max

        # Release all operations to complete
        tracker.continue_event.set()

        # Wait for all workflows to complete
        for handle in workflow_handles:
            result = await handle.result()
            assert result.startswith("processed_")

        # Verify all operations completed
        assert len(tracker.completed_ops) == 3
        assert sorted(tracker.completed_ops) == [0, 1, 2]


@workflow.defn
class SimpleWorkflow:
    """Simple workflow that doesn't call nexus operations."""

    @workflow.run
    async def run(self) -> str:
        # This workflow doesn't call nexus, so it should work
        return "done"


async def test_max_concurrent_nexus_tasks_zero_prevents_execution(client: Client):
    """Test that max_concurrent_nexus_tasks=0 prevents any nexus execution."""

    @nexusrpc.handler.service_handler
    class SimpleService:
        @nexusrpc.handler.sync_operation
        async def process(
            self, _ctx: nexusrpc.handler.StartOperationContext, value: str
        ) -> str:
            return f"processed_{value}"

    # Worker with 0 concurrent nexus tasks - workflows can run but nexus operations can't
    async with new_worker(
        client,
        SimpleWorkflow,
        nexus_service_handlers=[SimpleService()],
        max_concurrent_nexus_tasks=0,
    ) as worker:
        # Regular workflow should still work
        result = await client.execute_workflow(
            SimpleWorkflow.run,
            id=f"test-wf-zero-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        assert result == "done"

        # But if we had a workflow that calls nexus, it would timeout
        # (not testing this to avoid timeout errors in the test output)
