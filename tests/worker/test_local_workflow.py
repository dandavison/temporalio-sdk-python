"""Runs workflows against the in-process local server and compares their histories with those of
the same workflows run on a real server.

Requires TEMPORAL_LOCAL_SERVER_MODULE: the path of a precompiled local-server module (.cwasm).
"""

import os
import uuid
from datetime import timedelta
from typing import Any

from google.protobuf.json_format import MessageToDict

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from tests.helpers import new_worker

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


async def test_local_history_matches_server_history(client: Client):
    module = os.environ["TEMPORAL_LOCAL_SERVER_MODULE"]
    local_client = await Client.connect("local", local_server_module=module)
    workflow_id = f"wf-{uuid.uuid4()}"
    task_queue = f"tq-{uuid.uuid4()}"

    server_history = await run_workflow(client, workflow_id, task_queue)
    local_history = await run_workflow(local_client, workflow_id, task_queue)

    assert local_history == server_history


async def run_workflow(
    client: Client, workflow_id: str, task_queue: str
) -> list[dict[str, Any]]:
    async with new_worker(
        client,
        TimerThenRetriedActivityWorkflow,
        activities=[fail_first_attempt],
        task_queue=task_queue,
    ):
        handle = await client.start_workflow(
            TimerThenRetriedActivityWorkflow.run,
            "local",
            id=workflow_id,
            task_queue=task_queue,
        )
        assert await handle.result() == "Hello, local!"
    history = await handle.fetch_history()
    return [comparable(MessageToDict(e)) for e in history.events]


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
