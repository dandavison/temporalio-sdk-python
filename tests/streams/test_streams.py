"""End-to-end tests of workflow-owned streams, against each stream backend.

These need redis-server on PATH, and the prototype's server with retry stamps and direct Nexus
calls enabled, as the superproject's dev-server runs it: pass ``--workflow-environment
localhost:7233``.
"""

from __future__ import annotations

import asyncio
import socket
import subprocess
import uuid
from collections.abc import AsyncIterator
from datetime import timedelta

import pytest
import pytest_asyncio
import redis.asyncio

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.streams import (
    ClientProducer,
    CloseStatus,
    Record,
    StreamBackend,
    StreamRef,
    StreamWriter,
    decode,
    workflow_append,
)
from temporalio.streams.redis import RedisStreamBackend
from temporalio.worker import Worker


@pytest_asyncio.fixture  # type: ignore[reportUntypedFunctionDecorator]
async def redis_client() -> AsyncIterator[redis.asyncio.Redis]:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = subprocess.Popen(
        ["redis-server", "--port", str(port), "--save", "", "--appendonly", "no"],
        stdout=subprocess.DEVNULL,
    )
    client = redis.asyncio.Redis(port=port)
    try:
        for _ in range(100):
            try:
                await client.ping()  # type: ignore[misc,reportGeneralTypeIssues]
                break
            except redis.ConnectionError:
                await asyncio.sleep(0.05)
        yield client
    finally:
        await client.aclose()
        server.kill()
        server.wait()


@pytest.fixture(params=["redis", "native"])
def backend(request: pytest.FixtureRequest, client: Client) -> StreamBackend:
    if request.param == "redis":
        return RedisStreamBackend(
            request.getfixturevalue("redis_client"), client.namespace
        )
    from temporalio.streams.native import NativeStreamBackend

    return NativeStreamBackend(client)


async def read_all(backend: StreamBackend, ref: StreamRef) -> list[Record]:
    """Read every data record, omitting workflow task commit markers, which only native writes."""
    records: list[Record] = []
    cursor = None
    while True:
        resp = await backend.read(ref, after=cursor)
        if not resp.records:
            return records
        records.extend(r for r in resp.records if r.workflow_task_commit is None)
        cursor = resp.cursor


async def start_owner(client: Client) -> StreamRef:
    """Start a workflow, with no worker, to own a stream."""
    handle = await client.start_workflow(
        "StreamOwner", id=f"wf-{uuid.uuid4()}", task_queue=f"tq-{uuid.uuid4()}"
    )
    return StreamRef(handle.id)


async def values(records: list[Record]) -> list[str]:
    return [await decode(r, str) for r in records if r.payload is not None]


async def test_client_append_and_read(client: Client, backend: StreamBackend):
    ref = await start_owner(client)
    writer = StreamWriter(backend, ref, ClientProducer("restaurant", 1))
    await writer.append("accepted", topic="stage")
    cursors = await writer.append("ready", "picked up", topic="stage")
    assert len(cursors) == 2

    records = await read_all(backend, ref)
    assert await values(records) == ["accepted", "ready", "picked up"]
    assert all(
        r.payload and r.payload.metadata["encoding"] == b"json/plain" for r in records
    )
    assert [r.producer.seq for r in records] == [0, 1, 1]
    resumed = await backend.read(ref, after=records[0].cursor)
    assert await values(list(resumed.records)) == ["ready", "picked up"]


@workflow.defn
class WaitForSignalWorkflow:
    def __init__(self) -> None:
        self.done = False

    @workflow.run
    async def run(self) -> None:
        workflow_append("first")
        await workflow.wait_condition(lambda: self.done)
        workflow_append("second")

    @workflow.signal
    def finish(self) -> None:
        self.done = True


async def test_workflow_append_is_visible_while_workflow_runs_and_closes_on_completion(
    client: Client, backend: StreamBackend
):
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[WaitForSignalWorkflow],
        stream_backend=backend,
    ):
        handle = await client.start_workflow(
            WaitForSignalWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )
        ref = StreamRef(handle.id)
        first = await backend.read(ref, wait=timedelta(seconds=10))
        assert await values(list(first.records)) == ["first"]
        assert first.close is None

        await handle.signal(WaitForSignalWorkflow.finish)
        await handle.result()

    records = await read_all(backend, ref)
    assert await values(records) == ["first", "second"]
    assert all(
        r.payload and r.payload.metadata["encoding"] == b"json/plain" for r in records
    )
    producer = records[0].producer
    assert producer.producer == records[1].producer.producer
    assert records[1].producer.workflow_task > producer.workflow_task
    end = await backend.read(ref, after=records[-1].cursor)
    assert end.close is not None and end.close.status == CloseStatus.COMPLETED


async def test_workflow_append_is_not_repeated_on_replay(
    client: Client, backend: StreamBackend
):
    task_queue = f"tq-{uuid.uuid4()}"
    # With no cache, every workflow task replays the workflow from the start.
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[WaitForSignalWorkflow],
        stream_backend=backend,
        max_cached_workflows=0,
    ):
        handle = await client.start_workflow(
            WaitForSignalWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )
        await backend.read(StreamRef(handle.id), wait=timedelta(seconds=10))
        await handle.signal(WaitForSignalWorkflow.finish)
        await handle.result()

    assert await values(await read_all(backend, StreamRef(handle.id))) == [
        "first",
        "second",
    ]


failed_task_attempts = 0


@activity.defn
async def noop() -> None:
    pass


@workflow.defn(sandboxed=False)
class FailFirstTaskWorkflow:
    @workflow.run
    async def run(self) -> None:
        global failed_task_attempts
        workflow_append("before local activity")
        # The local activity's result arrives in a second activation of the same workflow task, so
        # the task's first activation has already appended when the task fails.
        await workflow.execute_local_activity(
            noop, start_to_close_timeout=timedelta(seconds=10)
        )
        if failed_task_attempts == 0:
            failed_task_attempts += 1
            raise RuntimeError("fail the workflow task")
        workflow_append("after local activity")


async def test_failed_workflow_task_records_carry_lower_epoch(
    client: Client, backend: StreamBackend
):
    global failed_task_attempts
    failed_task_attempts = 0
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[FailFirstTaskWorkflow],
        activities=[noop],
        stream_backend=backend,
    ):
        handle = await client.start_workflow(
            FailFirstTaskWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )
        await handle.result()

    records = await read_all(backend, StreamRef(handle.id))
    assert await values(records) == [
        "before local activity",
        "before local activity",
        "after local activity",
    ]
    failed, rerun = records[0].producer, records[1].producer
    assert failed.producer == rerun.producer
    assert failed.workflow_task == rerun.workflow_task
    assert rerun.epoch > failed.epoch


class StreamActivities:
    def __init__(self, backend: StreamBackend) -> None:
        self._backend = backend

    @activity.defn
    async def append_then_fail_first_attempt(self, ref: StreamRef) -> None:
        info = activity.info()
        await StreamWriter.for_activity(self._backend, ref).append(
            f"attempt {info.attempt}"
        )
        if info.attempt == 1:
            raise RuntimeError("fail the first attempt")


@workflow.defn
class RunActivityWorkflow:
    @workflow.run
    async def run(self) -> None:
        await workflow.execute_activity_method(
            StreamActivities.append_then_fail_first_attempt,
            StreamRef(workflow.info().workflow_id),
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=10), maximum_attempts=2
            ),
        )


async def test_retried_activity_attempt_has_higher_epoch(
    client: Client, backend: StreamBackend
):
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[RunActivityWorkflow],
        activities=[StreamActivities(backend).append_then_fail_first_attempt],
    ):
        handle = await client.start_workflow(
            RunActivityWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )
        await handle.result()

    records = await read_all(backend, StreamRef(handle.id))
    assert await values(records) == ["attempt 1", "attempt 2"]
    assert records[0].producer.producer == records[1].producer.producer
    assert records[1].producer.epoch > records[0].producer.epoch
