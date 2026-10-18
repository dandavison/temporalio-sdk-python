"""End-to-end tests of streams owned by workflows and Standalone Activities, against each stream
backend. Each test says which guarantee it checks and whether it holds on every backend.

These need redis-server on PATH, and the prototype's server with retry stamps and direct Nexus
calls enabled, as the superproject's dev-server runs it: pass ``--workflow-environment
localhost:7233``.

The three tests of a run chain that continues (a failed run with a retry policy, and a cron run that
completes or fails) fail on native, because the native stream is not moved to the next run of the
chain.
"""

from __future__ import annotations

import asyncio
import socket
import subprocess
import time
import uuid
from collections.abc import Iterator
from datetime import timedelta

import pytest
import pytest_asyncio

from temporalio import activity, workflow
from temporalio.api.enums.v1 import WorkflowTaskFailedCause
from temporalio.api.history.v1 import WorkflowTaskFailedEventAttributes
from temporalio.client import Client, WorkflowHandle
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.streams import (
    ClientProducer,
    CloseStatus,
    Record,
    StreamBackend,
    StreamClosedError,
    StreamRef,
    StreamWriter,
    WorkflowAppendMode,
    decode,
    workflow_append,
)
from temporalio.streams.native import NativeStreamBackend
from temporalio.streams.redis import RedisStreamBackend
from temporalio.worker import Worker


@pytest.fixture
def redis_url() -> Iterator[str]:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = subprocess.Popen(
        ["redis-server", "--port", str(port), "--save", "", "--appendonly", "no"],
        stdout=subprocess.DEVNULL,
    )
    try:
        for _ in range(100):
            try:
                socket.create_connection(("127.0.0.1", port)).close()
                break
            except OSError:
                time.sleep(0.05)
        yield f"redis://127.0.0.1:{port}"
    finally:
        server.kill()
        server.wait()


@pytest_asyncio.fixture(params=["redis", "native"])  # type: ignore[reportUntypedFunctionDecorator]
async def backend(request: pytest.FixtureRequest, client: Client) -> StreamBackend:
    if request.param == "redis":
        return await RedisStreamBackend.connect(
            request.getfixturevalue("redis_url"), client.namespace
        )
    return await NativeStreamBackend.connect(client)


async def read_all(backend: StreamBackend, ref: StreamRef) -> list[Record]:
    """Read every data record, omitting markers, which only native writes."""
    records: list[Record] = []
    cursor = None
    while True:
        resp = await backend.read(ref, after=cursor)
        if not resp.records:
            return records
        records.extend(r for r in resp.records if not r.is_marker)
        cursor = resp.cursor


async def start_owner(client: Client) -> StreamRef:
    """Start a workflow, with no worker, to own a stream."""
    handle = await client.start_workflow(
        "StreamOwner", id=f"wf-{uuid.uuid4()}", task_queue=f"tq-{uuid.uuid4()}"
    )
    return StreamRef.for_handle(handle)


async def values(records: list[Record]) -> list[str]:
    return [await decode(r, str) for r in records if r.payload is not None]


async def test_client_append_and_read(client: Client, backend: StreamBackend):
    """Every backend: an append stores its values in order with one seq, a record's payload is the
    value's payload, and a read after a record's cursor resumes after that record.
    """
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
    """Every backend: a workflow's records are visible while it runs, a later workflow task's
    records carry a higher workflow task, and the stream closes as completed when the run completes:
    on native the server closes it, on Redis the worker.
    """
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
        ref = StreamRef.for_handle(handle)
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
    """Every backend: replay does not append a workflow's records again."""
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
        await backend.read(StreamRef.for_handle(handle), wait=timedelta(seconds=10))
        await handle.signal(WaitForSignalWorkflow.finish)
        await handle.result()

    assert await values(await read_all(backend, StreamRef.for_handle(handle))) == [
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
    """Every backend: a failed workflow task attempt may deliver records, and the rerun's records
    carry the same producer and workflow task with a higher epoch, so a reader can discard the
    failed attempt's.
    """
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

    records = await read_all(backend, StreamRef.for_handle(handle))
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
            StreamRef.for_workflow(),
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=10), maximum_attempts=2
            ),
        )


async def test_retried_activity_attempt_has_higher_epoch(
    client: Client, backend: StreamBackend
):
    """Every backend: a retried activity attempt appends as the same producer with a higher epoch.
    Holds only when the server enables ``system.enableActivityRetryStampIncrement``.
    """
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

    records = await read_all(backend, StreamRef.for_handle(handle))
    assert await values(records) == ["attempt 1", "attempt 2"]
    assert records[0].producer.producer == records[1].producer.producer
    assert records[1].producer.epoch > records[0].producer.epoch


async def read_all_with_markers(backend: StreamBackend, ref: StreamRef) -> list[Record]:
    records: list[Record] = []
    cursor = None
    while True:
        resp = await backend.read(ref, after=cursor)
        if not resp.records:
            return records
        records.extend(resp.records)
        cursor = resp.cursor


def commit_attempt(records: list[Record], record: Record) -> int | None:
    """The attempt that the commit record of ``record``'s workflow task names, or None."""
    for r in records:
        c = r.workflow_task_commit
        if (
            c
            and c.workflow_run_id == record.producer.workflow_run_id
            and c.workflow_task == record.producer.workflow_task
        ):
            return c.attempt
    return None


class LocalActivities:
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

    @activity.defn
    async def sleep_then_append(self, ref: StreamRef) -> None:
        await asyncio.sleep(3)
        await StreamWriter.for_activity(self._backend, ref).append("late")

    @activity.defn
    async def append(self, ref: StreamRef) -> None:
        await StreamWriter.for_activity(self._backend, ref).append("appended")


@workflow.defn
class LocalActivityRetryWorkflow:
    @workflow.run
    async def run(self) -> None:
        await workflow.execute_local_activity_method(
            LocalActivities.append_then_fail_first_attempt,
            StreamRef.for_workflow(),
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=10)),
        )


async def test_local_activity_retry_has_higher_epoch(
    client: Client, backend: StreamBackend
):
    """Every backend: a retried local activity attempt appends as the same producer, in the same
    workflow task, with a higher epoch.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[LocalActivityRetryWorkflow],
        activities=[LocalActivities(backend).append_then_fail_first_attempt],
    ):
        handle = await client.start_workflow(
            LocalActivityRetryWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        await handle.result()

    records = await read_all(backend, StreamRef.for_handle(handle))
    assert await values(records) == ["attempt 1", "attempt 2"]
    first, second = records[0].producer, records[1].producer
    assert first.producer == second.producer
    assert first.producer.startswith("local-activity/")
    assert first.workflow_run_id == handle.first_execution_run_id
    assert (first.workflow_task, first.workflow_task_attempt) == (
        second.workflow_task,
        second.workflow_task_attempt,
    )
    assert second.epoch > first.epoch


@workflow.defn
class LocalActivityOutlivesTaskWorkflow:
    @workflow.run
    async def run(self) -> None:
        await workflow.execute_local_activity_method(
            LocalActivities.sleep_then_append,
            StreamRef.for_workflow(),
            start_to_close_timeout=timedelta(seconds=10),
        )


async def test_local_activity_appends_after_task_heartbeat(
    client: Client, backend: StreamBackend
):
    """Every backend: a local activity can append after its workflow task was completed early to
    keep it running. Native only: its append creates the stream, so its task has no commit record,
    and the stream's creation record says the task completed before the stream existed, which makes
    its records committed.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[LocalActivityOutlivesTaskWorkflow],
        activities=[LocalActivities(backend).sleep_then_append],
        stream_backend=backend,
    ):
        handle = await client.start_workflow(
            LocalActivityOutlivesTaskWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
            task_timeout=timedelta(seconds=1),
        )
        await handle.result()

    ref = StreamRef.for_handle(handle)
    records = await read_all_with_markers(backend, ref)
    [late] = [r for r in records if not r.is_marker]
    assert await decode(late, str) == "late"
    assert late.producer.producer.startswith("local-activity/")
    if isinstance(backend, NativeStreamBackend):
        [created] = [r.stream_created for r in records if r.stream_created]
        assert created.workflow_run_id == late.producer.workflow_run_id
        assert late.producer.workflow_task < created.next_workflow_task
        assert commit_attempt(records, late) is None


failed_local_activity_task_attempts = 0


@workflow.defn(sandboxed=False)
class LocalActivityOfFailedTaskWorkflow:
    @workflow.run
    async def run(self) -> None:
        global failed_local_activity_task_attempts
        await workflow.execute_local_activity_method(
            LocalActivities.append,
            StreamRef.for_workflow(),
            start_to_close_timeout=timedelta(seconds=10),
        )
        if failed_local_activity_task_attempts == 0:
            failed_local_activity_task_attempts += 1
            raise RuntimeError("fail the workflow task")


async def test_local_activity_of_failed_task_does_not_collide_with_rerun(
    client: Client, backend: StreamBackend
):
    """Every backend: the rerun of a failed workflow task runs its local activity again, which
    appends as the same producer under a higher workflow task attempt, so the two do not collide.
    Native only: the task's commit record has the rerun's attempt.
    """
    global failed_local_activity_task_attempts
    failed_local_activity_task_attempts = 0
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[LocalActivityOfFailedTaskWorkflow],
        activities=[LocalActivities(backend).append],
        stream_backend=backend,
    ):
        handle = await client.start_workflow(
            LocalActivityOfFailedTaskWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        await handle.result()

    records = await read_all_with_markers(backend, StreamRef.for_handle(handle))
    failed, rerun = [r for r in records if not r.is_marker]
    assert failed.producer.producer == rerun.producer.producer
    assert failed.producer.workflow_task == rerun.producer.workflow_task
    assert rerun.producer.workflow_task_attempt > failed.producer.workflow_task_attempt
    if isinstance(backend, NativeStreamBackend):
        assert commit_attempt(records, rerun) == rerun.producer.workflow_task_attempt


@workflow.defn(sandboxed=False)
class SlowLastActivationWorkflow:
    def __init__(self) -> None:
        self.go = False

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self.go)
        workflow_append("last")
        # Lets the test signal while the completion that ends the run is in flight.
        time.sleep(1)

    @workflow.signal
    def start(self) -> None:
        self.go = True

    @workflow.signal
    def other(self) -> None:
        pass


async def test_rejected_terminal_completion_does_not_close_stream(
    client: Client, backend: StreamBackend
):
    """Every backend: a signal arriving while the run's terminal completion is in flight makes the
    server reject that completion. The stream stays open, so the rerun of the task can append, and
    closes when the server accepts a terminal completion. The failed attempt's record may also be
    delivered, so only the set of values is checked.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[SlowLastActivationWorkflow],
        stream_backend=backend,
    ):
        handle = await client.start_workflow(
            SlowLastActivationWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        await handle.signal(SlowLastActivationWorkflow.start)
        await asyncio.sleep(0.5)
        await handle.signal(SlowLastActivationWorkflow.other)
        await handle.result()

    ref = StreamRef.for_handle(handle)
    records = await read_all(backend, ref)
    assert set(await values(records)) == {"last"}
    end = await backend.read(ref, after=records[-1].cursor)
    assert end.close is not None and end.close.status == CloseStatus.COMPLETED


retried_run_attempts = 0


@workflow.defn(sandboxed=False)
class FailFirstRunWorkflow:
    @workflow.run
    async def run(self) -> None:
        workflow_append(f"run attempt {workflow.info().attempt}")
        if workflow.info().attempt == 1:
            raise ApplicationError("fail the first run")


async def test_failed_run_with_retry_policy_does_not_close_stream(
    client: Client, backend: StreamBackend
):
    """Every backend: the retried run appends to the same stream, which closes when the retried run
    completes. Fails on native, because the stream is not moved to the retried run.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[FailFirstRunWorkflow],
        stream_backend=backend,
    ):
        handle = await client.start_workflow(
            FailFirstRunWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=10), maximum_attempts=2
            ),
        )
        await handle.result()

    ref = StreamRef.for_handle(handle)
    records = await read_all(backend, ref)
    assert await values(records) == ["run attempt 1", "run attempt 2"]
    end = await backend.read(ref, after=records[-1].cursor)
    assert end.close is not None and end.close.status == CloseStatus.COMPLETED


@workflow.defn
class CronWorkflow:
    @workflow.run
    async def run(self) -> None:
        workflow_append("cron run")


async def test_completed_cron_run_does_not_close_stream(
    client: Client, backend: StreamBackend
):
    """Every backend: a completed cron run does not close the stream, and the next run appends to
    it. Fails on native, because the stream is not moved to the next run.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[CronWorkflow],
        stream_backend=backend,
    ):
        handle = await client.start_workflow(
            CronWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
            cron_schedule="@every 1s",
        )
        ref = StreamRef.for_handle(handle)
        first = await backend.read(ref, wait=timedelta(seconds=10))
        assert await values(list(first.records)) == ["cron run"]
        # Wait until a later run has appended, so the first run's end has been processed.
        second = await backend.read(ref, after=first.cursor, wait=timedelta(seconds=10))
        assert await values(list(second.records)) == ["cron run"]
        assert first.close is None and second.close is None
        await handle.terminate()


@workflow.defn
class FailingCronWorkflow:
    @workflow.run
    async def run(self) -> None:
        workflow_append("cron run")
        raise ApplicationError("fail the cron run")


async def test_failed_cron_run_does_not_close_stream(
    client: Client, backend: StreamBackend
):
    """Every backend: a failed cron run does not close the stream, because the next cron run follows
    it, and the next run appends to it. Fails on native, because the stream is not moved to the
    next run.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[FailingCronWorkflow],
        stream_backend=backend,
    ):
        handle = await client.start_workflow(
            FailingCronWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
            cron_schedule="@every 1s",
        )
        ref = StreamRef.for_handle(handle)
        first = await backend.read(ref, wait=timedelta(seconds=10))
        assert await values(list(first.records)) == ["cron run"]
        # Wait until a later run has appended, so the first run's end has been processed.
        second = await backend.read(ref, after=first.cursor, wait=timedelta(seconds=10))
        assert await values(list(second.records)) == ["cron run"]
        assert first.close is None and second.close is None
        await handle.terminate()


@workflow.defn
class NoAppendWorkflow:
    @workflow.run
    async def run(self) -> None:
        pass


async def test_append_after_workflow_ended_without_appending_fails_as_closed(
    client: Client, backend: StreamBackend
):
    """Every backend: an append to the stream of a workflow that completed without appending fails
    as closed. Native creates the stream closed; on Redis the worker wrote the close when the
    workflow completed.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[NoAppendWorkflow],
        stream_backend=backend,
    ):
        handle = await client.start_workflow(
            NoAppendWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )
        await handle.result()
        ref = StreamRef.for_handle(handle)
        # On Redis, wait for the close, which the worker writes after the run's eviction.
        await backend.read(ref, wait=timedelta(seconds=5))

    writer = StreamWriter(backend, ref, ClientProducer("restaurant", 1))
    with pytest.raises(StreamClosedError):
        await writer.append("late")


@workflow.defn
class ManyRecordsWorkflow:
    @workflow.run
    async def run(self) -> None:
        for i in range(2500):
            workflow_append(i)


async def test_activation_of_more_records_than_one_append_holds(
    client: Client, backend: StreamBackend
):
    """Every backend: an activation's records that exceed one append's size limits are split into
    appends that fit, and all are stored in order.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[ManyRecordsWorkflow],
        stream_backend=backend,
    ):
        handle = await client.start_workflow(
            ManyRecordsWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )
        await handle.result()

    records = await read_all(backend, StreamRef.for_handle(handle))
    assert [await decode(r, int) for r in records] == list(range(2500))


async def test_stream_closes_on_completion_with_workflow_cache_disabled(
    client: Client, backend: StreamBackend
):
    """Every backend: the stream closes when the run completes with the workflow cache disabled.
    On Redis this depends on Core reporting the eviction that follows the accepted completion as
    WORKFLOW_EXECUTION_ENDING rather than CACHE_FULL.
    """
    task_queue = f"tq-{uuid.uuid4()}"
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
        await handle.signal(WaitForSignalWorkflow.finish)
        await handle.result()

    ref = StreamRef.for_handle(handle)
    records = await read_all(backend, ref)
    end = await backend.read(ref, after=records[-1].cursor, wait=timedelta(seconds=5))
    assert end.close is not None and end.close.status == CloseStatus.COMPLETED


@pytest_asyncio.fixture  # type: ignore[reportUntypedFunctionDecorator]
async def commit_mode_backend(client: Client) -> StreamBackend:
    return await NativeStreamBackend.connect(client, commit_mode=True)


def append_modes(records: list[Record]) -> list[WorkflowAppendMode]:
    return [r.workflow_append_mode.mode for r in records if r.workflow_append_mode]


async def test_commit_mode_delivers_only_the_committed_task_attempt(
    client: Client, commit_mode_backend: StreamBackend
):
    """Native commit mode only: the records of a failed workflow task attempt are never delivered.
    The records of every activation of the attempt that commits, before and after a local activity
    resolves, are delivered with its completion, and the run's mode is recorded as commit.
    """
    global failed_task_attempts
    failed_task_attempts = 0
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[FailFirstTaskWorkflow],
        activities=[noop],
        stream_backend=commit_mode_backend,
    ):
        handle = await client.start_workflow(
            FailFirstTaskWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )
        await handle.result()

    records = await read_all_with_markers(
        commit_mode_backend, StreamRef.for_handle(handle)
    )
    data = [r for r in records if not r.is_marker]
    assert await values(data) == ["before local activity", "after local activity"]
    assert {r.producer.workflow_task_attempt for r in data} == {2}
    assert commit_attempt(records, data[0]) == 2
    assert append_modes(records) == [WorkflowAppendMode.COMMIT]


async def test_commit_mode_records_are_visible_when_the_task_commits(
    client: Client, commit_mode_backend: StreamBackend
):
    """Native commit mode only: a workflow's records are visible while it runs, once their task
    commits, and the stream closes as completed when the run completes.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[WaitForSignalWorkflow],
        stream_backend=commit_mode_backend,
    ):
        handle = await client.start_workflow(
            WaitForSignalWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )
        ref = StreamRef.for_handle(handle)
        first = await commit_mode_backend.read(ref, wait=timedelta(seconds=10))
        assert await values([r for r in first.records if not r.is_marker]) == ["first"]
        await handle.signal(WaitForSignalWorkflow.finish)
        await handle.result()

    records = await read_all(commit_mode_backend, ref)
    assert await values(records) == ["first", "second"]
    end = await commit_mode_backend.read(ref, after=records[-1].cursor)
    assert end.close is not None and end.close.status == CloseStatus.COMPLETED


async def test_commit_mode_records_are_not_sent_again_on_replay(
    client: Client, commit_mode_backend: StreamBackend
):
    """Native commit mode only: with no cache, every workflow task replays the workflow from the
    start; the records of earlier tasks are not sent again.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[WaitForSignalWorkflow],
        stream_backend=commit_mode_backend,
        max_cached_workflows=0,
    ):
        handle = await client.start_workflow(
            WaitForSignalWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )
        await commit_mode_backend.read(
            StreamRef.for_handle(handle), wait=timedelta(seconds=10)
        )
        await handle.signal(WaitForSignalWorkflow.finish)
        await handle.result()

    records = await read_all(commit_mode_backend, StreamRef.for_handle(handle))
    assert await values(records) == ["first", "second"]


async def test_workers_that_disagree_on_append_mode_fail_the_workflow_task(
    client: Client, commit_mode_backend: StreamBackend
):
    """Native only: as in a rolling deploy, a sink-mode worker fixes the run's mode, and a
    commit-mode worker's workflow task then fails, naming both modes.
    """
    backend = await NativeStreamBackend.connect(client)
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
        await backend.read(StreamRef.for_handle(handle), wait=timedelta(seconds=10))

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[WaitForSignalWorkflow],
        stream_backend=commit_mode_backend,
    ):
        await handle.signal(WaitForSignalWorkflow.finish)
        failure = await wait_for_workflow_task_failure(handle)
        await handle.terminate()

    assert (
        failure.cause
        == WorkflowTaskFailedCause.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE
    )
    assert failure.failure.application_failure_info.type == "BAD_STREAM_APPEND"
    assert "WORKFLOW_APPEND_MODE_SINK" in failure.failure.message
    assert "WORKFLOW_APPEND_MODE_COMMIT" in failure.failure.message


async def wait_for_workflow_task_failure(
    handle: WorkflowHandle,
) -> WorkflowTaskFailedEventAttributes:
    for _ in range(100):
        async for event in handle.fetch_history_events():
            if event.HasField("workflow_task_failed_event_attributes"):
                return event.workflow_task_failed_event_attributes
        await asyncio.sleep(0.1)
    raise AssertionError("no workflow task failed")


class ToolActivities:
    """A tool call that owns its stream, as a Standalone Activity, and a workflow activity that
    starts it.
    """

    def __init__(self, client: Client, backend: StreamBackend, task_queue: str) -> None:
        self._client = client
        self._backend = backend
        self._task_queue = task_queue

    @activity.defn
    async def tool(self) -> str:
        writer = StreamWriter.for_activity(self._backend, StreamRef.for_activity())
        await writer.append("tool started", topic="progress")
        await writer.append("tool output")
        return "done"

    @activity.defn
    async def start_tool(self, tool_id: str) -> None:
        await self._client.start_activity(
            "tool",
            id=tool_id,
            task_queue=self._task_queue,
            start_to_close_timeout=timedelta(seconds=10),
        )


@workflow.defn
class StartToolWorkflow:
    @workflow.run
    async def run(self, tool_id: str) -> None:
        await workflow.execute_activity_method(
            ToolActivities.start_tool,
            tool_id,
            start_to_close_timeout=timedelta(seconds=10),
        )


async def test_client_reads_the_stream_of_a_standalone_activity_a_workflow_started(
    client: Client, backend: StreamBackend
):
    """Every backend: a tool call owns its stream: a workflow starts a Standalone Activity, which
    appends to its own stream, and a client that refers to the stream before the activity starts
    reads it. Native only: the stream closes with the activity's outcome; on Redis nothing closes
    it.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    tool_id = f"tool-{uuid.uuid4()}"
    tools = ToolActivities(client, backend, task_queue)
    ref = StreamRef(activity_id=tool_id)
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[StartToolWorkflow],
        activities=[tools.tool, tools.start_tool],
    ):
        first = await backend.read(ref, wait=timedelta(seconds=1))
        assert not first.records
        await client.execute_workflow(
            StartToolWorkflow.run,
            tool_id,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        assert await client.get_activity_handle(tool_id).result() == "done"

    records = await read_all(backend, ref)
    assert await values(records) == ["tool started", "tool output"]
    assert records[0].producer.producer == records[1].producer.producer
    if isinstance(backend, NativeStreamBackend):
        end = await backend.read(ref, after=records[-1].cursor)
        assert end.close is not None and end.close.status == CloseStatus.COMPLETED
