"""End-to-end tests of streams, against each stream backend. Each test says which guarantee it
checks and whether it holds on every backend.

These need redis-server on PATH, and the prototype's server with retry stamps and direct Nexus
calls enabled, as the superproject's dev-server runs it: pass ``--workflow-environment
localhost:7233``.

The three tests of a run chain that continues (a failed run with a retry policy, and a cron run that
completes or fails) fail on native, because native streams are not moved to the next run of the
chain.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import sys
import time
import uuid
from datetime import timedelta
from typing import Any

import pytest

from temporalio import activity, workflow
from temporalio.api.enums.v1 import EventType, WorkflowTaskFailedCause
from temporalio.api.history.v1 import WorkflowTaskFailedEventAttributes
from temporalio.client import Client, WorkflowFailureError, WorkflowHandle
from temporalio.common import RetryPolicy
from temporalio.contrib.redis_streams import RedisStreamBackend
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.streams import (
    CloseStatus,
    EpochFencedError,
    RecordStatus,
    StreamBackends,
    StreamClosedError,
    StreamHandle,
    StreamHandleBase,
    StreamMode,
    StreamRecord,
    StreamRecordAsyncIterator,
    StreamSettingsMismatchError,
    StreamTooLargeError,
    Withdrawal,
    WorkflowStreamHandle,
)
from temporalio.streams.backend import (
    NATIVE,
    Record,
    StreamRef,
    WorkflowAppendMode,
    client_backend,
)
from temporalio.streams.native import NativeStreamBackend
from temporalio.worker import Replayer, Worker
from tests.streams.conftest import is_native, redis_client


def stream_ref(handle: WorkflowHandle, name: str = "main") -> StreamRef:
    return StreamRef(handle.id, handle.first_execution_run_id or "", name=name)


async def raw_records(client: Client, ref: StreamRef) -> list[Record]:
    """Every stored record, markers included, as the default backend returns them."""
    backend = client_backend(client)
    records: list[Record] = []
    cursor = None
    while True:
        resp = await backend.read(ref, after=cursor)
        if not resp.records:
            return records
        records.extend(resp.records)
        cursor = resp.cursor


def data(records: list[Record]) -> list[Record]:
    return [r for r in records if not r.is_marker]


async def raw_values(client: Client, records: list[Record]) -> list[Any]:
    return [
        await client.data_converter.decode([r.payload])
        for r in data(records)
        if r.payload
    ]


async def read_values(
    stream: StreamHandle, type_hint: type = str, **kwargs: Any
) -> list[Any]:
    """The values a reader gets, without the withdrawal notices."""
    return [
        r.value
        async for r in stream.fetch_records(type_hint=type_hint, **kwargs)
        if isinstance(r, StreamRecord)
    ]


async def read_to_end(stream: StreamHandle) -> StreamRecordAsyncIterator[Any]:
    it = stream.fetch_records()
    async for _ in it:
        pass
    return it


async def collect_until_close(
    it: StreamRecordAsyncIterator[Any],
) -> list[StreamRecord[Any] | Withdrawal]:
    async def collect() -> list[StreamRecord[Any] | Withdrawal]:
        return [r async for r in it]

    return await asyncio.wait_for(collect(), timeout=30)


async def start_owner(client: Client) -> WorkflowHandle:
    """Start a workflow, with no worker, to own a stream."""
    return await client.start_workflow(
        "StreamOwner", id=f"wf-{uuid.uuid4()}", task_queue=f"tq-{uuid.uuid4()}"
    )


async def test_client_publish_and_read(stream_client: Client):
    """Every backend: a producer's values are stored in order, each publish is one append, a
    record's payload is the value's payload, and a read after a record's cursor resumes after that
    record.
    """
    handle = await start_owner(stream_client)
    stream = handle.stream("main")
    async with stream.producer("restaurant") as producer:
        await producer.publish("accepted", topic="stage")
        stage = producer.topic("stage", type=str)
        await stage.publish("ready", "picked up")

    records = [r async for r in stream.fetch_records(type_hint=str)]
    assert all(isinstance(r, StreamRecord) for r in records)
    assert [r.value for r in records if isinstance(r, StreamRecord)] == [
        "accepted",
        "ready",
        "picked up",
    ]
    raw = data(await raw_records(stream_client, stream_ref(handle)))
    assert all(
        r.payload and r.payload.metadata["encoding"] == b"json/plain" for r in raw
    )
    assert [r.seq for r in raw] == [0, 1, 1]
    first = records[0]
    assert isinstance(first, StreamRecord)
    assert await read_values(stream, after=first.cursor) == ["ready", "picked up"]


async def test_client_creates_a_standalone_stream_and_closes_it_on_exit(
    stream_client: Client,
):
    """Every backend: a client creates a standalone stream, which exists before any append, writes
    to it, and closes it on leaving the ``async with`` block. A stream not created yet does not
    exist.
    """
    stream_id = f"s-{uuid.uuid4()}"
    assert not await stream_client.get_stream_handle(stream_id=stream_id).exists()
    async with stream_client.create_stream(stream_id) as stream:
        assert await stream.exists()
        assert await read_values(stream) == []
        async with stream.producer("p") as producer:
            await producer.publish("a", "b")
    assert await read_values(stream) == ["a", "b"]
    end = await read_to_end(stream)
    assert end.close is not None and end.close.status == CloseStatus.COMPLETED
    with pytest.raises(StreamClosedError):
        await stream_client.create_stream(stream_id)


async def test_create_with_another_idle_timeout_fails(stream_client: Client):
    """Every backend: a create of a standalone stream that exists with another idle timeout fails;
    one with the same settings succeeds.
    """
    stream_id = f"s-{uuid.uuid4()}"
    await stream_client.create_stream(stream_id, idle_timeout=timedelta(hours=1))
    await stream_client.create_stream(stream_id, idle_timeout=timedelta(hours=1))
    with pytest.raises(StreamSettingsMismatchError):
        await stream_client.create_stream(stream_id, idle_timeout=timedelta(hours=2))


@workflow.defn
class WaitForSignalWorkflow:
    def __init__(self) -> None:
        self.done = False

    @workflow.run
    async def run(self) -> None:
        stream = await workflow.create_stream("main")
        await stream.publish("first")
        await workflow.wait_condition(lambda: self.done)
        await stream.publish("second")

    @workflow.signal
    def finish(self) -> None:
        self.done = True


async def test_workflow_publish_is_visible_while_workflow_runs_and_closes_on_completion(
    stream_client: Client,
):
    """Every backend: a workflow's records are visible while it runs, a later workflow task's
    records carry a higher workflow task, and the stream closes as completed when the run completes:
    on native the server closes it, on Redis the worker. The read states the stream's mode, sink.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        stream_client, task_queue=task_queue, workflows=[WaitForSignalWorkflow]
    ):
        handle = await stream_client.start_workflow(
            WaitForSignalWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )
        stream = handle.stream("main")
        it = stream.fetch_records(wait_new_records=True, type_hint=str)
        first = await asyncio.wait_for(it.__anext__(), timeout=10)
        assert isinstance(first, StreamRecord) and first.value == "first"
        assert it.close is None

        await handle.signal(WaitForSignalWorkflow.finish)
        await handle.result()

    records = [r async for r in stream.fetch_records(type_hint=str)]
    assert [r.value for r in records if isinstance(r, StreamRecord)] == [
        "first",
        "second",
    ]
    first, second = records
    assert isinstance(first, StreamRecord) and isinstance(second, StreamRecord)
    assert first.producer.producer == second.producer.producer
    assert second.producer.workflow_task > first.producer.workflow_task
    end = await read_to_end(stream)
    assert end.close is not None and end.close.status == CloseStatus.COMPLETED
    assert end.mode == StreamMode.SINK


async def test_workflow_creates_its_stream_before_appending(stream_client: Client):
    """Every backend: a stream that a workflow creates exists before its first append."""
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        stream_client, task_queue=task_queue, workflows=[WaitForSignalWorkflow]
    ):
        handle = await stream_client.start_workflow(
            WaitForSignalWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )
        await asyncio.wait_for(
            handle.stream("main").fetch_records(wait_new_records=True).__anext__(),
            timeout=10,
        )
        assert await handle.stream("main").exists()
        assert not await handle.stream("other").exists()
        await handle.signal(WaitForSignalWorkflow.finish)
        await handle.result()


@workflow.defn
class PublishThreeTimesWorkflow:
    @workflow.run
    async def run(self) -> None:
        stream = await workflow.create_stream("main")
        for i in range(3):
            await stream.publish(i)


async def test_awaited_workflow_publishes_add_no_workflow_task(stream_client: Client):
    """Every backend: each awaited publish is stored before workflow code resumes, within the
    activation, so three publishes in a row are three appends in one workflow task.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        stream_client, task_queue=task_queue, workflows=[PublishThreeTimesWorkflow]
    ):
        handle = await stream_client.start_workflow(
            PublishThreeTimesWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        await handle.result()

    records = data(await raw_records(stream_client, stream_ref(handle)))
    assert await raw_values(stream_client, records) == [[0], [1], [2]]
    assert len({r.producer.workflow_task for r in records}) == 1
    assert [r.seq for r in records] == [0, 1, 2]
    completed = [
        e
        async for e in handle.fetch_history_events()
        if e.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED
    ]
    assert len(completed) == 1


@activity.defn
async def noop() -> None:
    pass


@workflow.defn
class InterleavedPublishWorkflow:
    @workflow.run
    async def run(self) -> None:
        stream = await workflow.create_stream("main")

        async def publish_then_run_activity() -> None:
            await stream.publish("a")
            await workflow.execute_activity(
                noop, start_to_close_timeout=timedelta(seconds=10)
            )

        async def run_activity_then_publish() -> None:
            await workflow.execute_activity(
                noop, start_to_close_timeout=timedelta(seconds=10)
            )
            await stream.publish("b")

        await asyncio.gather(publish_then_run_activity(), run_activity_then_publish())


async def test_replay_of_awaited_publishes_is_deterministic(stream_client: Client):
    """Every backend: workflow code resumes after an awaited publish at the same point on replay,
    which does no I/O, as when it first ran, so commands interleaved with publishes replay without
    nondeterminism.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        stream_client,
        task_queue=task_queue,
        workflows=[InterleavedPublishWorkflow],
        activities=[noop],
    ):
        handle = await stream_client.start_workflow(
            InterleavedPublishWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        await handle.result()
    await Replayer(workflows=[InterleavedPublishWorkflow]).replay_workflow(
        await handle.fetch_history()
    )
    assert sorted(await read_values(handle.stream("main"))) == ["a", "b"]


@workflow.defn
class TwoStreamsWorkflow:
    @workflow.run
    async def run(self) -> None:
        progress = await workflow.create_stream("progress")
        tokens = await workflow.create_stream("tokens")
        await progress.publish("started")
        await tokens.publish("t1", "t2")
        await progress.publish("done")


async def test_workflow_streams_are_named_and_independent(stream_client: Client):
    """Every backend: a workflow owns several named streams, each with its own records and seqs."""
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        stream_client, task_queue=task_queue, workflows=[TwoStreamsWorkflow]
    ):
        handle = await stream_client.start_workflow(
            TwoStreamsWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )
        await handle.result()

    assert await read_values(handle.stream("progress")) == ["started", "done"]
    assert await read_values(handle.stream("tokens")) == ["t1", "t2"]
    progress = data(await raw_records(stream_client, stream_ref(handle, "progress")))
    assert [r.seq for r in progress] == [0, 1]
    for name in ["progress", "tokens"]:
        end = await read_to_end(handle.stream(name))
        assert end.close is not None and end.close.status == CloseStatus.COMPLETED


@activity.defn
async def publish_to(given: StreamHandleBase) -> None:
    await activity.get_stream_handle(given).publish("from activity")


@workflow.defn
class PublishToGivenStreamWorkflow:
    @workflow.run
    async def run(self, given: StreamHandleBase) -> None:
        stream = workflow.get_stream_handle(given)
        await stream.publish("from workflow")
        await workflow.execute_activity(
            publish_to, given, start_to_close_timeout=timedelta(seconds=10)
        )


async def test_workflow_and_activity_write_to_a_standalone_stream_passed_in(
    stream_client: Client,
):
    """Every backend: a client creates a standalone stream and passes its handle, as a value, to a
    workflow, which publishes to it as an unverified producer and passes it on to an activity,
    which publishes to it too.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    stream = await stream_client.create_stream(f"s-{uuid.uuid4()}")
    async with Worker(
        stream_client,
        task_queue=task_queue,
        workflows=[PublishToGivenStreamWorkflow],
        activities=[publish_to],
    ):
        await stream_client.execute_workflow(
            PublishToGivenStreamWorkflow.run,
            stream,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )

    records = [r async for r in stream.fetch_records(type_hint=str)]
    assert [r.value for r in records if isinstance(r, StreamRecord)] == [
        "from workflow",
        "from activity",
    ]
    producers = [r.producer.producer for r in records if isinstance(r, StreamRecord)]
    assert producers[0].startswith("workflow/") and producers[1].startswith("activity/")


@workflow.defn
class ContinueAsNewWithStreamWorkflow:
    @workflow.run
    async def run(self, stream: StreamHandleBase, run: int) -> None:
        await workflow.get_stream_handle(stream).publish(f"run {run}")
        if run == 1:
            workflow.continue_as_new(args=[stream, 2])


async def test_a_handle_passes_through_continue_as_new(stream_client: Client):
    """Every backend: a stream handle passes through continue-as-new in the workflow's input, so
    the next run writes to the same stream instead of creating one.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    stream = await stream_client.create_stream(f"s-{uuid.uuid4()}")
    async with Worker(
        stream_client,
        task_queue=task_queue,
        workflows=[ContinueAsNewWithStreamWorkflow],
    ):
        await stream_client.execute_workflow(
            ContinueAsNewWithStreamWorkflow.run,
            args=[stream, 1],
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
    assert await read_values(stream) == ["run 1", "run 2"]


@workflow.defn
class ScopedStreamWorkflow:
    def __init__(self) -> None:
        self.done = False

    @workflow.run
    async def run(self) -> None:
        async with workflow.create_stream("scoped") as stream:
            await stream.publish("inside")
        await workflow.wait_condition(lambda: self.done)

    @workflow.signal
    def finish(self) -> None:
        self.done = True


async def test_workflow_scoped_stream_closes_on_exit(stream_client: Client):
    """Every backend: a stream a workflow creates with ``async with`` closes when the block exits,
    while the workflow runs on.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        stream_client, task_queue=task_queue, workflows=[ScopedStreamWorkflow]
    ):
        handle = await stream_client.start_workflow(
            ScopedStreamWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )
        it = handle.stream("scoped").fetch_records(wait_new_records=True, type_hint=str)
        items = await collect_until_close(it)
        assert [r.value for r in items if isinstance(r, StreamRecord)] == ["inside"]
        assert it.close is not None and it.close.status == CloseStatus.COMPLETED
        await handle.signal(ScopedStreamWorkflow.finish)
        await handle.result()


@workflow.defn
class PublishToClosedStreamWorkflow:
    @workflow.run
    async def run(self, given: StreamHandleBase) -> str:
        stream = workflow.get_stream_handle(given)
        try:
            await stream.publish("late")
        except Exception as err:
            return f"raised {type(err).__name__}"
        return "published"


async def test_store_failure_fails_the_workflow_task(stream_client: Client):
    """Every backend: when the store fails a workflow's append, the workflow task fails and is
    retried; workflow code never sees the error, since replay would not.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    stream = await stream_client.create_stream(f"s-{uuid.uuid4()}")
    await stream.close()
    async with Worker(
        stream_client,
        task_queue=task_queue,
        workflows=[PublishToClosedStreamWorkflow],
    ):
        handle = await stream_client.start_workflow(
            PublishToClosedStreamWorkflow.run,
            stream,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        failure = await wait_for_workflow_task_failure(handle)
        await handle.terminate()
    assert failure.failure.application_failure_info.type == "StreamClosedError"


async def test_workflow_publish_is_not_repeated_on_replay(stream_client: Client):
    """Every backend: replay does not append a workflow's records again."""
    task_queue = f"tq-{uuid.uuid4()}"
    # With no cache, every workflow task replays the workflow from the start.
    async with Worker(
        stream_client,
        task_queue=task_queue,
        workflows=[WaitForSignalWorkflow],
        max_cached_workflows=0,
    ):
        handle = await stream_client.start_workflow(
            WaitForSignalWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )
        it = handle.stream("main").fetch_records(wait_new_records=True)
        await asyncio.wait_for(it.__anext__(), timeout=10)
        await handle.signal(WaitForSignalWorkflow.finish)
        await handle.result()

    assert await read_values(handle.stream("main")) == ["first", "second"]


failed_task_attempts = 0


@workflow.defn(sandboxed=False)
class FailFirstTaskWorkflow:
    @workflow.run
    async def run(self) -> None:
        global failed_task_attempts
        stream = await workflow.create_stream("main")
        await stream.publish("before local activity")
        # The local activity's result arrives in a second activation of the same workflow task, so
        # the task's first activation has already appended when the task fails.
        await workflow.execute_local_activity(
            noop, start_to_close_timeout=timedelta(seconds=10)
        )
        if failed_task_attempts == 0:
            failed_task_attempts += 1
            raise RuntimeError("fail the workflow task")
        await stream.publish("after local activity")


async def test_failed_workflow_task_records_carry_lower_epoch(stream_client: Client):
    """Every backend: a failed workflow task attempt may deliver records, and the rerun's records
    carry the same producer and workflow task with a higher epoch, so a reader discards the failed
    attempt's.
    """
    global failed_task_attempts
    failed_task_attempts = 0
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        stream_client,
        task_queue=task_queue,
        workflows=[FailFirstTaskWorkflow],
        activities=[noop],
    ):
        handle = await stream_client.start_workflow(
            FailFirstTaskWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )
        await handle.result()

    records = data(await raw_records(stream_client, stream_ref(handle)))
    assert await raw_values(stream_client, records) == [
        ["before local activity"],
        ["before local activity"],
        ["after local activity"],
    ]
    failed, rerun = records[0].producer, records[1].producer
    assert failed.producer == rerun.producer
    assert failed.workflow_task == rerun.workflow_task
    assert rerun.epoch > failed.epoch
    assert await read_values(handle.stream("main")) == [
        "before local activity",
        "after local activity",
    ]


@activity.defn
async def publish_then_fail_first_attempt(given: StreamHandleBase) -> None:
    stream = activity.get_stream_handle(given)
    info = activity.info()
    await stream.publish(f"attempt {info.attempt}")
    if info.attempt == 1:
        raise RuntimeError("fail the first attempt")


@workflow.defn
class RunActivityWorkflow:
    @workflow.run
    async def run(self, retry_interval_seconds: float) -> None:
        stream = await workflow.create_stream("main")
        await workflow.execute_activity(
            publish_then_fail_first_attempt,
            stream,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=retry_interval_seconds),
                maximum_attempts=2,
            ),
        )


async def test_retried_activity_attempt_has_higher_epoch(stream_client: Client):
    """Every backend: a retried activity attempt appends as the same producer with a higher epoch,
    and a reader that reads both discards the earlier attempt's records. Holds only when the server
    enables ``system.enableActivityRetryStampIncrement``.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        stream_client,
        task_queue=task_queue,
        workflows=[RunActivityWorkflow],
        activities=[publish_then_fail_first_attempt],
    ):
        handle = await stream_client.start_workflow(
            RunActivityWorkflow.run,
            0.01,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        await handle.result()

    records = data(await raw_records(stream_client, stream_ref(handle)))
    assert await raw_values(stream_client, records) == [["attempt 1"], ["attempt 2"]]
    assert records[0].producer.producer == records[1].producer.producer
    assert records[1].producer.epoch > records[0].producer.epoch
    assert await read_values(handle.stream("main")) == ["attempt 2"]


async def test_reader_is_told_when_a_higher_activity_epoch_supersedes_a_yielded_record(
    stream_client: Client,
):
    """Every backend: a reader that has already received a failed activity attempt's record gets a
    withdrawal notice for it when the retried attempt appends, then the retry's record.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        stream_client,
        task_queue=task_queue,
        workflows=[RunActivityWorkflow],
        activities=[publish_then_fail_first_attempt],
    ):
        handle = await stream_client.start_workflow(
            RunActivityWorkflow.run,
            2.0,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        it = handle.stream("main").fetch_records(wait_new_records=True, type_hint=str)
        items = await collect_until_close(it)

    first, withdrawal, second = items
    assert isinstance(first, StreamRecord) and first.value == "attempt 1"
    assert first.status == RecordStatus.FINAL
    assert isinstance(withdrawal, Withdrawal)
    assert (withdrawal.cursor, withdrawal.producer) == (first.cursor, first.producer)
    assert isinstance(second, StreamRecord) and second.value == "attempt 2"
    assert it.close is not None and it.close.status == CloseStatus.COMPLETED


withdrawn_task_attempts = 0


@activity.defn
async def sleep_in_first_task_attempt() -> None:
    if withdrawn_task_attempts == 0:
        await asyncio.sleep(2)


@workflow.defn(sandboxed=False)
class PublishOnlyInFailedTaskAttemptWorkflow:
    @workflow.run
    async def run(self) -> None:
        global withdrawn_task_attempts
        stream = await workflow.create_stream("main")
        if withdrawn_task_attempts == 0:
            await stream.publish("doomed")
        await workflow.execute_local_activity(
            sleep_in_first_task_attempt, start_to_close_timeout=timedelta(seconds=10)
        )
        if withdrawn_task_attempts == 0:
            withdrawn_task_attempts += 1
            raise RuntimeError("fail the workflow task")


async def test_reader_is_told_when_a_failed_workflow_task_attempt_is_withdrawn(
    stream_client: Client,
):
    """Native only: a reader receives a record of a workflow task attempt as provisional, and when
    the task's commit record names another attempt, a withdrawal notice for it. On Redis no commit
    record is written, so the record stays provisional and no notice follows.
    """
    global withdrawn_task_attempts
    withdrawn_task_attempts = 0
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        stream_client,
        task_queue=task_queue,
        workflows=[PublishOnlyInFailedTaskAttemptWorkflow],
        activities=[sleep_in_first_task_attempt],
    ):
        handle = await stream_client.start_workflow(
            PublishOnlyInFailedTaskAttemptWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        it = handle.stream("main").fetch_records(wait_new_records=True, type_hint=str)
        items = await collect_until_close(it)

    doomed = items[0]
    assert isinstance(doomed, StreamRecord) and doomed.value == "doomed"
    assert doomed.status == RecordStatus.PROVISIONAL
    if is_native(stream_client):
        [_, withdrawal] = items
        assert isinstance(withdrawal, Withdrawal)
        assert (withdrawal.cursor, withdrawal.producer) == (
            doomed.cursor,
            doomed.producer,
        )
    else:
        assert items == [doomed]
    assert it.close is not None and it.close.status == CloseStatus.COMPLETED


@activity.defn
async def local_publish_then_fail_first_attempt(given: StreamHandleBase) -> None:
    stream = activity.get_stream_handle(given)
    info = activity.info()
    await stream.publish(f"attempt {info.attempt}")
    if info.attempt == 1:
        raise RuntimeError("fail the first attempt")


@activity.defn
async def sleep_then_publish(given: StreamHandleBase) -> None:
    stream = activity.get_stream_handle(given)
    await asyncio.sleep(3)
    await stream.publish("late")


@activity.defn
async def publish(given: StreamHandleBase) -> None:
    stream = activity.get_stream_handle(given)
    await stream.publish("appended")


def own_stream(name: str = "main") -> WorkflowStreamHandle:
    """A handle to the workflow's own stream, which the workflow has not created: it is created by
    its first append.
    """
    info = workflow.info()
    return workflow.get_stream_handle(
        workflow_id=info.workflow_id,
        first_execution_run_id=info.first_execution_run_id,
        name=name,
    )


@workflow.defn
class LocalActivityRetryWorkflow:
    @workflow.run
    async def run(self) -> None:
        await workflow.execute_local_activity(
            local_publish_then_fail_first_attempt,
            own_stream(),
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=10)),
        )


async def test_local_activity_retry_has_higher_epoch(stream_client: Client):
    """Every backend: a retried local activity attempt appends as the same producer, in the same
    workflow task, with a higher epoch, and a reader discards the earlier attempt's records.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        stream_client,
        task_queue=task_queue,
        workflows=[LocalActivityRetryWorkflow],
        activities=[local_publish_then_fail_first_attempt],
    ):
        handle = await stream_client.start_workflow(
            LocalActivityRetryWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        await handle.result()

    records = data(await raw_records(stream_client, stream_ref(handle)))
    assert await raw_values(stream_client, records) == [["attempt 1"], ["attempt 2"]]
    first, second = records[0].producer, records[1].producer
    assert first.producer == second.producer
    assert first.producer.startswith("local-activity/")
    assert first.workflow_run_id == handle.first_execution_run_id
    assert (first.workflow_task, first.workflow_task_attempt) == (
        second.workflow_task,
        second.workflow_task_attempt,
    )
    assert second.epoch > first.epoch
    assert await read_values(handle.stream("main")) == ["attempt 2"]


@workflow.defn
class LocalActivityOutlivesTaskWorkflow:
    @workflow.run
    async def run(self) -> None:
        await workflow.execute_local_activity(
            sleep_then_publish,
            own_stream(),
            start_to_close_timeout=timedelta(seconds=10),
        )


async def test_local_activity_publishes_after_task_heartbeat(stream_client: Client):
    """Every backend: a local activity can append after its workflow task was completed early to
    keep it running. Native only: its append creates the stream, so its task has no commit record,
    and the stream's creation record says the task completed before the stream existed, which makes
    its records final.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        stream_client,
        task_queue=task_queue,
        workflows=[LocalActivityOutlivesTaskWorkflow],
        activities=[sleep_then_publish],
    ):
        handle = await stream_client.start_workflow(
            LocalActivityOutlivesTaskWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
            task_timeout=timedelta(seconds=1),
        )
        await handle.result()

    records = await raw_records(stream_client, stream_ref(handle))
    [late] = data(records)
    assert await raw_values(stream_client, [late]) == [["late"]]
    assert late.producer.producer.startswith("local-activity/")
    [record] = [r async for r in handle.stream("main").fetch_records()]
    assert isinstance(record, StreamRecord) and record.value == "late"
    if is_native(stream_client):
        [created] = [r.stream_created for r in records if r.stream_created]
        assert created.workflow_run_id == late.producer.workflow_run_id
        assert late.producer.workflow_task < created.next_workflow_task
        assert commit_attempt(records, late) is None
        assert record.status == RecordStatus.FINAL
    else:
        assert record.status == RecordStatus.PROVISIONAL


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


failed_local_activity_task_attempts = 0


@workflow.defn(sandboxed=False)
class LocalActivityOfFailedTaskWorkflow:
    @workflow.run
    async def run(self) -> None:
        global failed_local_activity_task_attempts
        await workflow.execute_local_activity(
            publish, own_stream(), start_to_close_timeout=timedelta(seconds=10)
        )
        if failed_local_activity_task_attempts == 0:
            failed_local_activity_task_attempts += 1
            raise RuntimeError("fail the workflow task")


async def test_local_activity_of_failed_task_does_not_collide_with_rerun(
    stream_client: Client,
):
    """Every backend: the rerun of a failed workflow task runs its local activity again, which
    appends as the same producer under a higher workflow task attempt, so the two do not collide.
    Native only: the task's commit record has the rerun's attempt.
    """
    global failed_local_activity_task_attempts
    failed_local_activity_task_attempts = 0
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        stream_client,
        task_queue=task_queue,
        workflows=[LocalActivityOfFailedTaskWorkflow],
        activities=[publish],
    ):
        handle = await stream_client.start_workflow(
            LocalActivityOfFailedTaskWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        await handle.result()

    records = await raw_records(stream_client, stream_ref(handle))
    failed, rerun = data(records)
    assert failed.producer.producer == rerun.producer.producer
    assert failed.producer.workflow_task == rerun.producer.workflow_task
    assert rerun.producer.workflow_task_attempt > failed.producer.workflow_task_attempt
    if is_native(stream_client):
        assert commit_attempt(records, rerun) == rerun.producer.workflow_task_attempt


@workflow.defn(sandboxed=False)
class SlowLastActivationWorkflow:
    def __init__(self) -> None:
        self.go = False

    @workflow.run
    async def run(self) -> None:
        stream = await workflow.create_stream("main")
        await workflow.wait_condition(lambda: self.go)
        await stream.publish("last")
        # Lets the test signal while the completion that ends the run is in flight.
        time.sleep(1)

    @workflow.signal
    def start(self) -> None:
        self.go = True

    @workflow.signal
    def other(self) -> None:
        pass


async def test_rejected_terminal_completion_does_not_close_stream(
    stream_client: Client,
):
    """Every backend: a signal arriving while the run's terminal completion is in flight makes the
    server reject that completion. The stream stays open, so the rerun of the task can append, and
    closes when the server accepts a terminal completion. The failed attempt's record may also be
    delivered, so only the set of values is checked.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        stream_client, task_queue=task_queue, workflows=[SlowLastActivationWorkflow]
    ):
        handle = await stream_client.start_workflow(
            SlowLastActivationWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        await handle.signal(SlowLastActivationWorkflow.start)
        await asyncio.sleep(0.5)
        await handle.signal(SlowLastActivationWorkflow.other)
        await handle.result()

    assert set(await read_values(handle.stream("main"))) == {"last"}
    end = await read_to_end(handle.stream("main"))
    assert end.close is not None and end.close.status == CloseStatus.COMPLETED


@workflow.defn(sandboxed=False)
class FailFirstRunWorkflow:
    @workflow.run
    async def run(self) -> None:
        await own_stream().publish(f"run attempt {workflow.info().attempt}")
        if workflow.info().attempt == 1:
            raise ApplicationError("fail the first run")


async def test_failed_run_with_retry_policy_does_not_close_stream(
    stream_client: Client,
):
    """Every backend: the retried run appends to the same stream, which closes when the retried run
    completes. Fails on native, because the stream is not moved to the retried run.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        stream_client, task_queue=task_queue, workflows=[FailFirstRunWorkflow]
    ):
        handle = await stream_client.start_workflow(
            FailFirstRunWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=10), maximum_attempts=2
            ),
        )
        await handle.result()

    stream = handle.stream("main")
    assert await read_values(stream) == ["run attempt 1", "run attempt 2"]
    end = await read_to_end(stream)
    assert end.close is not None and end.close.status == CloseStatus.COMPLETED


@workflow.defn
class CronWorkflow:
    @workflow.run
    async def run(self) -> None:
        await own_stream().publish("cron run")


@workflow.defn
class FailingCronWorkflow:
    @workflow.run
    async def run(self) -> None:
        await own_stream().publish("cron run")
        raise ApplicationError("fail the cron run")


async def check_next_cron_run_appends(handle: WorkflowHandle) -> None:
    it = handle.stream("main").fetch_records(wait_new_records=True, type_hint=str)
    first = await asyncio.wait_for(it.__anext__(), timeout=10)
    # Wait until a later run has appended, so the first run's end has been processed.
    second = await asyncio.wait_for(it.__anext__(), timeout=10)
    assert isinstance(first, StreamRecord) and first.value == "cron run"
    assert isinstance(second, StreamRecord) and second.value == "cron run"
    assert it.close is None


async def test_completed_cron_run_does_not_close_stream(stream_client: Client):
    """Every backend: a completed cron run does not close the stream, and the next run appends to
    it. Fails on native, because the stream is not moved to the next run.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(stream_client, task_queue=task_queue, workflows=[CronWorkflow]):
        handle = await stream_client.start_workflow(
            CronWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
            cron_schedule="@every 1s",
        )
        try:
            await check_next_cron_run_appends(handle)
        finally:
            await handle.terminate()


async def test_failed_cron_run_does_not_close_stream(stream_client: Client):
    """Every backend: a failed cron run does not close the stream, because the next cron run follows
    it, and the next run appends to it. Fails on native, because the stream is not moved to the
    next run.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        stream_client, task_queue=task_queue, workflows=[FailingCronWorkflow]
    ):
        handle = await stream_client.start_workflow(
            FailingCronWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
            cron_schedule="@every 1s",
        )
        try:
            await check_next_cron_run_appends(handle)
        finally:
            await handle.terminate()


@workflow.defn
class NoAppendWorkflow:
    @workflow.run
    async def run(self) -> None:
        pass


async def test_publish_after_workflow_ended_without_appending_fails_as_closed(
    stream_client: Client,
):
    """Every backend: an append to a stream of a workflow that completed without creating it fails
    as closed, and the producer raises the failure from every later call. Native fails the append
    because the owner has ended; on Redis the worker marked the run as ended when the workflow
    completed.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        stream_client, task_queue=task_queue, workflows=[NoAppendWorkflow]
    ):
        handle = await stream_client.start_workflow(
            NoAppendWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )
        await handle.result()

    producer = handle.stream("main").producer("restaurant")
    # On Redis the worker writes the run's end after the run's eviction.
    for _ in range(50):
        try:
            await producer.publish("late")
        except StreamClosedError:
            break
        await asyncio.sleep(0.1)
        producer = handle.stream("main").producer(f"restaurant-{uuid.uuid4()}")
    else:
        raise AssertionError("the append was not refused")
    with pytest.raises(StreamClosedError):
        await producer.publish("later")


@workflow.defn
class ManyRecordsWorkflow:
    @workflow.run
    async def run(self) -> None:
        stream = await workflow.create_stream("main")
        await stream.publish(*range(2500))


async def test_activation_of_more_records_than_one_append_holds(stream_client: Client):
    """Every backend: a workflow's records that exceed one append's size limits are split into
    appends that fit, and all are stored in order.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        stream_client, task_queue=task_queue, workflows=[ManyRecordsWorkflow]
    ):
        handle = await stream_client.start_workflow(
            ManyRecordsWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )
        await handle.result()

    assert await read_values(handle.stream("main"), type_hint=int) == list(range(2500))


async def test_stream_closes_on_completion_with_workflow_cache_disabled(
    stream_client: Client,
):
    """Every backend: the stream closes when the run completes with the workflow cache disabled.
    On Redis this depends on Core reporting the eviction that follows the accepted completion as
    WORKFLOW_EXECUTION_ENDING rather than CACHE_FULL.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        stream_client,
        task_queue=task_queue,
        workflows=[WaitForSignalWorkflow],
        max_cached_workflows=0,
    ):
        handle = await stream_client.start_workflow(
            WaitForSignalWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )
        await handle.signal(WaitForSignalWorkflow.finish)
        await handle.result()

    it = handle.stream("main").fetch_records(wait_new_records=True)
    await collect_until_close(it)
    assert it.close is not None and it.close.status == CloseStatus.COMPLETED


@workflow.defn(
    name="CommitModeFailFirstTaskWorkflow", sandboxed=False, stream_commit_mode=True
)
class CommitModeFailFirstTaskWorkflow:
    @workflow.run
    async def run(self) -> None:
        global failed_task_attempts
        stream = await workflow.create_stream("main")
        await stream.publish("before local activity")
        await workflow.execute_local_activity(
            noop, start_to_close_timeout=timedelta(seconds=10)
        )
        if failed_task_attempts == 0:
            failed_task_attempts += 1
            raise RuntimeError("fail the workflow task")
        await stream.publish("after local activity")


@workflow.defn(stream_commit_mode=True)
class CommitModeWaitForSignalWorkflow:
    def __init__(self) -> None:
        self.done = False

    @workflow.run
    async def run(self) -> None:
        stream = await workflow.create_stream("main")
        await stream.publish("first")
        await workflow.wait_condition(lambda: self.done)
        await stream.publish("second")

    @workflow.signal
    def finish(self) -> None:
        self.done = True


def append_modes(records: list[Record]) -> list[WorkflowAppendMode]:
    return [r.workflow_append_mode.mode for r in records if r.workflow_append_mode]


async def test_commit_mode_delivers_only_the_committed_task_attempt(client: Client):
    """Native only: the records of a failed workflow task attempt of a workflow that declares
    commit mode are never delivered. The records of every activation of the attempt that commits,
    before and after a local activity resolves, are delivered with its completion, and the run's
    mode is recorded as commit.
    """
    global failed_task_attempts
    failed_task_attempts = 0
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[CommitModeFailFirstTaskWorkflow],
        activities=[noop],
    ):
        handle = await client.start_workflow(
            CommitModeFailFirstTaskWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        await handle.result()

    records = await raw_records(client, stream_ref(handle))
    stored = data(records)
    assert await raw_values(client, stored) == [
        ["before local activity"],
        ["after local activity"],
    ]
    assert {r.producer.workflow_task_attempt for r in stored} == {2}
    assert commit_attempt(records, stored[0]) == 2
    assert append_modes(records) == [WorkflowAppendMode.COMMIT]
    end = await read_to_end(handle.stream("main"))
    assert end.mode == StreamMode.COMMIT


async def test_commit_mode_records_are_visible_when_the_task_commits(client: Client):
    """Native only: the records of a workflow that declares commit mode are visible while it runs,
    once their task commits, and final, and the stream closes as completed when the run completes.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client, task_queue=task_queue, workflows=[CommitModeWaitForSignalWorkflow]
    ):
        handle = await client.start_workflow(
            CommitModeWaitForSignalWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        it = handle.stream("main").fetch_records(wait_new_records=True, type_hint=str)
        first = await asyncio.wait_for(it.__anext__(), timeout=10)
        assert isinstance(first, StreamRecord) and first.value == "first"
        assert first.status == RecordStatus.FINAL
        await handle.signal(CommitModeWaitForSignalWorkflow.finish)
        await handle.result()

    assert await read_values(handle.stream("main")) == ["first", "second"]
    end = await read_to_end(handle.stream("main"))
    assert end.close is not None and end.close.status == CloseStatus.COMPLETED


async def test_commit_mode_records_are_not_sent_again_on_replay(client: Client):
    """Native only: with no cache, every workflow task replays the workflow from the start; the
    records of earlier tasks of a workflow that declares commit mode are not sent again.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[CommitModeWaitForSignalWorkflow],
        max_cached_workflows=0,
    ):
        handle = await client.start_workflow(
            CommitModeWaitForSignalWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        it = handle.stream("main").fetch_records(wait_new_records=True)
        await asyncio.wait_for(it.__anext__(), timeout=10)
        await handle.signal(CommitModeWaitForSignalWorkflow.finish)
        await handle.result()

    assert await read_values(handle.stream("main")) == ["first", "second"]


@workflow.defn(name="RollingDeployWorkflow")
class SinkModeRollingDeployWorkflow:
    def __init__(self) -> None:
        self.done = False

    @workflow.run
    async def run(self) -> None:
        stream = await workflow.create_stream("main")
        await stream.publish("first")
        await workflow.wait_condition(lambda: self.done)
        await stream.publish("second")

    @workflow.signal
    def finish(self) -> None:
        self.done = True


@workflow.defn(name="RollingDeployWorkflow", stream_commit_mode=True)
class CommitModeRollingDeployWorkflow:
    def __init__(self) -> None:
        self.done = False

    @workflow.run
    async def run(self) -> None:
        stream = await workflow.create_stream("main")
        await stream.publish("first")
        await workflow.wait_condition(lambda: self.done)
        await stream.publish("second")

    @workflow.signal
    def finish(self) -> None:
        self.done = True


async def test_workers_that_disagree_on_append_mode_fail_the_workflow_task(
    client: Client,
):
    """Native only: as in a rolling deploy that adds a commit-mode declaration to a workflow, a
    worker with the sink-mode definition fixes the run's mode, and a workflow task on a worker with
    the commit-mode definition then fails, naming both modes.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client, task_queue=task_queue, workflows=[SinkModeRollingDeployWorkflow]
    ):
        handle = await client.start_workflow(
            SinkModeRollingDeployWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        it = handle.stream("main").fetch_records(wait_new_records=True)
        await asyncio.wait_for(it.__anext__(), timeout=10)

    async with Worker(
        client, task_queue=task_queue, workflows=[CommitModeRollingDeployWorkflow]
    ):
        await handle.signal("finish")
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


def test_worker_with_a_redis_default_backend_refuses_a_commit_mode_workflow(
    client: Client, redis_url: str
):
    """Redis: a worker whose client's default stream backend is not native refuses to register a
    workflow that declares commit mode, since only native supports it.
    """
    with pytest.raises(ValueError, match="commit mode"):
        Worker(
            redis_client(client, redis_url),
            task_queue=f"tq-{uuid.uuid4()}",
            workflows=[CommitModeWaitForSignalWorkflow],
        )


@workflow.defn
class PublishToBothBackendsWorkflow:
    @workflow.run
    async def run(self) -> None:
        native = await workflow.create_stream("main")
        redis = await workflow.create_stream("main", backend="redis")
        await native.publish("native")
        await redis.publish("redis")


async def test_stream_backends_configure_the_client_and_its_workers(
    client: Client, redis_url: str
):
    """Native and Redis: a client's stream backends configuration gives it and its workers each
    named backend, and the default it names, and a handle sends its calls to the backend it names,
    so streams with the same reference on two backends are different streams.
    """
    two = Client(
        client.service_client,
        namespace=client.namespace,
        stream_backends=StreamBackends([RedisStreamBackend(redis_url)], default=NATIVE),
    )
    assert isinstance(client_backend(two), NativeStreamBackend)
    assert isinstance(client_backend(two, "redis"), RedisStreamBackend)
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        two, task_queue=task_queue, workflows=[PublishToBothBackendsWorkflow]
    ):
        handle = await two.start_workflow(
            PublishToBothBackendsWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        await handle.result()
    assert await read_values(handle.stream("main")) == ["native"]
    assert await read_values(handle.stream("main", backend="redis")) == ["redis"]


@workflow.defn
class PublishOnceWorkflow:
    @workflow.run
    async def run(self) -> None:
        stream = await workflow.create_stream("main")
        await stream.publish("hello")


async def test_listed_backend_is_the_default_backend(client: Client, redis_url: str):
    """Redis: a client connected with a stream backend listed and no default named, and its
    workers, have that backend as their default; a client without it uses native, and does not see
    the Redis stream.
    """
    redis = await Client.connect(
        client.service_client.config.target_host,
        namespace=client.namespace,
        stream_backends=StreamBackends(
            [RedisStreamBackend(redis_url, retention=timedelta(hours=1))]
        ),
    )
    assert isinstance(client_backend(redis), RedisStreamBackend)
    assert isinstance(client_backend(client), NativeStreamBackend)
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(redis, task_queue=task_queue, workflows=[PublishOnceWorkflow]):
        handle = await redis.start_workflow(
            PublishOnceWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )
        await handle.result()
    assert await read_values(handle.stream("main")) == ["hello"]
    native = client.get_workflow_handle(
        handle.id, first_execution_run_id=handle.first_execution_run_id
    )
    assert await read_values(native.stream("main")) == []


def test_redis_backend_takes_its_namespace_from_its_client(
    client: Client, redis_url: str
):
    """Redis: a backend keys its streams by the namespace of the client it is bound to, so it
    refuses to be bound to a client of another namespace.
    """
    backend = RedisStreamBackend(redis_url)
    first = Client(
        client.service_client,
        namespace=client.namespace,
        stream_backends=StreamBackends([backend]),
    )
    assert client_backend(first) is backend
    other = Client(
        client.service_client,
        namespace=f"{client.namespace}-other",
        stream_backends=StreamBackends([backend]),
    )
    with pytest.raises(ValueError, match="namespace"):
        client_backend(other)


@activity.defn
async def publish_each(given: StreamHandleBase, n: int) -> None:
    stream = activity.get_stream_handle(given)
    for i in range(n):
        await stream.publish(i)


@activity.defn
async def publish_buffered(given: StreamHandleBase, n: int) -> None:
    stream = activity.get_stream_handle(given)
    async with stream.buffered() as buffered:
        for i in range(n):
            buffered.publish(i)


@workflow.defn
class ActivityPublishesWorkflow:
    @workflow.run
    async def run(self, buffered: bool) -> None:
        stream = await workflow.create_stream("main")
        await workflow.execute_activity(
            publish_buffered if buffered else publish_each,
            args=[stream, 50],
            start_to_close_timeout=timedelta(seconds=10),
        )
        await stream.publish(-1)


async def test_activity_publish_is_awaited_and_a_buffered_writer_batches(
    stream_client: Client,
):
    """Every backend: each awaited publish of an activity is one append, stored when it returns; a
    buffered writer sends fewer appends than values, and flushes when its block exits, so all are
    stored before anything the workflow publishes after the activity.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        stream_client,
        task_queue=task_queue,
        workflows=[ActivityPublishesWorkflow],
        activities=[publish_each, publish_buffered],
    ):
        each, buffered = [
            await stream_client.start_workflow(
                ActivityPublishesWorkflow.run,
                b,
                id=f"wf-{uuid.uuid4()}",
                task_queue=task_queue,
            )
            for b in [False, True]
        ]
        await each.result()
        await buffered.result()

    for handle, appends in [(each, 50), (buffered, None)]:
        assert await read_values(handle.stream("main"), type_hint=int) == [
            *range(50),
            -1,
        ]
        records = data(await raw_records(stream_client, stream_ref(handle)))
        seqs = {r.seq for r in records if r.producer.producer.startswith("activity/")}
        if appends:
            assert len(seqs) == appends
        else:
            assert len(seqs) < 50


async def test_buffered_writer_sends_nothing_until_flushed(stream_client: Client):
    """Every backend: a buffered writer's publish only buffers. Nothing is stored until flush,
    which stores everything buffered, in publish order, in appends of at most 1000 records; leaving
    its block flushes too.
    """
    async with stream_client.create_stream(f"s-{uuid.uuid4()}") as stream:
        async with stream.producer("p") as producer:
            async with producer.buffered() as buffered:
                buffered.publish(*range(2500))
                await asyncio.sleep(0.5)
                assert await read_values(stream, type_hint=int) == []
                await buffered.flush()
                assert await read_values(stream, type_hint=int) == list(range(2500))
                buffered.publish(-1)
                await asyncio.sleep(0.5)
                assert len(await read_values(stream, type_hint=int)) == 2500
            records = data(await raw_records(stream_client, stream.ref))
            assert len({r.seq for r in records}) == 4
            assert await read_values(stream, type_hint=int) == [*range(2500), -1]


@activity.defn
def publish_from_sync_activity(stream: StreamHandleBase) -> None:
    activity.get_sync_stream_handle(stream).publish("sync")


@workflow.defn
class SyncActivityWorkflow:
    @workflow.run
    async def run(self) -> None:
        stream = await workflow.create_stream("main")
        await workflow.execute_activity(
            publish_from_sync_activity,
            stream,
            start_to_close_timeout=timedelta(seconds=10),
        )


async def test_sync_activity_publishes_through_a_blocking_handle(stream_client: Client):
    """Every backend: a synchronous activity in a thread pool publishes through a blocking handle."""
    task_queue = f"tq-{uuid.uuid4()}"
    with concurrent.futures.ThreadPoolExecutor() as executor:
        async with Worker(
            stream_client,
            task_queue=task_queue,
            workflows=[SyncActivityWorkflow],
            activities=[publish_from_sync_activity],
            activity_executor=executor,
        ):
            handle = await stream_client.start_workflow(
                SyncActivityWorkflow.run,
                id=f"wf-{uuid.uuid4()}",
                task_queue=task_queue,
            )
            await handle.result()
    records = [r async for r in handle.stream("main").fetch_records(type_hint=str)]
    [record] = records
    assert isinstance(record, StreamRecord) and record.value == "sync"
    assert record.producer.producer.startswith("activity/")


@activity.defn
async def publish_too_large(given: StreamHandleBase) -> str:
    stream = activity.get_stream_handle(given)
    await stream.publish("x" * (2 * 1024 * 1024))
    return "done"


@workflow.defn
class PublishTooLargeWorkflow:
    @workflow.run
    async def run(self) -> str:
        stream = await workflow.create_stream("main")
        return await workflow.execute_activity(
            publish_too_large,
            stream,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )


async def test_failed_publish_fails_the_activity_attempt(stream_client: Client):
    """Every backend: a publish that the store refuses raises in the activity, which fails the
    attempt with the append's error unless the activity handles it.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        stream_client,
        task_queue=task_queue,
        workflows=[PublishTooLargeWorkflow],
        activities=[publish_too_large],
    ):
        with pytest.raises(WorkflowFailureError) as err:
            await stream_client.execute_workflow(
                PublishTooLargeWorkflow.run,
                id=f"wf-{uuid.uuid4()}",
                task_queue=task_queue,
            )
    cause = err.value.cause
    assert isinstance(cause, ActivityError)
    assert isinstance(cause.cause, ApplicationError)
    assert cause.cause.type == StreamTooLargeError.__name__


async def test_second_live_producer_with_the_same_id_raises(stream_client: Client):
    """Every backend: a process cannot open a second live producer with the same ID on the same
    stream, and a producer opened after the first finishes gets a higher epoch, even within the
    same millisecond.
    """
    handle = await start_owner(stream_client)
    first = handle.stream("main").producer("ui")
    with pytest.raises(RuntimeError, match="ui"):
        handle.stream("main").producer("ui")
    await first.finish()
    second = handle.stream("main").producer("ui")
    assert second.epoch > first.epoch
    other = (await start_owner(stream_client)).stream("main").producer("ui")
    await second.finish()
    await other.finish()


_RESTARTED_PRODUCER = """
import asyncio, sys
from temporalio.client import Client

async def main(target, namespace, workflow_id, first_run_id, redis_url):
    stream_backends = None
    if redis_url:
        from temporalio.contrib.redis_streams import RedisStreamBackend
        from temporalio.streams import StreamBackends
        stream_backends = StreamBackends([RedisStreamBackend(redis_url)])
    client = await Client.connect(
        target, namespace=namespace, stream_backends=stream_backends
    )
    stream = client.get_stream_handle(
        workflow_id=workflow_id, first_execution_run_id=first_run_id, name="main"
    )
    async with stream.producer("ui") as producer:
        await producer.publish("restarted")

asyncio.run(main(*sys.argv[1:]))
"""


async def test_restarted_client_producer_fences_its_earlier_incarnation(
    stream_client: Client, request: pytest.FixtureRequest
):
    """Every backend: a producer with the default epoch, opened in a new process with the same ID,
    fences the earlier process's producer, and a reader discards the earlier producer's records.
    """
    handle = await start_owner(stream_client)
    stream = handle.stream("main")
    earlier = stream.producer("ui")
    await earlier.publish("first")

    redis_url = "" if is_native(stream_client) else request.getfixturevalue("redis_url")
    restarted = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        _RESTARTED_PRODUCER,
        stream_client.service_client.config.target_host,
        stream_client.namespace,
        handle.id,
        handle.first_execution_run_id or "",
        redis_url,
    )
    assert await asyncio.wait_for(restarted.wait(), timeout=30) == 0

    with pytest.raises(EpochFencedError):
        await earlier.publish("stale")
    records = data(await raw_records(stream_client, stream_ref(handle)))
    assert await raw_values(stream_client, records) == [["first"], ["restarted"]]
    assert records[1].producer.epoch > records[0].producer.epoch
    assert await read_values(stream) == ["restarted"]


async def test_fetch_last_records(stream_client: Client):
    """Every backend: the last N records, after markers are left out, optionally of some topics."""
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        stream_client, task_queue=task_queue, workflows=[WaitForSignalWorkflow]
    ):
        handle = await stream_client.start_workflow(
            WaitForSignalWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )
        await handle.signal(WaitForSignalWorkflow.finish)
        await handle.result()
    stream = handle.stream("main")
    assert [r.value for r in await stream.fetch_last_records(1, type_hint=str)] == [
        "second"
    ]
    assert [r.value for r in await stream.fetch_last_records(10)] == [
        "first",
        "second",
    ]

    owner = (await start_owner(stream_client)).stream("main")
    async with owner.producer("restaurant") as producer:
        await producer.publish("x", topic="a")
        await producer.publish("y", topic="b")
        await producer.publish("z", topic="a")
    last = await owner.fetch_last_records(2, topics=["a"])
    assert [(r.topic, r.value) for r in last] == [("a", "x"), ("a", "z")]


@workflow.defn
class CloseThenWaitWorkflow:
    def __init__(self) -> None:
        self.done = False

    @workflow.run
    async def run(self) -> None:
        stream = await workflow.create_stream("main")
        await stream.publish("a")
        await stream.close(reason="done")
        await workflow.wait_condition(lambda: self.done)

    @workflow.signal
    def finish(self) -> None:
        self.done = True


@activity.defn
async def publish_and_close() -> str:
    stream = await activity.create_stream("main")
    await stream.publish("tool output")
    await stream.close(reason="done")
    return "done"


async def test_owner_closes_its_stream(stream_client: Client):
    """Every backend: a workflow closes its own stream while it runs, after its records, and the
    close is not replaced when the run ends; a Standalone Activity closes its own stream.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        stream_client,
        task_queue=task_queue,
        workflows=[CloseThenWaitWorkflow],
        activities=[publish_and_close],
    ):
        handle = await stream_client.start_workflow(
            CloseThenWaitWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )
        it = handle.stream("main").fetch_records(wait_new_records=True, type_hint=str)
        assert [
            r.value
            for r in await collect_until_close(it)
            if isinstance(r, StreamRecord)
        ] == ["a"]
        assert it.close is not None
        assert (it.close.status, it.close.reason) == (CloseStatus.COMPLETED, "done")
        await handle.signal(CloseThenWaitWorkflow.finish)
        await handle.result()

        tool_id = f"tool-{uuid.uuid4()}"
        assert (
            await stream_client.execute_activity(
                publish_and_close,
                id=tool_id,
                task_queue=task_queue,
                start_to_close_timeout=timedelta(seconds=10),
            )
            == "done"
        )

    end = await read_to_end(handle.stream("main"))
    assert end.close is not None and end.close.reason == "done"
    tool = stream_client.get_stream_handle(activity_id=tool_id, name="main")
    assert await read_values(tool) == ["tool output"]
    end = await read_to_end(tool)
    assert end.close is not None
    assert (end.close.status, end.close.reason) == (CloseStatus.COMPLETED, "done")
    assert end.mode == StreamMode.SINK


@activity.defn
async def tool() -> str:
    stream = await activity.create_stream("main")
    await stream.topic("progress", type=str).publish("tool started")
    await stream.publish("tool output")
    return "done"


@activity.defn
async def start_tool(tool_id: str) -> None:
    await activity.client().start_activity(
        tool,
        id=tool_id,
        task_queue=activity.info().task_queue,
        start_to_close_timeout=timedelta(seconds=10),
    )


@workflow.defn
class StartToolWorkflow:
    @workflow.run
    async def run(self, tool_id: str) -> None:
        await workflow.execute_activity(
            start_tool, tool_id, start_to_close_timeout=timedelta(seconds=10)
        )


async def test_client_reads_the_stream_of_a_standalone_activity_a_workflow_started(
    stream_client: Client,
):
    """Every backend: a tool call owns its stream: a workflow starts a Standalone Activity, which
    creates its own stream and publishes to it, and a client that refers to the stream before the
    activity starts reads it. Native only: the stream closes with the activity's outcome; on Redis
    nothing closes it.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    tool_id = f"tool-{uuid.uuid4()}"
    stream = stream_client.get_stream_handle(activity_id=tool_id, name="main")
    async with Worker(
        stream_client,
        task_queue=task_queue,
        workflows=[StartToolWorkflow],
        activities=[tool, start_tool],
    ):
        assert [r async for r in stream.fetch_records()] == []
        await stream_client.execute_workflow(
            StartToolWorkflow.run,
            tool_id,
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        assert await stream_client.get_activity_handle(tool_id).result() == "done"

    records = [r async for r in stream.fetch_records(type_hint=str)]
    assert [r.value for r in records if isinstance(r, StreamRecord)] == [
        "tool started",
        "tool output",
    ]
    first, second = records
    assert isinstance(first, StreamRecord) and isinstance(second, StreamRecord)
    assert first.topic == "progress"
    assert first.producer.producer == second.producer.producer
    if is_native(stream_client):
        end = await read_to_end(stream)
        assert end.close is not None and end.close.status == CloseStatus.COMPLETED


@workflow.defn
class GetOtherStreamsWorkflow:
    @workflow.run
    async def run(self, other_id: str) -> list[str]:
        streams = [
            workflow.get_stream_handle(workflow_id=other_id, name="main"),
            workflow.get_stream_handle(activity_id=other_id, name="main"),
            workflow.get_external_workflow_handle(other_id).stream("main"),
        ]
        observed: list[str] = []
        for stream in streams:
            try:
                await stream.publish("x")
                publish = "published"
            except ValueError:
                publish = "publish refused"
            observed.append(
                f"{type(stream).__name__}, {publish}, "
                + await tail_reads_not_implemented(stream)
            )
        try:
            workflow.get_stream_handle()
        except ValueError:
            observed.append("no owner refused")
        return observed


async def tail_reads_not_implemented(stream: Any) -> str:
    stream.fetch_records()
    stream.topic("t", type=str).fetch_records()
    try:
        await stream.fetch_last_records(1)
        return "tail read did not raise"
    except NotImplementedError:
        return "tail reads not implemented"


async def test_workflow_gets_handles_to_other_owners_streams(client: Client):
    """Any backend: workflow code gets a handle to another owner's stream, by owner ID or from an
    external workflow handle, to which it cannot publish, and which reads, except the last
    records, which replay cannot read again.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client, task_queue=task_queue, workflows=[GetOtherStreamsWorkflow]
    ):
        observed = await client.execute_workflow(
            GetOtherStreamsWorkflow.run,
            "other",
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
    assert observed == [
        "WorkflowStreamHandle, publish refused, tail reads not implemented"
    ] * 3 + ["no owner refused"]


async def test_native_switched_off_leaves_no_native_backend(
    client: Client, redis_url: str, monkeypatch: pytest.MonkeyPatch
):
    """With native streams switched off by TEMPORAL_STREAMS_ENABLE_NATIVE, a client and its
    workers have no native backend. A client with no other backend fails its first stream call,
    saying so, though a worker created from it runs workflows that use no streams. With another
    backend listed, it is the default, a handle that names native fails, and a worker refuses a
    workflow that declares commit mode, which needs native.
    """
    monkeypatch.setenv("TEMPORAL_STREAMS_ENABLE_NATIVE", "false")
    bare = Client(client.service_client, namespace=client.namespace)
    with pytest.raises(ValueError, match="TEMPORAL_STREAMS_ENABLE_NATIVE=false"):
        client_backend(bare)
    with pytest.raises(ValueError, match="TEMPORAL_STREAMS_ENABLE_NATIVE=false"):
        await bare.get_stream_handle(workflow_id="wf", name="main").exists()
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(bare, task_queue=task_queue, workflows=[NoStreamWorkflow]):
        await bare.execute_workflow(
            NoStreamWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )
    off = Client(
        client.service_client,
        namespace=client.namespace,
        stream_backends=StreamBackends([RedisStreamBackend(redis_url)]),
    )
    assert isinstance(client_backend(off), RedisStreamBackend)
    with pytest.raises(ValueError):
        client_backend(off, NATIVE)
    with pytest.raises(ValueError):
        await off.get_stream_handle(
            workflow_id="wf", name="main", backend=NATIVE
        ).exists()
    with pytest.raises(ValueError, match="commit mode"):
        Worker(off, task_queue="tq", workflows=[CommitModeWaitForSignalWorkflow])


@workflow.defn
class NoStreamWorkflow:
    @workflow.run
    async def run(self) -> None:
        pass


def test_native_switch_must_be_true_or_false(
    client: Client, monkeypatch: pytest.MonkeyPatch
):
    """TEMPORAL_STREAMS_ENABLE_NATIVE set to anything but true or false fails the client's stream
    calls, naming the variable.
    """
    monkeypatch.setenv("TEMPORAL_STREAMS_ENABLE_NATIVE", "yes")
    with pytest.raises(ValueError, match="TEMPORAL_STREAMS_ENABLE_NATIVE"):
        client_backend(client)


def test_workflow_reads_switch_must_be_true_or_false(
    client: Client, monkeypatch: pytest.MonkeyPatch
):
    """TEMPORAL_STREAMS_ENABLE_WORKFLOW_READS set to anything but true or false fails the creation
    of a worker with workflows, naming the variable.
    """
    monkeypatch.setenv("TEMPORAL_STREAMS_ENABLE_WORKFLOW_READS", "True")
    with pytest.raises(ValueError, match="TEMPORAL_STREAMS_ENABLE_WORKFLOW_READS"):
        Worker(client, task_queue="tq", workflows=[NoStreamWorkflow])
