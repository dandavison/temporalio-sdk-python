"""End-to-end tests of workflow code reading streams, against each stream backend. Each test says
which guarantee of the Notifier proposal it checks (G1 to G17), if any, and on which backends it
holds. G2 (spurious wakes), G4 (no notification writes) and G11 (the subscription cap) have no test
here.

These need redis-server on PATH, and the prototype's server, as the superproject's dev-server runs
it: pass ``--workflow-environment localhost:7233``.
"""

from __future__ import annotations

import asyncio
import base64
import subprocess
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import timedelta

import pytest

from temporalio import activity, workflow
from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.enums.v1 import EventType
from temporalio.api.history.v1 import HistoryEvent
from temporalio.api.workflowservice.v1 import DeleteWorkflowExecutionRequest
from temporalio.client import Client, WorkflowFailureError, WorkflowHandle
from temporalio.converter import DataConverter
from temporalio.service import RPCError
from temporalio.streams import (
    RecordStatus,
    StreamBackends,
    StreamCursor,
    StreamError,
    StreamHandle,
    StreamRecord,
)
from temporalio.worker import Replayer, Worker
from tests.streams.conftest import is_native, redis_client


@dataclass
class ReadSpec:
    """What a reader workflow reads: a named stream of a workflow or a Standalone Activity, or a
    standalone stream, until ``count`` values or the end.
    """

    workflow_id: str = ""
    activity_id: str = ""
    stream_id: str = ""
    name: str = "main"
    count: int = 0
    wait_new_records: bool = True
    topics: list[str] | None = None
    page_size: int | None = None
    hold_provisional: bool | None = None
    owner_wait_seconds: float = 3600
    close_wait_seconds: float = 60
    hold_timeout_seconds: float = 300
    after: str = ""
    redis_wait_lifetime_seconds: float = 7 * 24 * 3600
    # Report each value as its first character and length, for values too large to return.
    sizes: bool = False


@dataclass
class ReadOutcome:
    values: list[str] = field(default_factory=list)
    statuses: list[int] = field(default_factory=list)
    withdrawals: int = 0
    close: int | None = None
    owner_end: int | None = None
    mode: int = 0
    continue_as_new_suggested: bool = False
    cursor: str = ""
    error: str = ""


@workflow.defn
class StreamReaderWorkflow:
    @workflow.run
    async def run(self, spec: ReadSpec) -> ReadOutcome:
        if spec.stream_id:
            stream = workflow.get_stream_handle(stream_id=spec.stream_id)
        elif spec.workflow_id:
            stream = workflow.get_stream_handle(
                workflow_id=spec.workflow_id, name=spec.name
            )
        else:
            stream = workflow.get_stream_handle(
                activity_id=spec.activity_id, name=spec.name
            )
        outcome = ReadOutcome()
        try:
            it = stream.fetch_records(
                after=StreamCursor(base64.b64decode(spec.after))
                if spec.after
                else None,
                topics=spec.topics,
                page_size=spec.page_size,
                wait_new_records=spec.wait_new_records,
                hold_provisional=spec.hold_provisional,
                owner_wait=timedelta(seconds=spec.owner_wait_seconds),
                close_wait=timedelta(seconds=spec.close_wait_seconds),
                hold_timeout=timedelta(seconds=spec.hold_timeout_seconds),
                redis_wait_lifetime=timedelta(seconds=spec.redis_wait_lifetime_seconds),
                type_hint=str,
            )
            async for r in it:
                if isinstance(r, StreamRecord):
                    outcome.values.append(
                        f"{r.value[0]}:{len(r.value)}" if spec.sizes else r.value
                    )
                    outcome.statuses.append(r.status)
                else:
                    outcome.withdrawals += 1
                if spec.count and len(outcome.values) == spec.count:
                    break
        except (StreamError, ValueError) as err:
            outcome.error = type(err).__name__
            return outcome
        outcome.close = it.close.status if it.close else None
        outcome.owner_end = it.owner_end
        outcome.mode = it.mode
        outcome.continue_as_new_suggested = it.continue_as_new_suggested
        outcome.cursor = base64.b64encode(it.cursor).decode() if it.cursor else ""
        return outcome


@workflow.defn
class OwnerWorkflow:
    """Creates its stream named "main", publishes what it is signalled to, and completes when told
    to finish. Signalled to create "late", it creates that stream and publishes to it.
    """

    def __init__(self) -> None:
        self.pending: list[str] = []
        self.late: list[str] = []
        self.done = False

    @workflow.run
    async def run(self, first: list[str]) -> None:
        stream = await workflow.create_stream("main")
        await stream.publish(*first)
        while True:
            await workflow.wait_condition(
                lambda: bool(self.pending) or bool(self.late) or self.done
            )
            if self.pending:
                values, self.pending = self.pending, []
                await stream.publish(*values)
            if self.late:
                values, self.late = self.late, []
                await (await workflow.create_stream("late")).publish(*values)
            if self.done:
                return

    @workflow.signal
    def publish(self, value: str) -> None:
        self.pending.append(value)

    @workflow.signal
    def publish_late(self, value: str) -> None:
        self.late.append(value)

    @workflow.signal
    def finish(self) -> None:
        self.done = True


@workflow.defn(stream_commit_mode=True)
class CommitOwnerWorkflow:
    """As OwnerWorkflow, declaring commit mode."""

    def __init__(self) -> None:
        self.done = False

    @workflow.run
    async def run(self, first: list[str]) -> None:
        stream = await workflow.create_stream("main")
        await stream.publish(*first)
        await workflow.wait_condition(lambda: self.done)

    @workflow.signal
    def finish(self) -> None:
        self.done = True


def unique(name: str) -> str:
    return f"{name}-{uuid.uuid4()}"


async def start_reader(
    client: Client, task_queue: str, spec: ReadSpec
) -> WorkflowHandle[StreamReaderWorkflow, ReadOutcome]:
    return await client.start_workflow(
        StreamReaderWorkflow.run, spec, id=unique("reader"), task_queue=task_queue
    )


async def start_owner(
    client: Client, task_queue: str, first: list[str] = [], commit: bool = False
) -> WorkflowHandle:
    return await client.start_workflow(
        CommitOwnerWorkflow.run if commit else OwnerWorkflow.run,
        first,
        id=unique("owner"),
        task_queue=task_queue,
    )


async def events(handle: WorkflowHandle) -> list[HistoryEvent]:
    return [e async for e in handle.fetch_history_events()]


async def wait_for(
    condition: Callable[[], Awaitable[bool]], timeout: float = 20
) -> None:
    async def poll() -> None:
        while not await condition():
            await asyncio.sleep(0.2)

    await asyncio.wait_for(poll(), timeout)


async def wait_for_event(
    handle: WorkflowHandle, event_type: EventType.ValueType, count: int = 1
) -> None:
    async def seen() -> bool:
        return sum(e.event_type == event_type for e in await events(handle)) >= count

    await wait_for(seen)


def count(history: list[HistoryEvent], event_type: EventType.ValueType) -> int:
    return sum(e.event_type == event_type for e in history)


def nexus_operations(history: list[HistoryEvent]) -> list[str]:
    return [
        e.nexus_operation_scheduled_event_attributes.operation
        for e in history
        if e.event_type == EventType.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED
    ]


def containing(history: list[HistoryEvent], value: str) -> list[str]:
    """The events that contain the value, other than the run's result."""
    return [
        EventType.Name(e.event_type)
        for e in history
        if value.encode() in e.SerializeToString()
        and e.event_type != EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
    ]


def assert_one_wait_at_a_time(history: list[HistoryEvent]) -> None:
    """G3: a reader has at most one pending Wait, and each Wait completes once."""
    pending = 0
    for e in history:
        if e.event_type == EventType.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED:
            pending += 1
            assert pending == 1, "a second Wait was scheduled while one was pending"
        elif e.event_type == EventType.EVENT_TYPE_NEXUS_OPERATION_COMPLETED:
            pending -= 1
    assert pending == 0


async def append_to(stream: StreamHandle, *values: str, topic: str = "") -> None:
    async with stream.producer(unique("producer")) as producer:
        await producer.publish(*values, topic=topic)


async def append(
    owner: WorkflowHandle, *values: str, topic: str = "", name: str = "main"
) -> None:
    await append_to(owner.stream(name), *values, topic=topic)


def reader_worker(client: Client, task_queue: str, **kwargs: object) -> Worker:
    return Worker(
        client,
        task_queue=task_queue,
        # A worker whose backend is not native refuses a workflow that declares commit mode.
        workflows=[StreamReaderWorkflow, OwnerWorkflow]
        + ([CommitOwnerWorkflow] if is_native(client) else []),
        **kwargs,  # type: ignore[arg-type]
    )


async def test_reader_waits_for_records_and_reads_to_the_close(stream_client: Client):
    """G1, G3, G5, G6, every backend: a workflow reader at the stream's end waits, and is woken by
    a client's append and by the owner's own; it ends at the close when the owner completes. It
    has at most one Wait pending, and the records it read are not in its History.
    """
    task_queue = unique("tq")
    values = [unique("client"), unique("client"), unique("owner")]
    async with reader_worker(stream_client, task_queue):
        owner = await start_owner(stream_client, task_queue)
        reader = await start_reader(stream_client, task_queue, ReadSpec(owner.id))
        await wait_for_event(reader, EventType.EVENT_TYPE_NEXUS_OPERATION_STARTED)
        await append(owner, values[0], values[1])
        await asyncio.sleep(1)
        await owner.signal(OwnerWorkflow.publish, values[2])
        await asyncio.sleep(1)
        await owner.signal(OwnerWorkflow.finish)
        outcome = await reader.result()

    assert outcome.values == values
    assert outcome.close == 1
    history = await events(reader)
    assert_one_wait_at_a_time(history)
    assert [containing(history, v) for v in values] == [[], [], []]


async def test_reader_of_one_stream_is_not_woken_by_another_stream_of_the_owner(
    stream_client: Client,
):
    """Every backend: an owner's streams are separate, so an append to one does not wake a reader
    waiting on another.
    """
    task_queue = unique("tq")
    value = unique("value")
    async with reader_worker(stream_client, task_queue):
        owner = await start_owner(stream_client, task_queue)
        reader = await start_reader(
            stream_client, task_queue, ReadSpec(owner.id, count=1)
        )
        await wait_for_event(reader, EventType.EVENT_TYPE_NEXUS_OPERATION_STARTED)
        woken = count(
            await events(reader), EventType.EVENT_TYPE_NEXUS_OPERATION_COMPLETED
        )
        await append(owner, unique("other"), name="other")
        await asyncio.sleep(2)
        assert (
            count(await events(reader), EventType.EVENT_TYPE_NEXUS_OPERATION_COMPLETED)
            == woken
        )
        await append(owner, value)
        outcome = await reader.result()
    assert outcome.values == [value]


async def test_reader_waits_for_a_stream_its_owner_has_not_created(
    stream_client: Client,
):
    """Every backend: a reader of a stream that its owner has not created yet waits, without
    creating it, and reads it once the owner creates it.
    """
    task_queue = unique("tq")
    value = unique("value")
    async with reader_worker(stream_client, task_queue):
        owner = await start_owner(stream_client, task_queue)
        reader = await start_reader(
            stream_client, task_queue, ReadSpec(owner.id, name="late", count=1)
        )
        await wait_for_event(reader, EventType.EVENT_TYPE_NEXUS_OPERATION_STARTED)
        assert not await owner.stream("late").exists()
        await owner.signal(OwnerWorkflow.publish_late, value)
        outcome = await reader.result()
    assert outcome.values == [value]


async def test_workflow_reads_a_standalone_stream_to_its_close(stream_client: Client):
    """Every backend: a workflow reads a standalone stream, which a client creates, appends to
    and closes, and ends at its close.
    """
    task_queue = unique("tq")
    stream_id = unique("stream")
    values = [unique("value"), unique("value")]
    async with reader_worker(stream_client, task_queue):
        stream = await stream_client.create_stream(stream_id)
        reader = await start_reader(
            stream_client, task_queue, ReadSpec(stream_id=stream_id)
        )
        await wait_for_event(reader, EventType.EVENT_TYPE_NEXUS_OPERATION_STARTED)
        await append_to(stream, values[0])
        await asyncio.sleep(1)
        await append_to(stream, values[1])
        await stream.close()
        outcome = await reader.result()
    assert outcome.values == values
    assert outcome.close == 1
    assert outcome.mode == 1


async def test_waiting_reader_has_no_open_task_and_survives_a_worker_restart(
    stream_client: Client,
):
    """G9, every backend: a waiting reader has no open workflow task and does not poll; a record
    appended while no worker runs wakes it once a worker starts.
    """
    task_queue = unique("tq")
    async with reader_worker(stream_client, task_queue):
        owner = await start_owner(stream_client, task_queue)
        reader = await start_reader(
            stream_client, task_queue, ReadSpec(owner.id, count=1)
        )
        await wait_for_event(reader, EventType.EVENT_TYPE_NEXUS_OPERATION_STARTED)
    description = await reader.describe()
    assert not description.raw_description.HasField("pending_workflow_task")
    tasks = count(await events(reader), EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED)
    await asyncio.sleep(2)
    assert (
        count(await events(reader), EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED)
        == tasks
    )

    value = unique("value")
    await append(owner, value)
    async with reader_worker(stream_client, task_queue):
        outcome = await reader.result()
    assert outcome.values == [value]


@workflow.defn
class TwoStreamReaderWorkflow:
    @workflow.run
    async def run(self, owner_ids: list[str]) -> list[str]:
        values: list[str] = []
        for owner_id in owner_ids:
            async for r in workflow.get_stream_handle(
                workflow_id=owner_id, name="main"
            ).fetch_records(wait_new_records=True, type_hint=str):
                assert isinstance(r, StreamRecord)
                values.append(r.value)
                break
        return values


async def test_replay_rereads_sink_ranges_and_uses_recorded_commit_mode_reads(
    client: Client,
):
    """G6, G14, native: with no cache every workflow task replays. A read of a sink-mode stream
    keeps only its range in History and is read again on replay; a read of a commit-mode stream is
    a recorded Read operation, with its records in History. A run that reads only commit-mode
    streams replays offline from its exported History.
    """
    task_queue = unique("tq")
    sink_value, commit_value = unique("sink"), unique("commit")
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[TwoStreamReaderWorkflow, OwnerWorkflow, CommitOwnerWorkflow],
        max_cached_workflows=0,
    ):
        sink_owner = await start_owner(client, task_queue, [sink_value])
        commit_owner = await start_owner(
            client, task_queue, [commit_value], commit=True
        )
        both = await client.start_workflow(
            TwoStreamReaderWorkflow.run,
            [sink_owner.id, commit_owner.id],
            id=unique("reader"),
            task_queue=task_queue,
        )
        assert await both.result() == [sink_value, commit_value]
        commit_only = await client.start_workflow(
            TwoStreamReaderWorkflow.run,
            [commit_owner.id],
            id=unique("reader"),
            task_queue=task_queue,
        )
        assert await commit_only.result() == [commit_value]

    history = await events(both)
    assert containing(history, commit_value) == ["EVENT_TYPE_NEXUS_OPERATION_COMPLETED"]
    assert containing(history, sink_value) == []
    assert nexus_operations(history).count("Read") == 1
    await Replayer(workflows=[TwoStreamReaderWorkflow]).replay_workflow(
        await commit_only.fetch_history()
    )


async def delete_stream(
    client: Client, redis_url: str | None, owner: WorkflowHandle
) -> None:
    """Remove the owner's stream: on Redis every key, on native the owner's execution."""
    if redis_url:
        port = redis_url.rsplit(":", 1)[1]
        subprocess.run(
            ["redis-cli", "-p", port, "FLUSHALL"], check=True, capture_output=True
        )
        return
    await owner.terminate()
    await client.workflow_service.delete_workflow_execution(
        DeleteWorkflowExecutionRequest(
            namespace=client.namespace,
            workflow_execution=WorkflowExecution(workflow_id=owner.id),
        )
    )

    async def gone() -> bool:
        try:
            await owner.describe()
            return False
        except RPCError:
            return True

    await wait_for(gone, timeout=60)


@workflow.defn
class ReadThenWaitForSignalWorkflow:
    def __init__(self) -> None:
        self.go = False

    @workflow.run
    async def run(self, owner_id: str) -> str:
        async for r in workflow.get_stream_handle(
            workflow_id=owner_id, name="main"
        ).fetch_records(wait_new_records=True, type_hint=str):
            assert isinstance(r, StreamRecord)
            await workflow.wait_condition(lambda: self.go)
            return r.value
        raise AssertionError("unreachable")

    @workflow.signal
    def proceed(self) -> None:
        self.go = True


# The native case deletes the owner, which the dev server does about 20 s later.
@pytest.mark.timeout(120)
async def test_replay_whose_range_is_gone_fails_the_task_with_stream_range_gone(
    stream_client: Client, request: pytest.FixtureRequest
):
    """G7, every backend: a replay that needs a range that is no longer in the store fails the
    workflow task with an application failure of type STREAM_RANGE_GONE, and never continues with
    other data.
    """
    task_queue = unique("tq")
    redis_url = (
        None if is_native(stream_client) else request.getfixturevalue("redis_url")
    )
    async with Worker(
        stream_client,
        task_queue=task_queue,
        workflows=[ReadThenWaitForSignalWorkflow, OwnerWorkflow],
        max_cached_workflows=0,
    ):
        owner = await start_owner(stream_client, task_queue, [unique("value")])
        reader = await stream_client.start_workflow(
            ReadThenWaitForSignalWorkflow.run,
            owner.id,
            id=unique("reader"),
            task_queue=task_queue,
        )
        await wait_for_event(reader, EventType.EVENT_TYPE_MARKER_RECORDED)
        await delete_stream(stream_client, redis_url, owner)
        await reader.signal(ReadThenWaitForSignalWorkflow.proceed)

        async def range_gone() -> bool:
            return any(
                e.workflow_task_failed_event_attributes.failure.application_failure_info.type
                == "STREAM_RANGE_GONE"
                for e in await events(reader)
            )

        await wait_for(range_gone)
    await reader.terminate()


async def test_reader_is_told_to_continue_as_new_before_a_redis_range_can_expire(
    client: Client, redis_url: str
):
    """G8, Redis: an idle reader's iteration ends with continue_as_new_suggested set once its
    oldest range is older than half the Redis retention. (On native the threshold is half the
    namespace retention after the owner closes, too long to test.)
    """
    stream_client = redis_client(client, redis_url, retention=timedelta(seconds=6))
    task_queue = unique("tq")
    async with reader_worker(stream_client, task_queue):
        owner = await start_owner(stream_client, task_queue, [unique("value")])
        reader = await start_reader(stream_client, task_queue, ReadSpec(owner.id))
        outcome = await asyncio.wait_for(reader.result(), timeout=20)
    assert len(outcome.values) == 1
    assert outcome.continue_as_new_suggested
    assert outcome.close is None and outcome.owner_end is None


async def test_filtered_wait_completes_only_on_a_matching_topic(client: Client):
    """G10, native: a reader of one topic is not woken by an append to another."""
    task_queue = unique("tq")
    async with reader_worker(client, task_queue):
        owner = await start_owner(client, task_queue)
        await wait_for_event(owner, EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED)
        reader = await start_reader(
            client, task_queue, ReadSpec(owner.id, count=1, topics=["a"])
        )
        await wait_for_event(reader, EventType.EVENT_TYPE_NEXUS_OPERATION_STARTED)
        woken = count(
            await events(reader), EventType.EVENT_TYPE_NEXUS_OPERATION_COMPLETED
        )
        await append(owner, unique("b"), topic="b")
        await asyncio.sleep(2)
        assert (
            count(await events(reader), EventType.EVENT_TYPE_NEXUS_OPERATION_COMPLETED)
            == woken
        )
        value = unique("a")
        await append(owner, value, topic="a")
        outcome = await reader.result()
    assert outcome.values == [value]


async def test_a_workflow_task_spends_at_most_the_read_budget_on_reads(
    stream_client: Client,
):
    """G12, every backend: on a busy stream a workflow task makes at most 10 reads; the next task
    continues reading.
    """
    task_queue = unique("tq")
    values = [unique("value") for _ in range(30)]
    async with reader_worker(stream_client, task_queue):
        owner = await start_owner(stream_client, task_queue)
        await append(owner, *values)
        reader = await start_reader(
            stream_client,
            task_queue,
            ReadSpec(owner.id, wait_new_records=False, page_size=1),
        )
        outcome = await reader.result()
    assert outcome.values == values
    reads_per_task = [0]
    for e in await events(reader):
        if e.event_type == EventType.EVENT_TYPE_MARKER_RECORDED:
            reads_per_task[-1] += 1
        elif e.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
            reads_per_task.append(0)
    assert sum(reads_per_task) >= 30
    assert max(reads_per_task) <= 10


async def test_a_commit_mode_read_larger_than_the_payload_limit_returns_over_several_reads(
    client: Client,
):
    """G13, native: records of a commit-mode stream that total more than the namespace's payload
    size limit come back over several recorded reads, in order.
    """
    task_queue = unique("tq")
    values = [f"{i}" + "x" * 900_000 for i in range(4)]
    async with reader_worker(client, task_queue):
        owner = await start_owner(client, task_queue, commit=True)
        await wait_for_event(owner, EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED)
        for value in values:
            await append(owner, value)
        reader = await start_reader(
            client, task_queue, ReadSpec(owner.id, count=4, sizes=True)
        )
        outcome = await reader.result()
    assert outcome.values == [f"{v[0]}:{len(v)}" for v in values]
    assert nexus_operations(await events(reader)).count("Read") >= 2


@pytest.mark.parametrize("commit", [True, False])
async def test_reader_waits_for_the_owners_mode_then_reads_by_its_path(
    client: Client, commit: bool
):
    """G14, native: before the owner's first workflow task completes, the stream's mode is unknown
    and a reader waits instead of reading; then it reads by the mode's path: recorded Read
    operations for commit mode, the read local activity for sink mode.
    """
    owner_queue, reader_queue = unique("tq"), unique("tq")
    value = unique("value")
    async with reader_worker(client, reader_queue):
        owner = await start_owner(client, owner_queue, [value], commit=commit)
        reader = await start_reader(client, reader_queue, ReadSpec(owner.id, count=1))
        await wait_for_event(reader, EventType.EVENT_TYPE_NEXUS_OPERATION_STARTED)
        async with reader_worker(client, owner_queue):
            outcome = await reader.result()
    assert outcome.values == [value]
    assert outcome.mode == (2 if commit else 1)
    assert ("Read" in nexus_operations(await events(reader))) == commit


async def test_reader_raises_stream_mode_unknown_when_the_owner_never_runs(
    client: Client,
):
    """G14, native: a reader waits for the mode for at most owner_wait, then raises
    StreamModeUnknownError.
    """
    task_queue = unique("tq")
    async with reader_worker(client, task_queue):
        owner = await start_owner(client, unique("tq"))
        outcome = await (
            await start_reader(
                client, task_queue, ReadSpec(owner.id, owner_wait_seconds=3)
            )
        ).result()
    assert outcome.error == "StreamModeUnknownError"


hold_owner_failing = False


@workflow.defn(sandboxed=False)
class FailingOwnerWorkflow:
    """Creates its stream; when signalled, publishes, then fails its workflow task while
    hold_owner_failing is set. Its first workflow task completes, so the stream's mode is known.
    """

    def __init__(self) -> None:
        self.go = False

    @workflow.run
    async def run(self, value: str) -> None:
        stream = await workflow.create_stream("main")
        await workflow.wait_condition(lambda: self.go)
        await stream.publish(value)
        if hold_owner_failing:
            raise RuntimeError("fail the workflow task")

    @workflow.signal
    def proceed(self) -> None:
        self.go = True


@workflow.defn
class HoldReaderWorkflow:
    def __init__(self) -> None:
        self.continue_requested = False

    @workflow.run
    async def run(self, owner_id: str, after: str, earlier: list[str]) -> list[str]:
        it = workflow.get_stream_handle(
            workflow_id=owner_id, name="main"
        ).fetch_records(
            after=StreamCursor(base64.b64decode(after)) if after else None,
            wait_new_records=True,
            type_hint=str,
        )
        values: list[str] = []

        async def read() -> None:
            async for r in it:
                assert isinstance(r, StreamRecord)
                assert r.status == RecordStatus.FINAL
                values.append(r.value)
                if len(values) == 2:
                    return

        reading = asyncio.create_task(read())
        await workflow.wait_condition(lambda: reading.done() or self.continue_requested)
        if not reading.done():
            workflow.continue_as_new(
                args=[
                    owner_id,
                    base64.b64encode(it.cursor).decode() if it.cursor else "",
                    earlier + values,
                ]
            )
        return earlier + values

    @workflow.signal
    def please_continue(self) -> None:
        self.continue_requested = True


async def test_held_record_holds_back_later_records_across_continue_as_new(
    client: Client,
):
    """G15, native: a workflow reader gets only final workflow-task records by default, in stream
    order: a record of a workflow task attempt that has not committed holds back every later
    record, so the reader yields nothing before it continues as new, and the iterator's cursor does
    not pass it, so the new run reads it again. The failed attempts' records are never yielded:
    the owner's record comes after the client's, from the attempt that commits.
    """
    global hold_owner_failing
    hold_owner_failing = True
    task_queue = unique("tq")
    owner_value, client_value = unique("owner"), unique("client")
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[FailingOwnerWorkflow, HoldReaderWorkflow],
    ):
        owner = await client.start_workflow(
            FailingOwnerWorkflow.run,
            owner_value,
            id=unique("owner"),
            task_queue=task_queue,
        )
        await wait_for_event(owner, EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED)
        reader = await client.start_workflow(
            HoldReaderWorkflow.run,
            args=[owner.id, "", []],
            id=unique("reader"),
            task_queue=task_queue,
        )
        await owner.signal(FailingOwnerWorkflow.proceed)
        await wait_for_event(owner, EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED)
        await append(owner, client_value)
        await wait_for_event(reader, EventType.EVENT_TYPE_NEXUS_OPERATION_STARTED)
        await reader.signal(HoldReaderWorkflow.please_continue)
        await wait_for_event(
            client.get_workflow_handle(reader.id, run_id=reader.first_execution_run_id),
            EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
        )
        hold_owner_failing = False
        result = await client.get_workflow_handle(reader.id).result()
    assert result == [client_value, owner_value]


async def test_reader_that_continues_as_new_while_waiting_reads_and_waits_again(
    stream_client: Client,
):
    """Every backend: a reader that continues as new with a Wait pending abandons that Wait; the
    new run reads after the iterator's cursor, waits again, and is woken by the next append.
    HoldReaderWorkflow returns after reading two records in one run.
    """
    task_queue = unique("tq")
    first, second, third = unique("first"), unique("second"), unique("third")
    async with Worker(
        stream_client,
        task_queue=task_queue,
        workflows=[OwnerWorkflow, HoldReaderWorkflow],
    ):
        owner = await start_owner(stream_client, task_queue)
        await wait_for_event(owner, EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED)
        await append(owner, first)
        reader = await stream_client.start_workflow(
            HoldReaderWorkflow.run,
            args=[owner.id, "", []],
            id=unique("reader"),
            task_queue=task_queue,
        )
        await wait_for_event(reader, EventType.EVENT_TYPE_NEXUS_OPERATION_STARTED)
        first_run = stream_client.get_workflow_handle(
            reader.id, run_id=reader.first_execution_run_id
        )
        await reader.signal(HoldReaderWorkflow.please_continue)
        await wait_for_event(
            first_run, EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW
        )
        latest = stream_client.get_workflow_handle(reader.id)
        await wait_for_event(latest, EventType.EVENT_TYPE_NEXUS_OPERATION_STARTED)
        await append(owner, second)
        await append(owner, third)
        result = await latest.result()

    assert result == [first, second, third]
    abandoned = await events(first_run)
    assert count(
        abandoned, EventType.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED
    ) == 1 + count(abandoned, EventType.EVENT_TYPE_NEXUS_OPERATION_COMPLETED)


async def test_hold_provisional_is_refused_on_redis(client: Client, redis_url: str):
    """Redis: without commit records a hold could last indefinitely, so a reader that asks to
    hold provisional records is refused.
    """
    stream_client = redis_client(client, redis_url)
    task_queue = unique("tq")
    async with reader_worker(stream_client, task_queue):
        owner = await start_owner(stream_client, task_queue)
        outcome = await (
            await start_reader(
                stream_client, task_queue, ReadSpec(owner.id, hold_provisional=True)
            )
        ).result()
    assert outcome.error == "ValueError"


async def test_reader_of_an_owner_that_never_appears_raises_stream_not_found(
    stream_client: Client,
):
    """G16, every backend: a read of an owner, or a standalone stream, that does not exist retries
    with backoff for owner_wait, then raises StreamNotFoundError.
    """
    task_queue = unique("tq")
    async with reader_worker(stream_client, task_queue):
        for spec in [
            ReadSpec(unique("missing"), owner_wait_seconds=3),
            ReadSpec(stream_id=unique("missing"), owner_wait_seconds=3),
        ]:
            outcome = await (
                await start_reader(stream_client, task_queue, spec)
            ).result()
            assert outcome.error == "StreamNotFoundError"


async def test_reader_tells_an_owner_that_ended_without_closing_its_stream(
    client: Client, redis_url: str
):
    """G17, Redis: after a terminate the worker writes no close, so the reader ends with close
    unset and owner_end set to the owner's terminal state, after close_wait.
    """
    stream_client = redis_client(client, redis_url)
    task_queue = unique("tq")
    value = unique("value")
    async with reader_worker(stream_client, task_queue):
        owner = await start_owner(stream_client, task_queue, [value])
        reader = await start_reader(
            stream_client, task_queue, ReadSpec(owner.id, close_wait_seconds=2)
        )
        await wait_for_event(reader, EventType.EVENT_TYPE_NEXUS_OPERATION_STARTED)
        await owner.terminate()
        outcome = await reader.result()
    assert outcome.values == [value]
    assert outcome.close is None
    assert outcome.owner_end == 4


@activity.defn
async def tool() -> None:
    stream = await activity.create_stream("main")
    await stream.publish("tool started", "tool output")
    await stream.close()


async def test_workflow_reads_the_stream_of_a_standalone_activity(
    stream_client: Client,
):
    """Every backend: a workflow reads the stream of a Standalone Activity, which is always
    sink-mode, to its close.
    """
    task_queue = unique("tq")
    tool_id = unique("tool")
    async with Worker(
        stream_client,
        task_queue=task_queue,
        workflows=[StreamReaderWorkflow],
        activities=[tool],
    ):
        reader = await start_reader(
            stream_client, task_queue, ReadSpec(activity_id=tool_id)
        )
        await stream_client.start_activity(
            tool,
            id=tool_id,
            task_queue=task_queue,
            start_to_close_timeout=timedelta(seconds=10),
        )
        outcome = await reader.result()
    assert outcome.values == ["tool started", "tool output"]
    assert outcome.close == 1
    assert outcome.mode == 1


def append_without_notify(redis_url: str, owner: WorkflowHandle, value: str) -> None:
    """Append a record to a Redis stream directly, as a producer that bypasses the backend does,
    so no Notify follows.
    """
    payload = DataConverter.default.payload_converter.to_payload(value)
    subprocess.run(
        [
            "redis-cli",
            "-p",
            redis_url.rsplit(":", 1)[1],
            "-x",
            "XADD",
            f"stream:{{default/workflow/{owner.id}}}:{owner.first_execution_run_id}:main:log",
            "*",
            "kind",
            "data",
            "topic",
            "",
            "producer",
            "client/raw",
            "run",
            "",
            "wt",
            "0",
            "wta",
            "0",
            "epoch",
            "0",
            "seq",
            "0",
            "payload",
        ],
        input=payload.SerializeToString(),
        check=True,
        capture_output=True,
    )


async def test_redis_reader_reads_a_record_whose_notify_was_lost_when_its_wait_expires(
    client: Client, redis_url: str
):
    """G1, Redis: a record appended with no Notify does not wake a waiting reader, but the
    reader's Wait expires after redis_wait_lifetime, and it reads the record then.
    """
    stream_client = redis_client(client, redis_url)
    task_queue = unique("tq")
    first, second = unique("first"), unique("second")
    async with reader_worker(stream_client, task_queue):
        owner = await start_owner(stream_client, task_queue, [first])
        reader = await start_reader(
            stream_client,
            task_queue,
            ReadSpec(owner.id, count=2, redis_wait_lifetime_seconds=3),
        )
        await wait_for_event(reader, EventType.EVENT_TYPE_NEXUS_OPERATION_STARTED)
        append_without_notify(redis_url, owner, second)
        outcome = await asyncio.wait_for(reader.result(), timeout=20)
    assert outcome.values == [first, second]


@workflow.defn
class ReadOwnStreamWorkflow:
    @workflow.run
    async def run(self) -> None:
        stream = await workflow.create_stream("main")
        async for _ in stream.fetch_records():
            pass


async def test_workflow_reads_switched_off(client: Client):
    """Native: with workflow reads switched off in the stream backends configuration, a workflow's
    read fails the workflow task, and the worker registers no read local activity.
    """
    off = Client(
        client.service_client,
        namespace=client.namespace,
        stream_backends=StreamBackends(workflow_reads=False),
    )
    task_queue = unique("tq")
    async with Worker(
        off, task_queue=task_queue, workflows=[ReadOwnStreamWorkflow]
    ) as worker:
        assert worker._activity_worker is None
        handle = await off.start_workflow(
            ReadOwnStreamWorkflow.run, id=unique("wf"), task_queue=task_queue
        )
        await wait_for_event(handle, EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED)
        failure = next(
            e.workflow_task_failed_event_attributes.failure
            for e in await events(handle)
            if e.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED
        )
        assert "workflow reads" in failure.message.lower()
        await handle.terminate()
    with pytest.raises(WorkflowFailureError):
        await handle.result()
