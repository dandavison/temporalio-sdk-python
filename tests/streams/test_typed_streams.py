"""Tests of typed streams: a stream definition names a stream and declares its topics' types, and
every context binds it to a handle. On the wire a typed stream is the stream with the definition's
name, and a typed topic is the topic with its name, so typed and string-named access interoperate.

The end-to-end tests run against each stream backend and need what ``test_streams.py`` needs.
"""

from __future__ import annotations

import concurrent.futures
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest

from temporalio import activity, streams, workflow
from temporalio.client import Client
from temporalio.streams import (
    CloseStatus,
    ProducerIdentity,
    RecordStatus,
    StreamCursor,
    StreamHandleBase,
    StreamRecord,
    Withdrawal,
)
from temporalio.worker import Worker


@dataclass
class Turn:
    kind: str


@dataclass
class ToolEvent:
    tool: str


class AgentEvents(streams.Stream):
    turns = streams.Topic[Turn]()
    deltas = streams.Topic[str]()
    tool = streams.Topic[ToolEvent](name="tool_event")


class SessionEvents(streams.Stream, name="session-events"):
    notes = streams.Topic["Note"]()


@dataclass
class Note:
    text: str


def values(records: list[StreamRecord[Any] | Withdrawal]) -> list[Any]:
    return [r.value for r in records if isinstance(r, StreamRecord)]


def test_a_definition_names_its_stream_and_topics():
    assert AgentEvents.stream_name == "AgentEvents"
    assert SessionEvents.stream_name == "session-events"
    assert [(t.name, t.type) for t in AgentEvents.stream_topics.values()] == [
        ("turns", Turn),
        ("deltas", str),
        ("tool_event", ToolEvent),
    ]
    assert SessionEvents.notes.type is Note


def test_a_definition_attribute_must_be_a_topic_with_a_type():
    with pytest.raises(TypeError, match="turns"):

        class AnnotationOnly(streams.Stream):  # type: ignore[reportUnusedClass]
            turns: streams.Topic[Turn]  # type: ignore[reportUninitializedInstanceVariable]

    with pytest.raises(TypeError, match="turns"):

        class Untyped(streams.Stream):  # type: ignore[reportUnusedClass]
            turns = streams.Topic()  # type: ignore[var-annotated]

    with pytest.raises(TypeError, match="close"):

        class Reserved(streams.Stream):  # type: ignore[reportUnusedClass]
            close = streams.Topic[str]()  # type: ignore


def test_a_topic_matches_only_its_own_records():
    def record(topic: str, value: Any) -> StreamRecord[Any]:
        return StreamRecord(
            value=value,
            topic=topic,
            cursor=StreamCursor(b""),
            producer=ProducerIdentity("client/p", "", 0, 0, 0),
            status=RecordStatus.FINAL,
        )

    turn = record("turns", Turn("started"))
    matched = AgentEvents.turns.match(turn)
    assert matched is turn
    assert AgentEvents.tool.match(turn) is None
    assert AgentEvents.tool.match(record("tool_event", ToolEvent("x"))) is not None
    assert (
        AgentEvents.turns.match(
            Withdrawal(StreamCursor(b""), ProducerIdentity("client/p", "", 0, 0, 0))
        )
        is None
    )


@activity.defn
async def publish_typed(given: StreamHandleBase) -> None:
    events = AgentEvents(activity.get_stream_handle(given))
    await events.deltas.publish("a", "b")
    await events.tool.publish(ToolEvent("search"))


@workflow.defn
class TypedAgentWorkflow:
    def __init__(self) -> None:
        self.done = False

    @workflow.run
    async def run(self) -> None:
        events = await workflow.create_stream(AgentEvents)
        await events.turns.publish(Turn("started"))
        await workflow.execute_activity(
            publish_typed, events.handle, start_to_close_timeout=timedelta(seconds=10)
        )
        await workflow.wait_condition(lambda: self.done)

    @workflow.signal
    def finish(self) -> None:
        self.done = True


async def test_typed_publish_and_read_in_workflow_activity_and_client(
    stream_client: Client,
):
    """Every backend: a workflow, its activity and a client publish to a typed stream's topics; a
    topic read decodes as the topic's type, a whole-stream read decodes each record as its topic's
    type, and a string-named reader reads the same stream and topics by their wire names.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        stream_client,
        task_queue=task_queue,
        workflows=[TypedAgentWorkflow],
        activities=[publish_typed],
    ):
        handle = await stream_client.start_workflow(
            TypedAgentWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )
        events = handle.stream(AgentEvents)
        async for _ in events.tool.fetch_records(wait_new_records=True):
            break
        async with events.producer("ui") as ui:
            await ui.turns.publish(Turn("user"))
        await handle.signal(TypedAgentWorkflow.finish)
        await handle.result()

    assert values([r async for r in events.turns.fetch_records()]) == [
        Turn("started"),
        Turn("user"),
    ]
    assert values([r async for r in events.deltas.fetch_records()]) == ["a", "b"]
    whole = [r async for r in events.fetch_records()]
    assert values(whole) == [
        Turn("started"),
        "a",
        "b",
        ToolEvent("search"),
        Turn("user"),
    ]
    assert [t.value.kind for r in whole if (t := AgentEvents.turns.match(r))] == [
        "started",
        "user",
    ]

    by_name = handle.stream("AgentEvents")
    assert values(
        [
            r
            async for r in by_name.fetch_records(
                topics=["tool_event"], type_hint=ToolEvent
            )
        ]
    ) == [ToolEvent("search")]


async def test_typed_reader_reads_what_a_string_named_writer_wrote(
    stream_client: Client,
):
    """Every backend: a typed reader of a standalone stream decodes what a string-named producer
    wrote to a declared topic as the topic's type, and a record of an undeclared topic as the
    converter's default. A typed stream created with ``async with`` closes on exit.
    """
    stream_id = f"s-{uuid.uuid4()}"
    async with stream_client.create_stream(AgentEvents, stream_id=stream_id) as events:
        async with events.producer("typed") as typed:
            await typed.turns.publish(Turn("started"))
        async with events.handle.producer("untyped") as untyped:
            await untyped.publish({"tool": "search"}, topic="tool_event")
            await untyped.publish("free text", topic="notes")

    reader = stream_client.get_stream_handle(AgentEvents, stream_id=stream_id)
    it = reader.fetch_records()
    assert values([r async for r in it]) == [
        Turn("started"),
        ToolEvent("search"),
        "free text",
    ]
    assert it.close is not None and it.close.status == CloseStatus.COMPLETED


@activity.defn
def publish_typed_from_sync_activity(given: StreamHandleBase) -> None:
    activity.get_sync_stream_handle(given).topic(AgentEvents.turns).publish(
        Turn("sync")
    )


@workflow.defn
class TypedSyncActivityWorkflow:
    @workflow.run
    async def run(self) -> None:
        events = await workflow.create_stream(AgentEvents)
        await workflow.execute_activity(
            publish_typed_from_sync_activity,
            events.handle,
            start_to_close_timeout=timedelta(seconds=10),
        )


async def test_sync_activity_publishes_to_a_typed_topic(stream_client: Client):
    """Every backend: a synchronous activity publishes to a typed topic through its blocking
    handle.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    with concurrent.futures.ThreadPoolExecutor() as executor:
        async with Worker(
            stream_client,
            task_queue=task_queue,
            workflows=[TypedSyncActivityWorkflow],
            activities=[publish_typed_from_sync_activity],
            activity_executor=executor,
        ):
            handle = await stream_client.start_workflow(
                TypedSyncActivityWorkflow.run,
                id=f"wf-{uuid.uuid4()}",
                task_queue=task_queue,
            )
            await handle.result()
    events = handle.stream(AgentEvents)
    assert values([r async for r in events.turns.fetch_records()]) == [Turn("sync")]
