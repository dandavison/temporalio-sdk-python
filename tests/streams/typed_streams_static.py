# pyright: reportUnnecessaryTypeIgnoreComment=error
# mypy: warn-unused-ignores
"""Static types of typed streams, checked by ``poe lint`` (pyright, mypy and basedpyright). Nothing
here runs: each ``assert_type`` is the static type a context's call gives, and each ``type: ignore``
marks a call that must be a static error (an unneeded ignore is an error itself).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from typing_extensions import assert_type

from temporalio import activity, streams, workflow
from temporalio.client import Client, WorkflowHandle
from temporalio.streams import (
    PublishingTopicHandle,
    StreamHandleBase,
    StreamRecord,
    StreamRecordAsyncIterator,
    TopicReader,
    Withdrawal,
    WorkflowTopicHandle,
)
from tests.streams.test_typed_streams import AgentEvents, ToolEvent, Turn


async def definition() -> None:
    assert_type(AgentEvents.turns, streams.Topic[Turn])
    assert_type(AgentEvents.tool.type, type[ToolEvent])


async def view(events: AgentEvents) -> None:
    assert_type(events.turns, streams.TopicHandle[Turn])
    await events.turns.publish(Turn("started"), Turn("ended"))
    await events.turns.publish("started")  # type: ignore
    await events.undeclared.publish("x")  # type: ignore
    assert_type(events.turns.fetch_records(), StreamRecordAsyncIterator[Turn])
    assert_type(
        await events.tool.fetch_last_records(1), Sequence[StreamRecord[ToolEvent]]
    )
    assert_type(events.fetch_records(), StreamRecordAsyncIterator[Any])
    async for r in events.fetch_records():
        assert_type(r, StreamRecord[Any] | Withdrawal)
        if (t := AgentEvents.turns.match(r)) is not None:
            assert_type(t.value, Turn)
    await events.close()


async def client(client: Client, handle: WorkflowHandle[Any, Any]) -> None:
    assert_type(handle.stream(AgentEvents), AgentEvents)
    assert_type(client.get_stream_handle(AgentEvents, workflow_id="a"), AgentEvents)
    assert_type(await client.create_stream(AgentEvents, stream_id="s"), AgentEvents)
    async with client.create_stream(AgentEvents, stream_id="s") as events:
        assert_type(events, AgentEvents)
        async with events.producer("ui") as ui:
            assert_type(ui, AgentEvents)
    assert_type(
        client.get_stream_handle(workflow_id="a", name="n").topic(AgentEvents.turns),
        TopicReader[Turn],
    )


async def in_workflow(given: StreamHandleBase) -> None:
    assert_type(await workflow.create_stream(AgentEvents), AgentEvents)
    assert_type(workflow.get_stream_handle(AgentEvents, workflow_id="a"), AgentEvents)
    assert_type(
        workflow.get_external_workflow_handle("a").stream(AgentEvents), AgentEvents
    )
    assert_type(
        workflow.get_stream_handle(given).topic(AgentEvents.turns),
        WorkflowTopicHandle[Turn],
    )


async def in_activity(given: StreamHandleBase) -> None:
    assert_type(await activity.create_stream(AgentEvents), AgentEvents)
    assert_type(activity.get_stream_handle(AgentEvents, workflow_id="a"), AgentEvents)
    assert_type(AgentEvents(activity.get_stream_handle(given)), AgentEvents)
    assert_type(
        activity.get_stream_handle(given).topic(AgentEvents.turns),
        PublishingTopicHandle[Turn],
    )
    sync_turns = activity.get_sync_stream_handle(given).topic(AgentEvents.turns)
    sync_turns.publish(Turn("started"))
    sync_turns.publish("started")  # type: ignore
