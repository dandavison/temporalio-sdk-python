"""The stream handles of workflow code, and what workflow code writes for the worker to store."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Generic, NoReturn, TypeVar

import temporalio.workflow
from temporalio.streams._backend import StreamClose
from temporalio.streams._handles import StreamHandleBase
from temporalio.streams._iterator import StreamCursor, StreamRecord
from temporalio.streams._record import AppendRecord

T = TypeVar("T")

_NO_READS = "Reading streams in workflow code is not implemented yet"


@dataclass(frozen=True)
class WorkflowStreamHandle(StreamHandleBase):
    """Workflow code's handle to a stream: one of its own streams, from
    :py:func:`temporalio.workflow.create_stream`, or a stream it was given, from
    :py:func:`temporalio.workflow.get_stream_handle`. Workflow code writes to its own streams and to
    standalone streams.

    ``await publish(...)`` hands the records to the worker. Once nothing else in the workflow can
    run, the worker appends everything that workflow code is waiting on, resolves the waits, and
    runs workflow code on, in the same activation, so a publish adds no workflow task. If the
    store fails the append, the workflow task fails and is retried; workflow code never sees the
    error, because replay would not see it. Replay does no I/O: its publishes resolve when nothing
    else can run, as they did when the code first ran.

    In a workflow that declares commit mode (``@workflow.defn(stream_commit_mode=True)``, native
    only), the records for its own streams travel in the workflow task's completion and become
    visible when the task commits, so a failed task attempt's records are never delivered; a
    publish resolves without waiting for the store. All of a task's records go in that one
    completion, which the server's request size limit bounds. Otherwise a failed task attempt may
    already have delivered records, which readers then withdraw (on native, using the server's
    commit records). In both modes the append is not a command and adds nothing to history.

    .. warning::
        This API is a prototype and will change.
    """

    async def publish(self, *values: Any, topic: str = "") -> None:
        """Convert the values and append them to the topic."""
        await temporalio.workflow._Runtime.current().workflow_stream_publish(
            self, topic, values
        )

    def topic(self, name: str, *, type: type[T]) -> WorkflowTopicHandle[T]:
        """A handle that publishes values of ``type`` to one topic."""
        return WorkflowTopicHandle(self, name, type)

    async def close(
        self, *, failure: BaseException | None = None, reason: str = ""
    ) -> None:
        """Close one of the workflow's own streams, as failed if ``failure`` is given and as
        completed otherwise, after the records published before it. Later publishes raise. Without
        it, on native the server closes the workflow's streams when the run chain ends, and on
        other backends the worker closes them after the server accepts the run's terminal
        completion.

        The close is written before the workflow task completes. If the task attempt then fails,
        its retry appends and closes again, which fail because the stream is closed, so the run's
        workflow tasks keep failing. Not supported in commit mode yet.
        """
        await temporalio.workflow._Runtime.current().workflow_stream_close(
            self, failure, reason
        )

    def fetch_records(self, **kwargs: Any) -> NoReturn:  # type:ignore[reportUnusedParameter]
        """Not implemented yet."""
        raise NotImplementedError(_NO_READS)

    async def fetch_last_records(
        self,
        n: int,  # type:ignore[reportUnusedParameter]
        **kwargs: Any,  # type:ignore[reportUnusedParameter]
    ) -> Sequence[StreamRecord[Any]]:
        """Not implemented yet."""
        raise NotImplementedError(_NO_READS)


class WorkflowTopicHandle(Generic[T]):
    """Workflow code's handle to one topic of a stream."""

    def __init__(self, stream: WorkflowStreamHandle, name: str, type: type[T]) -> None:
        """Create a topic handle. Use :py:meth:`WorkflowStreamHandle.topic` instead."""
        self._stream = stream
        self.name = name
        self.type = type

    async def publish(self, *values: T) -> None:
        """See :py:meth:`WorkflowStreamHandle.publish`."""
        await self._stream.publish(*values, topic=self.name)

    def fetch_records(
        self,
        *,
        after: StreamCursor | None = None,  # type:ignore[reportUnusedParameter]
        **kwargs: Any,  # type:ignore[reportUnusedParameter]
    ) -> NoReturn:
        """Not implemented yet."""
        raise NotImplementedError(_NO_READS)


@dataclass
class PendingStreamWrites:
    """What workflow code wrote to one stream that the worker has not yet passed to the backend:
    one of its own streams, by ``name``, or a standalone stream, by ``stream_id``. ``create`` is set
    when the worker must create the stream first.
    """

    backend: str
    name: str = ""
    stream_id: str = ""
    create: bool = False
    records: list[AppendRecord] = field(default_factory=list)
    close: StreamClose | None = None
