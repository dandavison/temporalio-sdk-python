"""The stream handle of workflow code."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Generic, NoReturn, TypeVar

import temporalio.workflow
from temporalio.streams._iterator import StreamCursor, StreamRecord

T = TypeVar("T")

_NO_READS = "Reading streams in workflow code is not implemented yet"


class WorkflowStreamHandle:
    """Workflow code's handle to its own stream.

    ``publish`` buffers in the workflow. In a workflow that declares commit mode
    (``@workflow.defn(stream_commit_mode=True)``, native only), the records of a workflow task
    travel in its completion and become visible when the task commits, so a failed task attempt's
    records are never delivered; all of a task's records go in that one completion, which the
    server's request size limit bounds. Otherwise the worker appends the records at the end of each
    activation, before sending its completion, so a failed task attempt may already have delivered
    records, which readers then withdraw (on native, using the server's commit records). In both
    modes the append is not a command and adds nothing to history, and replay does not send the
    records again. If the append fails, the workflow task fails and is retried.

    .. warning::
        This API is a prototype and will change.
    """

    def publish(self, value: Any, *, topic: str = "") -> None:
        """Convert the value and buffer it for the end of the activation, or for the task
        completion in commit mode. Raises if the workflow closed its stream.
        """
        temporalio.workflow._Runtime.current().workflow_stream_append(topic, value)

    def topic(self, name: str, *, type: type[T]) -> WorkflowTopicHandle[T]:
        """A handle that publishes values of ``type`` to one topic."""
        return WorkflowTopicHandle(self, name, type)

    def close(self, *, failure: BaseException | None = None, reason: str = "") -> None:
        """Close the stream, as failed if ``failure`` is given, and as completed otherwise. The
        worker writes the close after the activation's records. Later publishes raise. Without it,
        on native the server closes the stream when the run chain ends, and on other backends the
        worker closes it after the server accepts the run's terminal completion.

        The close is written before the workflow task completes. If the task attempt then fails,
        its retry appends and closes again, which fail because the stream is closed, so the run's
        workflow tasks keep failing. Not supported in commit mode yet.
        """
        temporalio.workflow._Runtime.current().workflow_stream_close(failure, reason)

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
    """Workflow code's handle to one topic of its own stream."""

    def __init__(self, stream: WorkflowStreamHandle, name: str, type: type[T]) -> None:
        """Create a topic handle. Use :py:meth:`WorkflowStreamHandle.topic` instead."""
        self._stream = stream
        self.name = name
        self.type = type

    def publish(self, value: T) -> None:
        """See :py:meth:`WorkflowStreamHandle.publish`."""
        self._stream.publish(value, topic=self.name)

    def fetch_records(
        self,
        *,
        after: StreamCursor | None = None,  # type:ignore[reportUnusedParameter]
        **kwargs: Any,  # type:ignore[reportUnusedParameter]
    ) -> NoReturn:
        """Not implemented yet."""
        raise NotImplementedError(_NO_READS)
