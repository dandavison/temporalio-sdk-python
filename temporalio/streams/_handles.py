"""Stream handles for clients and activities."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import timedelta
from types import TracebackType
from typing import Any, Generic, Protocol, TypeVar

import temporalio.activity
import temporalio.api.failure.v1
import temporalio.client
import temporalio.converter
from temporalio.streams._backend import CloseStatus, StreamClose
from temporalio.streams._iterator import (
    StreamCursor,
    StreamRecord,
    StreamRecordAsyncIterator,
    fetch_last_records,
)
from temporalio.streams._publisher import Publisher, claim_producer
from temporalio.streams._record import ActivityProducer, ClientProducer
from temporalio.streams._ref import StreamRef
from temporalio.streams.backend import client_backend

T = TypeVar("T")


class StreamReader(Protocol):
    """Reads a stream. Shared by every handle that reads."""

    def topic(self, name: str, *, type: type[T]) -> TopicReader[T]:
        """A reader of one topic, whose values decode as ``type``."""
        ...

    def fetch_records(
        self,
        *,
        after: StreamCursor | None = None,
        topics: Sequence[str] | None = None,
        page_size: int | None = None,
        wait_new_records: bool = False,
        type_hint: type | None = None,
    ) -> StreamRecordAsyncIterator[Any]:
        """Iterate over the stream's records after ``after``, or from the beginning.

        Args:
            after: A record's cursor, or an iterator's, to resume after.
            topics: The topics to read; all if unset.
            page_size: The most records each read returns, markers included.
            wait_new_records: Wait for new records at the stream's end, and end only at its close.
                Otherwise the iteration ends at the stream's current end.
            type_hint: The type to decode values as. Without it, values decode as the payload
                converter's default.
        """
        ...

    async def fetch_last_records(
        self,
        n: int,
        *,
        topics: Sequence[str] | None = None,
        type_hint: type | None = None,
    ) -> Sequence[StreamRecord[Any]]:
        """The last ``n`` records of the topics, or of all, oldest first, leaving out withdrawn
        records. A record's status is judged from the records read, so a workflow record whose task
        completed before the stream existed can be provisional here, though final when read from
        the beginning.
        """
        ...


class TopicReader(Generic[T]):
    """Reads one topic of a stream, decoding values as ``T``."""

    def __init__(self, reader: StreamReader, name: str, type: type[T]) -> None:
        """Create a topic reader. Use ``topic`` on a stream handle instead."""
        self._reader = reader
        self._name = name
        self._type = type

    def fetch_records(
        self,
        *,
        after: StreamCursor | None = None,
        page_size: int | None = None,
        wait_new_records: bool = False,
    ) -> StreamRecordAsyncIterator[T]:
        """See :py:meth:`StreamReader.fetch_records`."""
        return self._reader.fetch_records(
            after=after,
            topics=[self._name],
            page_size=page_size,
            wait_new_records=wait_new_records,
            type_hint=self._type,
        )


class _Publishing(Protocol):
    def publish(
        self, value: Any, *, topic: str = "", force_flush: bool = False
    ) -> None: ...


class PublishingTopicHandle(Generic[T]):
    """Publishes values of type ``T`` to one topic."""

    def __init__(self, publisher: _Publishing, name: str, type: type[T]) -> None:
        """Create a topic handle. Use ``topic`` on a producer or stream handle instead."""
        self._publisher = publisher
        self.name = name
        self.type = type

    def publish(self, value: T, *, force_flush: bool = False) -> None:
        """Publish a value to the topic. See the publisher's ``publish``."""
        self._publisher.publish(value, topic=self.name, force_flush=force_flush)


class StreamHandle:
    """A client's handle to a stream: reads, and named producers.

    .. warning::
        This API is a prototype and will change.
    """

    def __init__(self, client: temporalio.client.Client, ref: StreamRef) -> None:
        """Create a handle. Use ``stream()`` on a workflow or activity handle, or
        :py:meth:`temporalio.client.Client.get_stream_handle`, instead.
        """
        self._client = client
        self._ref = ref

    def topic(self, name: str, *, type: type[T]) -> TopicReader[T]:
        """See :py:meth:`StreamReader.topic`."""
        return TopicReader(self, name, type)

    def fetch_records(
        self,
        *,
        after: StreamCursor | None = None,
        topics: Sequence[str] | None = None,
        page_size: int | None = None,
        wait_new_records: bool = False,
        type_hint: type | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> StreamRecordAsyncIterator[Any]:
        """See :py:meth:`StreamReader.fetch_records`. ``rpc_metadata`` and ``rpc_timeout`` apply to
        each read's RPCs on native; other backends ignore them.
        """
        return StreamRecordAsyncIterator(
            client_backend(self._client),
            self._ref,
            self._client.data_converter,
            after=after,
            topics=topics,
            page_size=page_size,
            wait_new_records=wait_new_records,
            type_hint=type_hint,
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
        )

    async def fetch_last_records(
        self,
        n: int,
        *,
        topics: Sequence[str] | None = None,
        type_hint: type | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> Sequence[StreamRecord[Any]]:
        """See :py:meth:`StreamReader.fetch_last_records`."""
        return await fetch_last_records(
            client_backend(self._client),
            self._ref,
            self._client.data_converter,
            n,
            topics=topics,
            type_hint=type_hint,
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
        )

    def producer(
        self,
        id: str,
        *,
        epoch: int | None = None,
        batch_interval: timedelta = timedelta(milliseconds=100),
    ) -> StreamProducer:
        """A producer that appends as ``id``.

        A producer's state is kept on the stream, on native for the stream's life, so use one ID
        per writer process or session, not one per request.

        Args:
            id: The producer's ID. Raises ``RuntimeError`` if this process has a live producer
                with the ID on the stream: one that has not finished, failed, or exited its
                ``async with`` block.
            epoch: The producer's epoch, from 0 to 2^53-1. A higher epoch fences the ID's lower
                ones. By default the current time in milliseconds, but never less than one more
                than the last epoch this process used for the ID, so a restarted process fences its
                earlier incarnation, unless the clock went back across the restart, in which case
                its appends fail with :py:class:`temporalio.streams.EpochFencedError`. A client
                that keeps a durable counter can pass it here.
            batch_interval: How long a published value may wait in the buffer.
        """
        epoch, release = claim_producer(self._ref, id, epoch)
        return StreamProducer(
            id,
            epoch,
            Publisher(
                client_backend(self._client),
                self._ref,
                ClientProducer(id, epoch),
                self._client.data_converter.payload_converter,
                self._client.data_converter,
                batch_interval,
                release,
            ),
        )


class StreamProducer:
    """A client's named producer. An async context manager that flushes on exit.

    ``publish`` buffers; the producer sends at most one append per ``batch_interval``, or sooner
    when the buffer reaches an append's size limits or a publish forces a flush. A failed append,
    after the backend's retries, drops what was buffered and is raised from the next ``flush``,
    ``publish`` or ``finish``, and from every call after; open a new producer to continue. Values
    still buffered when the process exits are lost.

    .. warning::
        This API is a prototype and will change.
    """

    def __init__(self, id: str, epoch: int, publisher: Publisher) -> None:
        """Create a producer. Use :py:meth:`StreamHandle.producer` instead."""
        self.id = id
        self.epoch = epoch
        self._publisher = publisher

    async def __aenter__(self) -> StreamProducer:
        """Return self."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Flush, and release the producer's ID."""
        try:
            await self.flush()
        finally:
            self._publisher.release()

    def publish(
        self, value: Any, *, topic: str = "", force_flush: bool = False
    ) -> None:
        """Convert the value and buffer it for appending. Must be called in the event loop."""
        self._publisher.publish(value, topic, force_flush)

    def topic(self, name: str, *, type: type[T]) -> PublishingTopicHandle[T]:
        """A handle that publishes values of ``type`` to one topic."""
        return PublishingTopicHandle(self, name, type)

    async def flush(self) -> None:
        """Wait until everything published so far is stored."""
        await self._publisher.flush()

    async def finish(self) -> None:
        """Flush, then record that this producer will write no more, and release its ID. It does
        not close the stream.
        """
        await self._publisher.finish()


class ActivityStreamHandle:
    """An activity's handle to a stream: its workflow's stream for a workflow or local activity,
    and its own for a Standalone Activity.

    ``publish`` buffers as :py:class:`StreamProducer` does, appending as the activity attempt,
    whose epoch is its attempt stamp. The worker flushes before completing the attempt, and a
    failed flush fails the attempt; values still buffered when the worker process crashes are lost.

    A workflow activity names its workflow's stream by its workflow's current run, which identifies
    the stream only in the chain's first run.

    .. warning::
        This API is a prototype and will change.
    """

    def __init__(
        self,
        client: temporalio.client.Client,
        info: temporalio.activity.Info,
        payload_converter: temporalio.converter.PayloadConverter,
    ) -> None:
        """Create a handle. Use :py:func:`temporalio.activity.stream` instead."""
        self._standalone = not info.workflow_id
        ref = (
            StreamRef(
                activity_id=info.activity_id, activity_run_id=info.activity_run_id or ""
            )
            if self._standalone
            else StreamRef(info.workflow_id or "", info.workflow_run_id or "")
        )
        self._reader = StreamHandle(client, ref)
        self._publisher = Publisher(
            client_backend(client),
            ref,
            ActivityProducer(info),
            payload_converter,
            client.data_converter,
            timedelta(milliseconds=100),
        )
        self._data_converter = client.data_converter

    def topic(self, name: str, *, type: type[T]) -> PublishingTopicHandle[T]:
        """A handle that publishes values of ``type`` to one topic."""
        return PublishingTopicHandle(self, name, type)

    def publish(
        self, value: Any, *, topic: str = "", force_flush: bool = False
    ) -> None:
        """Convert the value and buffer it for appending."""
        self._publisher.publish(value, topic, force_flush)

    async def flush(self) -> None:
        """Wait until everything published so far is stored."""
        await self._publisher.flush()

    async def close(
        self, *, failure: BaseException | None = None, reason: str = ""
    ) -> None:
        """Flush, then close the Standalone Activity's own stream, as failed if ``failure`` is
        given, and as completed otherwise. On native the server closes the stream when the activity
        ends, if the activity has not; on other backends nothing does.
        """
        if not self._standalone:
            raise RuntimeError(
                "Only a Standalone Activity owns its stream; a workflow activity's stream is its "
                "workflow's"
            )
        close = StreamClose(CloseStatus.COMPLETED, reason=reason)
        if failure:
            proto = temporalio.api.failure.v1.Failure()
            await self._data_converter.encode_failure(failure, proto)
            close = StreamClose(CloseStatus.FAILED, proto, reason)
        await self._publisher.close(close)

    def fetch_records(
        self,
        *,
        after: StreamCursor | None = None,
        topics: Sequence[str] | None = None,
        page_size: int | None = None,
        wait_new_records: bool = False,
        type_hint: type | None = None,
    ) -> StreamRecordAsyncIterator[Any]:
        """See :py:meth:`StreamReader.fetch_records`."""
        return self._reader.fetch_records(
            after=after,
            topics=topics,
            page_size=page_size,
            wait_new_records=wait_new_records,
            type_hint=type_hint,
        )

    async def fetch_last_records(
        self,
        n: int,
        *,
        topics: Sequence[str] | None = None,
        type_hint: type | None = None,
    ) -> Sequence[StreamRecord[Any]]:
        """See :py:meth:`StreamReader.fetch_last_records`."""
        return await self._reader.fetch_last_records(
            n, topics=topics, type_hint=type_hint
        )
