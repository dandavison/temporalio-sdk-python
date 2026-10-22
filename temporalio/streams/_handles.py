"""Stream handles for clients and activities, and what every handle shares."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Coroutine, Generator, Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from types import TracebackType
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar

import temporalio.activity
import temporalio.api.failure.v1
import temporalio.converter
from temporalio.streams._backend import CloseStatus, StreamClose
from temporalio.streams._iterator import (
    ClientRecordIterator,
    StreamCursor,
    StreamRecord,
    StreamRecordAsyncIterator,
    fetch_last_records,
)
from temporalio.streams._publisher import Appender, BufferedStreamWriter, claim_producer
from temporalio.streams._record import ActivityProducer, AppendRecord, ClientProducer
from temporalio.streams._ref import StreamRef

if TYPE_CHECKING:
    import temporalio.client

T = TypeVar("T")
H = TypeVar("H", bound="StreamHandleBase")


@dataclass(frozen=True)
class StreamHandleBase:
    """What every stream handle holds: its stream's reference and backend. A handle is a value, so
    it can be passed in workflow and activity inputs, and through continue-as-new, and each kind of
    handle can be made from another with the ``get_stream_handle`` function of the context that
    uses it.
    """

    backend: str
    """The name of the backend that stores the stream, among the client's stream backends."""
    workflow_id: str = ""
    first_run_id: str = ""
    activity_id: str = ""
    activity_run_id: str = ""
    name: str = ""
    """The stream's name among its owner's streams."""
    stream_id: str = ""
    """Set for a standalone stream, which has no owner."""

    def __post_init__(self) -> None:
        """Check the reference."""
        self.ref  # noqa: B018

    @property
    def ref(self) -> StreamRef:
        """The stream's reference."""
        return StreamRef(
            workflow_id=self.workflow_id,
            first_run_id=self.first_run_id,
            activity_id=self.activity_id,
            activity_run_id=self.activity_run_id,
            name=self.name,
            stream_id=self.stream_id,
        )

    @classmethod
    def of(cls: type[H], backend: str, ref: StreamRef) -> H:
        """The handle of this kind to the stream ``ref`` on ``backend``."""
        return cls(
            backend=backend,
            workflow_id=ref.workflow_id,
            first_run_id=ref.first_run_id,
            activity_id=ref.activity_id,
            activity_run_id=ref.activity_run_id,
            name=ref.name,
            stream_id=ref.stream_id,
        )


def handle_ref(
    stream: StreamHandleBase | None,
    *,
    workflow_id: str | None,
    first_execution_run_id: str | None,
    activity_id: str | None,
    activity_run_id: str | None,
    stream_id: str | None,
    name: str | None,
    backend: str | None,
) -> tuple[str | None, StreamRef]:
    """The backend, None for the default, and reference that ``get_stream_handle``'s arguments
    name.
    """
    if stream:
        if any((workflow_id, activity_id, stream_id, name, backend)):
            raise ValueError(
                "Pass either a stream handle or the stream's IDs, not both"
            )
        return stream.backend, stream.ref
    return backend, StreamRef(
        workflow_id=workflow_id or "",
        first_run_id=first_execution_run_id or "",
        activity_id=activity_id or "",
        activity_run_id=activity_run_id or "",
        name=name or "",
        stream_id=stream_id or "",
    )


class Creating(Generic[H]):
    """A stream being created: await it for the stream's handle, or use it as an async context
    manager, which closes the stream on exit, as failed if the block raises and as completed
    otherwise. The close is best effort: nothing closes the stream if the process stops first.
    """

    def __init__(
        self,
        create: Callable[[], Awaitable[H]],
        close: Callable[[H, BaseException | None], Awaitable[None]],
    ) -> None:
        """Wrap a create. Use a ``create_stream`` function instead."""
        self._create = create
        self._close = close
        self._handle: H | None = None

    def __await__(self) -> Generator[Any, None, H]:
        """Create the stream and return its handle."""
        return self._create().__await__()

    async def __aenter__(self) -> H:
        """Create the stream and return its handle."""
        self._handle = await self._create()
        return self._handle

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the stream."""
        assert self._handle
        await self._close(self._handle, exc_value)


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
    async def publish(self, *values: Any, topic: str = "") -> None: ...


class PublishingTopicHandle(Generic[T]):
    """Publishes values of type ``T`` to one topic."""

    def __init__(self, publisher: _Publishing, name: str, type: type[T]) -> None:
        """Create a topic handle. Use ``topic`` on a producer or stream handle instead."""
        self._publisher = publisher
        self.name = name
        self.type = type

    async def publish(self, *values: T) -> None:
        """Publish values to the topic. See the publisher's ``publish``."""
        await self._publisher.publish(*values, topic=self.name)


@dataclass(frozen=True)
class StreamHandle(StreamHandleBase):
    """A client's handle to a stream: reads, named producers, and, for a standalone stream, close.

    A handle from a client method is bound to that client; one passed in as a value is bound with
    :py:meth:`temporalio.client.Client.get_stream_handle`, and in an activity uses the activity's
    client.

    .. warning::
        This API is a prototype and will change.
    """

    def _bound(self, client: temporalio.client.Client) -> StreamHandle:
        object.__setattr__(self, "_client", client)
        return self

    @property
    def client(self) -> temporalio.client.Client:
        """The client the handle's calls go through."""
        client = self.__dict__.get("_client")
        if not client:
            client = temporalio.activity.client()
        return client

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
        return ClientRecordIterator(
            _backend(self.client, self.backend),
            self.ref,
            self.client.data_converter,
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
            _backend(self.client, self.backend),
            self.ref,
            self.client.data_converter,
            n,
            topics=topics,
            type_hint=type_hint,
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
        )

    async def exists(self) -> bool:
        """Whether the stream has been created, by its creator or its first append. A stream that
        its creator created explicitly exists before any append; a stream created by its first
        append does not exist until then.
        """
        resp = await _backend(self.client, self.backend).read(self.ref, max_records=1)
        return resp.exists

    def producer(self, id: str, *, epoch: int | None = None) -> StreamProducer:
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
        """
        epoch, release = claim_producer(self.backend, self.ref, id, epoch)
        return StreamProducer(
            id,
            epoch,
            Appender(
                _backend(self.client, self.backend),
                self.ref,
                ClientProducer(id, epoch),
                self.client.data_converter,
            ),
            self.client.data_converter.payload_converter,
            release,
        )

    async def close(
        self, *, failure: BaseException | None = None, reason: str = ""
    ) -> None:
        """Close a standalone stream, as failed if ``failure`` is given and as completed otherwise.
        Any caller may close a standalone stream: the creator's right to close is not enforced. A
        close of a closed stream succeeds if it is the same close. An owner's stream is closed by
        its owner.
        """
        if not self.stream_id:
            raise RuntimeError("Only a standalone stream can be closed by a client")
        close = await _stream_close(self.client.data_converter, failure, reason)
        await _backend(self.client, self.backend).close(self.ref, None, 0, close)


class StreamProducer:
    """A client's named producer. An async context manager that releases its ID on exit.

    Each ``publish`` appends at once, as one producer with the next seq, and waits until the store
    has the records; concurrent calls are made one at a time. A failed append, after the backend's
    retries, is raised, and from every later call too; open a new producer to continue.
    ``buffered`` gives a writer that buffers in memory until flushed.

    .. warning::
        This API is a prototype and will change.
    """

    def __init__(
        self,
        id: str,
        epoch: int,
        appender: Appender,
        payload_converter: temporalio.converter.PayloadConverter,
        release: Callable[[], None],
    ) -> None:
        """Create a producer. Use :py:meth:`StreamHandle.producer` instead."""
        self.id = id
        self.epoch = epoch
        self.payload_converter = payload_converter
        self._appender = appender
        self._release = release

    async def __aenter__(self) -> StreamProducer:
        """Return self."""
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        """Release the producer's ID."""
        self._release()

    async def publish(self, *values: Any, topic: str = "") -> None:
        """Convert the values and append them to the topic."""
        await self._append(_records(self.payload_converter, values, topic))

    def topic(self, name: str, *, type: type[T]) -> PublishingTopicHandle[T]:
        """A handle that publishes values of ``type`` to one topic."""
        return PublishingTopicHandle(self, name, type)

    def buffered(self) -> BufferedStreamWriter:
        """A writer that buffers what it publishes, and appends it through this producer when
        flushed.
        """
        return BufferedStreamWriter(self)

    async def finish(self) -> None:
        """Record that this producer will write no more, and release its ID. It does not close the
        stream.
        """
        try:
            await self._appender.finish()
        finally:
            self._release()

    async def _append(self, records: Sequence[AppendRecord]) -> None:
        await self._appender.append(records)


@dataclass(frozen=True)
class ActivityStreamHandle(StreamHandleBase):
    """An activity's handle to a stream: from a handle passed to the activity, with
    :py:func:`temporalio.activity.get_stream_handle`, or to a Standalone Activity's own stream,
    from :py:func:`temporalio.activity.create_stream`.

    Each ``publish`` appends at once, as the activity attempt, whose epoch is its attempt stamp,
    and waits until the store has the records; one append is in flight at a time per stream and
    attempt. Nothing is buffered, so nothing is flushed when the attempt completes. ``buffered``
    gives a writer that buffers in memory until the activity flushes it.

    .. warning::
        This API is a prototype and will change.
    """

    @property
    def payload_converter(self) -> temporalio.converter.PayloadConverter:
        """The activity's payload converter."""
        return temporalio.activity.payload_converter()

    async def publish(self, *values: Any, topic: str = "") -> None:
        """Convert the values and append them to the topic."""
        await self._append(_records(self.payload_converter, values, topic))

    def topic(self, name: str, *, type: type[T]) -> PublishingTopicHandle[T]:
        """A handle that publishes values of ``type`` to one topic."""
        return PublishingTopicHandle(self, name, type)

    def buffered(self) -> BufferedStreamWriter:
        """A writer that buffers what it publishes, and appends it through this handle when
        flushed. The activity must flush it, or use it with ``async with``, before the attempt
        completes.
        """
        return BufferedStreamWriter(self)

    async def close(
        self, *, failure: BaseException | None = None, reason: str = ""
    ) -> None:
        """Close the stream, as failed if ``failure`` is given and as completed otherwise: a
        Standalone Activity's own stream, as the activity's attempt, or a standalone stream. On
        native the server closes an activity's streams when the activity ends, if the activity has
        not; on other backends nothing does.
        """
        await _close_from_activity(
            self, _activity_appender(self), temporalio.activity.info(), failure, reason
        )

    def fetch_records(self, **kwargs: Any) -> StreamRecordAsyncIterator[Any]:
        """See :py:meth:`StreamReader.fetch_records`."""
        return self._reader().fetch_records(**kwargs)

    async def fetch_last_records(
        self, n: int, **kwargs: Any
    ) -> Sequence[StreamRecord[Any]]:
        """See :py:meth:`StreamReader.fetch_last_records`."""
        return await self._reader().fetch_last_records(n, **kwargs)

    async def _append(self, records: Sequence[AppendRecord]) -> None:
        await _activity_appender(self).append(records)

    def _reader(self) -> StreamHandle:
        return StreamHandle.of(self.backend, self.ref)._bound(
            temporalio.activity.client()
        )


@dataclass(frozen=True)
class SyncActivityStreamHandle(StreamHandleBase):
    """A synchronous activity's handle to a stream, from
    :py:func:`temporalio.activity.get_sync_stream_handle`: the blocking counterpart of
    :py:class:`ActivityStreamHandle`. Available in activities that run in a thread pool, not in a
    process pool.

    .. warning::
        This API is a prototype and will change.
    """

    def publish(self, *values: Any, topic: str = "") -> None:
        """Convert the values, append them to the topic, and return once the store has them."""
        records = _records(temporalio.activity.payload_converter(), values, topic)
        _run_on_worker_loop(_activity_appender(self).append(records))

    def close(self, *, failure: BaseException | None = None, reason: str = "") -> None:
        """See :py:meth:`ActivityStreamHandle.close`."""
        _run_on_worker_loop(
            _close_from_activity(
                self,
                _activity_appender(self),
                temporalio.activity.info(),
                failure,
                reason,
            )
        )


async def _close_from_activity(
    handle: StreamHandleBase,
    appender: Appender,
    info: temporalio.activity.Info,
    failure: BaseException | None,
    reason: str,
) -> None:
    close = await _stream_close(appender.data_converter, failure, reason)
    if handle.stream_id:
        await appender.backend.close(handle.ref, None, 0, close)
        return
    if info.workflow_id or handle.activity_id != info.activity_id:
        raise RuntimeError(
            "An activity can close only its own stream, as a Standalone Activity, or a standalone "
            "stream"
        )
    owner = appender.producer
    assert isinstance(owner, ActivityProducer)
    await appender.call(
        lambda seq: appender.backend.close(handle.ref, owner, seq, close)
    )


def _run_on_worker_loop(coro: Coroutine[Any, Any, None]) -> None:
    loop = temporalio.activity._Context.current().worker_loop
    if not loop:
        coro.close()
        raise RuntimeError(
            "Streams are available in synchronous activities only when they run in a thread pool"
        )
    asyncio.run_coroutine_threadsafe(coro, loop).result()


def _activity_appender(handle: StreamHandleBase) -> Appender:
    """The appender of the current activity attempt for the stream, shared by every handle to it in
    the attempt, so that its appends take consecutive seqs one at a time.
    """
    context = temporalio.activity._Context.current()
    key = (handle.backend, handle.ref)
    appender = context.stream_appenders.get(key)
    if not appender:
        client = context.stream_client
        if not client:
            raise RuntimeError(
                "Streams are available in activities only when they have a client: in tests, pass "
                "one to ActivityEnvironment"
            )
        appender = context.stream_appenders[key] = Appender(
            _backend(client, handle.backend),
            handle.ref,
            ActivityProducer(context.info()),
            client.data_converter,
        )
    return appender


def _records(
    payload_converter: temporalio.converter.PayloadConverter,
    values: Sequence[Any],
    topic: str,
) -> list[AppendRecord]:
    return [AppendRecord(topic, payload_converter.to_payload(v)) for v in values]


async def _stream_close(
    data_converter: temporalio.converter.DataConverter,
    failure: BaseException | None,
    reason: str,
) -> StreamClose:
    if not failure:
        return StreamClose(CloseStatus.COMPLETED, reason=reason)
    proto = temporalio.api.failure.v1.Failure()
    await data_converter.encode_failure(failure, proto)
    return StreamClose(CloseStatus.FAILED, proto, reason)


def _backend(client: temporalio.client.Client, name: str) -> Any:
    from temporalio.streams.backend import client_backend

    return client_backend(client, name)
