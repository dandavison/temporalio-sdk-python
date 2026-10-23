"""Stream definitions: a stream's name and its topics' types, bound to a handle in any context."""

from __future__ import annotations

import builtins
import inspect
import sys
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, ForwardRef, Generic, TypeVar, overload

from typing_extensions import Self

from temporalio.streams._iterator import (
    StreamCursor,
    StreamRecord,
    StreamRecordAsyncIterator,
    Withdrawal,
)

if TYPE_CHECKING:
    from temporalio.streams._handles import StreamHandleBase, StreamProducer

T = TypeVar("T")
D = TypeVar("D", bound="Stream")


class Topic(Generic[T]):
    """A topic of a stream definition: its name, by default the attribute's, and the type of its
    values. On a definition class it is this metadata; on a definition bound to a handle it is a
    :py:class:`TopicHandle`. See :py:class:`Stream`.
    """

    def __init__(self, *, name: str = "") -> None:
        """Declare a topic, named ``name`` on the wire instead of by its attribute."""
        self.name = name
        self._type: Any = None
        self._owner: builtins.type[Stream] | None = None

    @property
    def type(self) -> builtins.type[T]:
        """The type that the topic's values decode as."""
        if self._owner and isinstance(self._type, (str, ForwardRef)):
            ref = (
                self._type
                if isinstance(self._type, str)
                else self._type.__forward_arg__
            )
            module = vars(sys.modules[self._owner.__module__])
            self._type = eval(ref, module, dict(vars(self._owner)))
        return self._type

    @overload
    def __get__(self, obj: None, owner: builtins.type[Any]) -> Topic[T]: ...

    @overload
    def __get__(self, obj: Stream, owner: builtins.type[Any]) -> TopicHandle[T]: ...

    def __get__(
        self, obj: Stream | None, owner: builtins.type[Any]
    ) -> Topic[T] | TopicHandle[T]:
        """The topic on a definition class; its handle on a bound definition."""
        return self if obj is None else TopicHandle(obj.handle, self)

    def match(self, item: StreamRecord[Any] | Withdrawal) -> StreamRecord[T] | None:
        """``item`` if it is a record of this topic, else None: narrows a record of a whole-stream
        read to this topic's type.
        """
        if isinstance(item, StreamRecord) and item.topic == self.name:
            return item
        return None

    def _bind(self, owner: builtins.type[Stream], attr: str) -> None:
        args = getattr(getattr(self, "__orig_class__", None), "__args__", ())
        if not args:
            raise TypeError(
                f"{owner.__name__}.{attr} has no type: declare it as "
                f"{attr} = streams.Topic[T]()"
            )
        self.name = self.name or attr
        self._type = args[0]
        self._owner = owner


class TopicHandle(Generic[T]):
    """One topic of a stream definition bound to a handle: publishes values of type ``T``, and
    reads them decoded as ``T``, through the handle. Each call needs the handle to support it:
    a client's stream handle reads, and its producer publishes; a workflow's or an activity's
    handle does both.

    .. warning::
        This API is a prototype and will change.
    """

    def __init__(self, handle: Any, topic: Topic[T]) -> None:
        """Bind a topic. Use the topic's attribute on a bound definition instead."""
        self._handle = handle
        self.name = topic.name
        self.type = topic.type

    async def publish(self, *values: T) -> None:
        """Publish values to the topic, with the handle's ``publish``."""
        if not hasattr(self._handle, "publish"):
            raise TypeError(
                f"A {type(self._handle).__name__} does not publish: a client publishes through "
                "a producer, from producer(id)"
            )
        await self._handle.publish(*values, topic=self.name)

    def fetch_records(
        self,
        *,
        after: StreamCursor | None = None,
        page_size: int | None = None,
        wait_new_records: bool = False,
        **kwargs: Any,
    ) -> StreamRecordAsyncIterator[T]:
        """The topic's records, with the handle's ``fetch_records``, whose other options
        ``kwargs`` passes on.
        """
        return self._handle.fetch_records(
            after=after,
            topics=[self.name],
            page_size=page_size,
            wait_new_records=wait_new_records,
            type_hint=self.type,
            **kwargs,
        )

    async def fetch_last_records(
        self, n: int, **kwargs: Any
    ) -> Sequence[StreamRecord[T]]:
        """The topic's last ``n`` records, with the handle's ``fetch_last_records``."""
        return await self._handle.fetch_last_records(
            n, topics=[self.name], type_hint=self.type, **kwargs
        )


class Stream:
    """A stream definition: a stream's name and its topics' types, so that a stream can be part of
    a workflow's or an activity's contract. Subclass it, and declare each topic as a class
    attribute::

        class AgentEvents(streams.Stream):            # or streams.Stream, name="agent-events"
            turns = streams.Topic[Turn]()
            deltas = streams.Topic[str]()
            tool = streams.Topic[ToolEvent](name="tool_event")

    The stream's name defaults to the class's, and a topic's to its attribute's. A definition is
    metadata only: its stream is the stream with its name, and its topics are the topics with
    their names, so typed and string-named access to a stream interoperate. A standalone stream has
    no name; a definition of one supplies only its topics' types.

    Pass the class wherever a stream name is accepted (``workflow.create_stream(AgentEvents)``,
    ``handle.stream(AgentEvents)``, ``client.get_stream_handle(AgentEvents, workflow_id=...)``,
    and the like) to get an instance bound to that context's handle, or bind a handle yourself
    with ``AgentEvents(handle)``. A bound definition's topic attributes are
    :py:class:`TopicHandle` s, and its methods act on the whole stream. Pass :py:attr:`handle`, not
    the bound definition, in workflow and activity inputs.

    .. warning::
        This API is a prototype and will change.
    """

    stream_name: ClassVar[str] = ""
    """The stream's name."""
    stream_topics: ClassVar[Mapping[str, Topic[Any]]] = {}
    """The topics, by attribute name."""

    def __init_subclass__(cls, name: str = "", **kwargs: Any) -> None:
        """Read the definition's topics."""
        super().__init_subclass__(**kwargs)
        try:
            annotated = list(inspect.get_annotations(cls))
        except NameError:
            annotated = ["an annotated attribute"]
        if annotated:
            raise TypeError(
                f"{cls.__name__}.{annotated[0]}: declare a topic as "
                f"{annotated[0]} = streams.Topic[T](), not with an annotation"
            )
        topics = dict(cls.stream_topics)
        for attr, value in vars(cls).items():
            if isinstance(value, Topic) or getattr(value, "__origin__", None) is Topic:
                if hasattr(Stream, attr):
                    raise TypeError(
                        f"{cls.__name__}.{attr}: the name is taken by streams.Stream"
                    )
                if not isinstance(value, Topic):
                    raise TypeError(
                        f"{cls.__name__}.{attr} is not a topic: declare it as "
                        f"{attr} = streams.Topic[T]()"
                    )
                value._bind(cls, attr)
                topics[attr] = value
        cls.stream_name = name or cls.__name__
        cls.stream_topics = topics

    def __init__(self, handle: StreamHandleBase | StreamProducer) -> None:
        """Bind the definition to a handle of any context, or to a client's producer."""
        self.handle: Any = handle
        """The handle the definition is bound to, for what a definition does not declare, such as
        publishing to a topic it has no attribute for."""

    def fetch_records(
        self,
        *,
        after: StreamCursor | None = None,
        topics: Sequence[str | Topic[Any]] | None = None,
        page_size: int | None = None,
        wait_new_records: bool = False,
        **kwargs: Any,
    ) -> StreamRecordAsyncIterator[Any]:
        """The stream's records, with the handle's ``fetch_records``, each value decoded as its
        topic's type, or as the payload converter's default for a topic the definition does not
        declare. Narrow a record with its topic's :py:meth:`Topic.match`.
        """
        return self.handle.fetch_records(
            after=after,
            topics=_names(topics),
            page_size=page_size,
            wait_new_records=wait_new_records,
            type_hint=self._types(),
            **kwargs,
        )

    async def fetch_last_records(
        self,
        n: int,
        *,
        topics: Sequence[str | Topic[Any]] | None = None,
        **kwargs: Any,
    ) -> Sequence[StreamRecord[Any]]:
        """The stream's last ``n`` records, decoded as :py:meth:`fetch_records` decodes them."""
        return await self.handle.fetch_last_records(
            n, topics=_names(topics), type_hint=self._types(), **kwargs
        )

    async def close(
        self, *, failure: BaseException | None = None, reason: str = ""
    ) -> None:
        """Close the stream, with the handle's ``close``."""
        await self.handle.close(failure=failure, reason=reason)

    def producer(self, id: str, *, epoch: int | None = None) -> Self:
        """The definition bound to a client's producer: see
        :py:meth:`temporalio.streams.StreamHandle.producer`. Use it with ``async with`` to release
        the producer's ID on exit.
        """
        return type(self)(self.handle.producer(id, epoch=epoch))

    async def __aenter__(self) -> Self:
        """Return self."""
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        """Leave the handle's ``async with`` block, if it has one, as a producer does."""
        if aexit := getattr(self.handle, "__aexit__", None):
            await aexit(*exc_info)

    @classmethod
    def _types(cls) -> Mapping[str, type]:
        return {t.name: t.type for t in cls.stream_topics.values()}


def _names(topics: Sequence[str | Topic[Any]] | None) -> list[str] | None:
    if topics is None:
        return None
    return [t if isinstance(t, str) else t.name for t in topics]


def definition_args(
    stream: StreamHandleBase | type[Stream] | None,
    name: str | None,
    stream_id: str | None,
) -> tuple[type[Stream] | None, StreamHandleBase | None, str | None]:
    """The definition, handle and name that ``get_stream_handle``'s arguments give. A definition
    gives the name of an owner's stream; a standalone stream has none.
    """
    if not (isinstance(stream, type) and issubclass(stream, Stream)):
        return None, stream, name
    if name:
        raise ValueError("Pass either a stream definition or a name, not both")
    return stream, None, None if stream_id else stream.stream_name


def topic_args(name: str | Topic[T], type: type[T] | None) -> tuple[str, type[T]]:
    """The name and type of a topic given by its name and type, or as a definition's topic."""
    if isinstance(name, Topic):
        return name.name, name.type
    if type is None:
        raise TypeError("A topic given by name needs its type")
    return name, type


def stream_name(name: str | type[Stream]) -> str:
    """The name, or the definition's name."""
    return name if isinstance(name, str) else name.stream_name
