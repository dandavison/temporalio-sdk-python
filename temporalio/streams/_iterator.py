"""Reading a stream: records with their status, and notices of withdrawn records."""

from __future__ import annotations

from collections import deque
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from enum import IntEnum
from typing import Any, Generic, NewType, TypeVar

import temporalio.converter
from temporalio.streams._backend import CloseStatus, StreamBackend, StreamClose
from temporalio.streams._record import ProducerIdentity, Record, WorkflowAppendMode
from temporalio.streams._ref import StreamRef

T = TypeVar("T")

StreamCursor = NewType("StreamCursor", bytes)
"""The boundary after a record. Opaque, valid only for its stream, and safe to pass as workflow
input."""

# Native caps a read's wait at 20 s, so a reader waiting longer reads again.
_READ_WAIT = timedelta(seconds=20)


class RecordStatus(IntEnum):
    """Whether a record can still be withdrawn.

    Only workflow and local activity records are provisional: a record of a workflow task is final
    once the task's commit record says that the record's attempt completed the task. Records of
    activities and clients are final when read, but are withdrawn if the same producer appends at a
    higher epoch.
    """

    FINAL = 1
    PROVISIONAL = 2


class StreamMode(IntEnum):
    """How the stream's owner appends its own records."""

    UNKNOWN = 0
    """No read has told the mode yet."""
    SINK = 1
    """The owner's records are appended as it runs, so a failed workflow task attempt's records
    can be read, and are then withdrawn."""
    COMMIT = 2
    """The owning workflow's records become visible when its workflow task commits."""


@dataclass(frozen=True)
class StreamRecord(Generic[T]):
    """A record and its value."""

    value: T
    topic: str
    cursor: StreamCursor
    """Pass as ``after=`` to read the records after this one."""
    producer: ProducerIdentity
    status: RecordStatus
    """The record's status when the iterator yielded it."""


@dataclass(frozen=True)
class Withdrawal:
    """A notice that a record yielded earlier is withdrawn: it was written by a workflow task attempt
    that did not complete its task, or a producer appended at a higher epoch after it.
    """

    cursor: StreamCursor
    """The withdrawn record's cursor."""
    producer: ProducerIdentity


class StreamRecordAsyncIterator(Generic[T]):
    """Asynchronous iterator over a stream's records, in stream order, and notices of records it
    yielded earlier that are withdrawn.

    It makes no request until the first iteration. Without ``wait_new_records`` it ends at the
    stream's current end; with it, it waits for new records and ends only at the stream's close.
    Markers and producers' finish records are never yielded.

    Each record is yielded with its status as the iterator knows it: records it has read but not
    yet yielded count, so a reader that trails the writers usually receives committed records as
    final, and never receives records already known to be withdrawn. When it learns later that a
    yielded record is withdrawn, it yields a :py:class:`Withdrawal` for it. To do so it keeps the
    cursor of every record it yielded at each producer's current epoch, and of every provisional
    record until its workflow task commits, so its memory grows with the records it reads.

    Only native writes the commit records that make workflow records final, so on another backend
    every workflow and local activity record is provisional, and a failed workflow task attempt's
    record is withdrawn only if the attempt that completes the task appends in it.
    """

    def __init__(
        self,
        backend: StreamBackend,
        ref: StreamRef,
        data_converter: temporalio.converter.DataConverter,
        *,
        after: StreamCursor | None,
        topics: Sequence[str] | None,
        page_size: int | None,
        wait_new_records: bool,
        type_hint: type | None,
        rpc_metadata: Mapping[str, str | bytes],
        rpc_timeout: timedelta | None,
    ) -> None:
        """Create an iterator. Use ``fetch_records`` on a stream handle instead."""
        self._backend = backend
        self._ref = ref
        self._data_converter = data_converter
        self._read_after = after
        self._topics = topics or ()
        self._page_size = page_size or 0
        self._wait_new_records = wait_new_records
        self._type_hint = type_hint
        self._rpc_metadata = rpc_metadata
        self._rpc_timeout = rpc_timeout
        self._items: deque[StreamRecord[T] | Withdrawal] = deque()
        self._rule = _FailedAttemptRule()
        self._ended = False
        self.cursor: StreamCursor | None = after
        """Where to resume so that no record is lost: the cursor of the last record yielded, or
        ``after`` if none was."""
        self.close: StreamClose | None = None
        """Set when the iteration ended at the stream's close."""
        self.owner_end: CloseStatus | None = None
        """Not set yet: reserved for an owner that ended without its stream reporting a close."""
        self.mode = StreamMode.SINK if ref.activity_id else StreamMode.UNKNOWN
        """The owner's mode: sink for a Standalone Activity, and for a workflow the mode its run's
        append mode record gave, if the iterator has read one. The read response does not report
        the mode, so a read that starts after that record, or of a backend that writes none, leaves
        it unknown."""

    def __aiter__(self) -> StreamRecordAsyncIterator[T]:
        """Return self."""
        return self

    async def __anext__(self) -> StreamRecord[T] | Withdrawal:
        """Return the next record or withdrawal notice."""
        while not self._items:
            if self._ended:
                raise StopAsyncIteration
            await self._read()
        item = self._items.popleft()
        if isinstance(item, StreamRecord):
            self.cursor = item.cursor
        return item

    async def _read(self) -> None:
        resp = await self._backend.read(
            self._ref,
            after=self._read_after,
            topics=self._topics,
            max_records=self._page_size,
            wait=_READ_WAIT if self._wait_new_records else timedelta(0),
            rpc_metadata=self._rpc_metadata,
            rpc_timeout=self._rpc_timeout,
        )
        self._read_after = (
            StreamCursor(resp.cursor) if resp.cursor else self._read_after
        )
        for r in resp.records:
            if r.workflow_append_mode:
                self.mode = (
                    StreamMode.COMMIT
                    if r.workflow_append_mode.mode == WorkflowAppendMode.COMMIT
                    else StreamMode.SINK
                )
        self._items.extend(
            await _decode(
                self._data_converter,
                list(self._rule.judge(resp.records)),
                self._type_hint,
            )
        )
        if resp.close:
            self.close = resp.close
            self._ended = True
        elif not resp.records and not self._wait_new_records:
            self._ended = True


async def fetch_last_records(
    backend: StreamBackend,
    ref: StreamRef,
    data_converter: temporalio.converter.DataConverter,
    n: int,
    *,
    topics: Sequence[str] | None,
    type_hint: type | None,
    rpc_metadata: Mapping[str, str | bytes],
    rpc_timeout: timedelta | None,
) -> Sequence[StreamRecord[Any]]:
    """Read the last ``n`` records, leaving out markers, which the backend counts towards ``n``."""
    if n <= 0:
        return []
    last = n
    while True:
        resp = await backend.read(
            ref,
            topics=topics or (),
            last=last,
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
        )
        found = sum(1 for r in resp.records if r.payload is not None)
        if found >= n or len(resp.records) < last:
            break
        last *= 2
    items = await _decode(
        data_converter, list(_FailedAttemptRule().judge(resp.records)), type_hint
    )
    return [i for i in items if isinstance(i, StreamRecord)][-n:]


async def _decode(
    data_converter: temporalio.converter.DataConverter,
    judged: list[tuple[Record, RecordStatus] | Withdrawal],
    type_hint: type | None,
) -> list[StreamRecord[Any] | Withdrawal]:
    payloads = [
        j[0].payload
        for j in judged
        if isinstance(j, tuple) and j[0].payload is not None
    ]
    values = iter(
        await data_converter.decode(
            payloads, [type_hint] * len(payloads) if type_hint else None
        )
        if payloads
        else []
    )
    return [
        StreamRecord(
            value=next(values),
            topic=j[0].topic,
            cursor=StreamCursor(j[0].cursor),
            producer=j[0].producer,
            status=j[1],
        )
        if isinstance(j, tuple)
        else j
        for j in judged
    ]


@dataclass
class _Yielded:
    """The records yielded at a producer's current epoch, which a higher epoch withdraws."""

    epoch: tuple[int, ...]
    records: list[Record] = field(default_factory=list)


class _FailedAttemptRule:
    """Judges each data record final, provisional or withdrawn, and finds yielded records that are
    withdrawn later.

    A workflow or local activity record of workflow task T is final when T's commit record names
    its attempt, withdrawn when it names another, final when T precedes the stream creation
    record's next workflow task, and provisional otherwise. A record of any producer is withdrawn
    when a record of the same producer at a higher epoch follows. A workflow producer's epoch is
    compared only within one workflow task, since every task starts again at attempt 1.
    """

    def __init__(self) -> None:
        self._commits: dict[tuple[str, int], int] = {}
        self._created: dict[str, int] = {}
        self._epochs: dict[tuple[str, int], _Yielded] = {}
        self._provisional: dict[tuple[str, int], list[Record]] = {}

    def judge(
        self, records: Sequence[Record]
    ) -> Iterator[tuple[Record, RecordStatus] | Withdrawal]:
        """Yield each data record of a page with its status, leaving out withdrawn records, and
        a withdrawal notice for each record yielded from an earlier page that this page withdraws.
        """
        page_epochs: dict[tuple[str, int], tuple[int, ...]] = {}
        for r in records:
            if c := r.workflow_task_commit:
                self._commits[c.workflow_run_id, c.workflow_task] = c.attempt
            elif s := r.stream_created:
                self._created[s.workflow_run_id] = s.next_workflow_task
            elif not r.is_marker:
                key, epoch = _epoch(r)
                page_epochs[key] = max(epoch, page_epochs.get(key, epoch))
        for r in records:
            if c := r.workflow_task_commit:
                yield from self._resolve(c.workflow_run_id, c.workflow_task, c.attempt)
                continue
            if r.is_marker:
                continue
            key, epoch = _epoch(r)
            yielded = self._epochs.get(key)
            if not yielded or epoch > yielded.epoch:
                if yielded:
                    yield from (
                        Withdrawal(StreamCursor(w.cursor), w.producer)
                        for w in yielded.records
                    )
                yielded = self._epochs[key] = _Yielded(epoch)
                self._forget_earlier_tasks(r)
            if r.finish or epoch < yielded.epoch or epoch < page_epochs[key]:
                continue
            status = self._status(r)
            if status is None:
                continue
            yielded.records.append(r)
            if status == RecordStatus.PROVISIONAL:
                p = r.producer
                self._provisional.setdefault(
                    (p.workflow_run_id, p.workflow_task), []
                ).append(r)
            yield r, status

    def _status(self, r: Record) -> RecordStatus | None:
        p = r.producer
        if not p.workflow_run_id:
            return RecordStatus.FINAL
        committed = self._commits.get((p.workflow_run_id, p.workflow_task))
        if committed is not None:
            return RecordStatus.FINAL if committed == p.workflow_task_attempt else None
        if p.workflow_task < self._created.get(p.workflow_run_id, 0):
            return RecordStatus.FINAL
        return RecordStatus.PROVISIONAL

    def _resolve(
        self, run_id: str, workflow_task: int, attempt: int
    ) -> Iterator[Withdrawal]:
        for r in self._provisional.pop((run_id, workflow_task), []):
            if r.producer.workflow_task_attempt != attempt:
                yield Withdrawal(StreamCursor(r.cursor), r.producer)

    def _forget_earlier_tasks(self, r: Record) -> None:
        # A run's record of task T means its earlier tasks completed: none can have another
        # attempt, and a commit record, if the backend writes them, has already been read.
        p = r.producer
        if not p.producer.startswith("workflow/"):
            return
        for key in [
            k
            for k in self._provisional
            if k[0] == p.workflow_run_id and k[1] < p.workflow_task
        ]:
            del self._provisional[key]
        for key in [
            k for k in self._epochs if k[0] == p.producer and k[1] < p.workflow_task
        ]:
            del self._epochs[key]


def _epoch(r: Record) -> tuple[tuple[str, int], tuple[int, ...]]:
    p = r.producer
    if p.producer.startswith("workflow/"):
        return (p.producer, p.workflow_task), (p.workflow_task_attempt,)
    return (p.producer, 0), (p.workflow_task, p.workflow_task_attempt, p.epoch)
