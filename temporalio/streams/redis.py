"""A :py:class:`temporalio.streams.StreamBackend` that stores streams in Redis.

.. warning::
    This API is a prototype and will change.

A stream is three keys sharing a hash tag: a Redis stream holding the log, a hash holding each
producer's dedupe and fencing state, and a metadata hash holding existence, close and deletion.
Every append and close goes through one Lua script, which checks and writes in one step. Producers
that XADD to the log directly are not supported.

Properties weaker than Temporal-native streams, all because Redis cannot see the server's state:

* A stale producer is fenced only once its successor has appended.
* Nothing records which attempt of a workflow task committed, so a failed attempt's records are not
  withdrawn if the attempt that succeeds appends nothing.
* Appends from speculative workflow tasks are not rejected.
* The stream is keyed by workflow ID, not by run chain, so a closed stream blocks a new chain with
  the same workflow ID from appending.
* The owner's worker writes the close when the workflow ends, so after a worker crash, or when the
  workflow is terminated or times out, the close can be late or missing.
* A topic filter is applied after reading: the reader receives every record.
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Sequence
from datetime import timedelta
from typing import cast

import redis.asyncio
import redis.exceptions

import temporalio.api.common.v1
import temporalio.api.failure.v1
from temporalio.streams import (
    ActivityProducer,
    AppendRecord,
    ClientProducer,
    CloseStatus,
    EpochFencedError,
    Producer,
    ProducerIdentity,
    ReadResponse,
    Record,
    SeqDivergentError,
    SeqStaleError,
    StreamBackend,
    StreamClose,
    StreamClosedError,
    StreamCursorError,
    StreamError,
    StreamExpiredError,
    StreamNotFoundError,
    StreamNotOwnerError,
    StreamRef,
    WorkflowProducer,
)

# KEYS: meta, log, producers.
# ARGV: producer, workflow task, epoch, seq, digest, closing ("1" or "0"), entry count, then
# for each entry a field count followed by that many field/value pairs.
_APPEND_SCRIPT = """
local meta, log, producers = KEYS[1], KEYS[2], KEYS[3]
local producer, wt, epoch, seq, digest = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4]), ARGV[5]
local closing = ARGV[6] == '1'
if redis.call('HGET', meta, 'deleted') == '1' then
  return redis.error_reply('EXPIRED')
end
local raw = redis.call('HGET', producers, producer)
if raw then
  local st = cjson.decode(raw)
  if wt < st.wt or (wt == st.wt and epoch < st.epoch) then
    return redis.error_reply('FENCED')
  end
  if wt == st.wt and epoch == st.epoch then
    if seq == st.seq then
      if digest == st.digest then
        return st.ids
      end
      return redis.error_reply('SEQ_DIVERGENT')
    end
    if seq < st.seq then
      return redis.error_reply('SEQ_STALE')
    end
  end
end
if redis.call('HGET', meta, 'closed') == '1' then
  return redis.error_reply('CLOSED')
end
local ids = {}
local i = 8
for _ = 1, tonumber(ARGV[7]) do
  local nfields = tonumber(ARGV[i])
  i = i + 1
  local args = {'XADD', log, '*', 'producer', producer, 'wt', wt, 'epoch', epoch, 'seq', seq}
  for j = 0, 2 * nfields - 1 do
    args[#args + 1] = ARGV[i + j]
    if closing and j % 2 == 0 then
      redis.call('HSET', meta, 'close_' .. ARGV[i + j], ARGV[i + j + 1])
    end
  end
  i = i + 2 * nfields
  ids[#ids + 1] = redis.call(unpack(args))
end
redis.call('HSET', producers, producer, cjson.encode({wt = wt, epoch = epoch, seq = seq, digest = digest, ids = ids}))
redis.call('HSET', meta, 'exists', '1')
if closing then
  redis.call('HSET', meta, 'closed', '1')
end
return ids
"""

_ERRORS: dict[str, type[StreamError]] = {
    "EXPIRED": StreamExpiredError,
    "FENCED": EpochFencedError,
    "SEQ_DIVERGENT": SeqDivergentError,
    "SEQ_STALE": SeqStaleError,
    "CLOSED": StreamClosedError,
}

_DEFAULT_MAX_RECORDS = 1000

_Entry = list[bytes]


class RedisStreamBackend(StreamBackend):
    """Stores streams in Redis, as a customer's worker or client would."""

    def __init__(self, client: redis.asyncio.Redis, namespace: str) -> None:
        """Create a backend storing the namespace's streams in the given Redis."""
        self._client = client
        self._namespace = namespace
        self._append_script = client.register_script(_APPEND_SCRIPT)

    async def append(
        self,
        ref: StreamRef,
        producer: Producer,
        seq: int,
        records: Sequence[AppendRecord],
        *,
        finish: bool = False,
    ) -> Sequence[bytes]:
        """See :py:meth:`temporalio.streams.StreamBackend.append`."""
        entries = [
            [
                b"kind",
                b"data",
                b"topic",
                r.topic.encode(),
                b"payload",
                r.payload.SerializeToString(),
            ]
            for r in records
        ]
        if finish:
            entries.append([b"kind", b"finish"])
        ids = await self._run_append(ref, producer, seq, entries, closing=False)
        return [self._cursor(ref, i) for i in ids[: len(records)]]

    async def close(
        self, ref: StreamRef, owner: WorkflowProducer, seq: int, close: StreamClose
    ) -> None:
        """See :py:meth:`temporalio.streams.StreamBackend.close`."""
        if owner.workflow_id != ref.workflow_id:
            raise StreamNotOwnerError
        entry = [
            b"kind",
            b"close",
            b"status",
            str(int(close.status)).encode(),
            b"failure",
            close.failure.SerializeToString() if close.failure else b"",
            b"reason",
            close.reason.encode(),
        ]
        await self._run_append(ref, owner, seq, [entry], closing=True)

    async def read(
        self,
        ref: StreamRef,
        *,
        after: bytes | None = None,
        topics: Sequence[str] = (),
        max_records: int = 0,
        wait: timedelta = timedelta(0),
        last: int = 0,
    ) -> ReadResponse:
        """See :py:meth:`temporalio.streams.StreamBackend.read`."""
        meta_key, log_key, _ = self._keys(ref)
        meta = await self._client.hgetall(meta_key)  # type: ignore[misc]
        if meta.get(b"deleted") == b"1":
            raise StreamExpiredError
        last_id = b"0-0"
        if after is not None:
            last_id = self._entry_id(ref, after)
            if meta.get(b"exists") != b"1":
                raise StreamNotFoundError
        if last > 0:
            return await self._read_tail(ref, topics, last)
        deadline = time.monotonic() + wait.total_seconds()
        records: list[Record] = []
        cursor = after
        while True:
            entries = await self._client.xrange(
                log_key, b"(" + last_id, b"+", count=max_records or _DEFAULT_MAX_RECORDS
            )
            remaining = deadline - time.monotonic()
            if not entries and remaining > 0:
                entries = await self._block(log_key, last_id, remaining)
            for entry_id, fields in entries:
                last_id = entry_id
                cursor = self._cursor(ref, entry_id)
                if fields[b"kind"] == b"close":
                    break
                record = _record(cursor, fields, topics)
                if record:
                    records.append(record)
            if records or deadline <= time.monotonic():
                break
            if await self._closed_at(ref, last_id):
                break
        close = (
            await self._close_from_meta(ref)
            if await self._closed_at(ref, last_id)
            else None
        )
        return ReadResponse(records=records, cursor=cursor, close=close)

    async def _run_append(
        self,
        ref: StreamRef,
        producer: Producer,
        seq: int,
        entries: list[_Entry],
        *,
        closing: bool,
    ) -> list[bytes]:
        identity, workflow_task, epoch = _producer(producer)
        args: list[bytes | str | int] = [
            identity,
            workflow_task,
            epoch,
            seq,
            _append_digest(entries),
            "1" if closing else "0",
            len(entries),
        ]
        for entry in entries:
            args.append(len(entry) // 2)
            args.extend(entry)
        try:
            ids = await self._append_script(keys=list(self._keys(ref)), args=args)
        except redis.exceptions.ResponseError as err:
            error = _ERRORS.get(str(err).removeprefix("ERR "))
            if error:
                raise error from err
            raise
        return cast(list[bytes], ids)

    async def _block(
        self, log_key: str, last_id: bytes, remaining: float
    ) -> list[tuple[bytes, dict[bytes, bytes]]]:
        result = await self._client.xread(
            {log_key: last_id}, block=max(1, int(remaining * 1000))
        )
        return result[0][1] if result else []

    async def _closed_at(self, ref: StreamRef, last_id: bytes) -> bool:
        """Whether the stream is closed and the reader has read every entry up to ``last_id``,
        which includes the close entry, the stream's last.
        """
        meta_key, log_key, _ = self._keys(ref)
        if await self._client.hget(meta_key, "closed") != b"1":  # type: ignore[misc]
            return False
        return not await self._client.xrange(log_key, b"(" + last_id, b"+", count=1)

    async def _close_from_meta(self, ref: StreamRef) -> StreamClose:
        meta = await self._client.hgetall(self._keys(ref)[0])  # type: ignore[misc]
        failure = None
        if meta.get(b"close_failure"):
            failure = temporalio.api.failure.v1.Failure()
            failure.ParseFromString(meta[b"close_failure"])
        return StreamClose(
            status=CloseStatus(int(meta[b"close_status"])),
            failure=failure,
            reason=meta.get(b"close_reason", b"").decode(),
        )

    async def _read_tail(
        self, ref: StreamRef, topics: Sequence[str], last: int
    ) -> ReadResponse:
        entries = await self._client.xrevrange(self._keys(ref)[1], b"+", b"-")
        records: list[Record] = []
        for entry_id, fields in entries:
            if len(records) == last:
                break
            record = _record(self._cursor(ref, entry_id), fields, topics)
            if record:
                records.insert(0, record)
        cursor = self._cursor(ref, entries[0][0]) if entries else None
        return ReadResponse(records=records, cursor=cursor, close=None)

    def _keys(self, ref: StreamRef) -> tuple[str, str, str]:
        prefix = self._prefix(ref)
        return f"{prefix}:meta", f"{prefix}:log", f"{prefix}:producers"

    def _prefix(self, ref: StreamRef) -> str:
        return f"stream:{{{self._namespace}/{ref.workflow_id}}}"

    def _cursor(self, ref: StreamRef, entry_id: bytes) -> bytes:
        return self._prefix(ref).encode() + b"#" + entry_id

    def _entry_id(self, ref: StreamRef, cursor: bytes) -> bytes:
        prefix, sep, entry_id = cursor.partition(b"#")
        if not sep or prefix != self._prefix(ref).encode():
            raise StreamCursorError
        return entry_id


def _producer(producer: Producer) -> tuple[str, int, int]:
    """Return the stored (producer, workflow task, epoch)."""
    if isinstance(producer, ClientProducer):
        return f"client/{producer.id}", 0, producer.epoch
    if isinstance(producer, ActivityProducer):
        info = producer.info
        if info.workflow_run_id:
            return (
                f"activity/{info.workflow_run_id}/{info.activity_id}",
                0,
                info.attempt_stamp,
            )
        return f"activity/{info.activity_run_id}", 0, info.attempt_stamp
    return (
        f"workflow/{producer.run_id}",
        producer.previous_started_event_id,
        producer.attempt,
    )


def _record(
    cursor: bytes, fields: dict[bytes, bytes], topics: Sequence[str]
) -> Record | None:
    kind = fields[b"kind"]
    topic = fields.get(b"topic", b"").decode()
    if kind == b"close" or (topics and topic not in topics):
        return None
    payload = None
    if kind == b"data":
        payload = temporalio.api.common.v1.Payload()
        payload.ParseFromString(fields[b"payload"])
    return Record(
        cursor=cursor,
        producer=ProducerIdentity(
            producer=fields[b"producer"].decode(),
            workflow_task=int(fields[b"wt"]),
            epoch=int(fields[b"epoch"]),
            seq=int(fields[b"seq"]),
        ),
        topic=topic,
        payload=payload,
        finish=kind == b"finish",
    )


def _append_digest(entries: list[_Entry]) -> str:
    """Identify an append's content, so that a retry with different content is detected."""
    h = hashlib.sha256()
    for entry in entries:
        for field in entry:
            h.update(len(field).to_bytes(8, "big"))
            h.update(field)
    return h.hexdigest()
