"""A :py:class:`temporalio.streams.StreamBackend` that stores streams in Redis.

.. warning::
    This API is a prototype and will change.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta

import redis.asyncio

from temporalio.streams import (
    Producer,
    ReadResponse,
    RecordInput,
    StreamBackend,
    StreamClose,
    StreamRef,
    WorkflowProducer,
)


class RedisStreamBackend(StreamBackend):
    """Stores streams in Redis, as a customer's worker or client would."""

    def __init__(self, client: redis.asyncio.Redis, namespace: str) -> None:
        """Create a backend storing the namespace's streams in the given Redis."""
        self._client = client
        self._namespace = namespace

    async def append(
        self,
        ref: StreamRef,
        producer: Producer,
        seq: int,
        records: Sequence[RecordInput],
        *,
        finish: bool = False,
    ) -> Sequence[bytes]:
        """See :py:meth:`temporalio.streams.StreamBackend.append`."""
        raise NotImplementedError

    async def close(
        self, ref: StreamRef, owner: WorkflowProducer, seq: int, close: StreamClose
    ) -> None:
        """See :py:meth:`temporalio.streams.StreamBackend.close`."""
        raise NotImplementedError

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
        raise NotImplementedError
