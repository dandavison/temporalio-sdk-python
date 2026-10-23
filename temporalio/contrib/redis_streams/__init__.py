"""Streams stored in Redis.

.. warning::
    This API is a prototype and will change.

A :py:class:`RedisStreamBackend` in a client's stream backends configuration stores the streams
whose handles name it, for the client and its workers, in Redis instead of in Temporal. As the
first listed backend it is the default:

.. code-block:: python

    client = await Client.connect(
        "localhost:7233",
        stream_backends=StreamBackends([RedisStreamBackend("redis://localhost:6379")]),
    )

The calls are made by the temporalio-streams crate, through the bridge; the server never talks to
Redis. A stream of an owner is keyed by the namespace, its owner's kind and ID, its run (a
workflow's first run ID, or a Standalone Activity's run ID, which a write needs) and its name; a
standalone stream by the namespace and its stream ID. Every create, append and close goes through
one Lua script, which checks and writes in one step. Producers that XADD to the log directly are
not supported. Every write refreshes the keys' time-to-live, the retention.

The deployment must run Redis 6.2 or later with ``maxmemory-policy`` ``noeviction``. The backend
connects, and checks this, on its first call, not when the client connects.

Properties weaker than Temporal-native streams, all because Redis cannot see the server's state:

* A stale producer is fenced only once its successor has appended.
* Nothing writes workflow task commit records, stream creation records or append mode records, so
  a failed workflow task attempt's records, and its local activities', are not withdrawn if the
  attempt that succeeds appends nothing, and a reader receives every workflow and local activity
  record as provisional.
* Producers are not verified, and anything that can write to Redis can write any key, so the
  owner-only close guards against mistakes, not against other writers.
* Appends from speculative workflow tasks are stored: neither the worker nor Core can tell that a
  task is speculative.
* The owner's worker writes a workflow's close, to every stream of the run, and only after Core
  reports that the server accepted the run's terminal completion. It writes none for a cron run that completes or fails,
  or for a failed run with a retry policy, since the chain may continue. A stream is left open,
  until its retention passes, when its workflow is terminated or times out, when a failed run
  with a retry policy is not retried (native closes it), or when the worker stops before writing
  the close.
* Nothing closes a Standalone Activity's stream when the activity ends: the worker cannot learn
  whether the server accepted the activity's completion, or whether a failed attempt will be
  retried. The activity must close its own stream, or it stays open until its retention passes.
* A topic filter is applied after reading: the reader receives every record.
* A standalone stream's idle timeout is applied by the next create, append or close after it, and
  by readers before they report whether the stream is closed: nothing in Redis runs when it
  passes, so a reader waiting at the stream's end learns of the close only when its wait ends.
* Workflow readers wait on the stream's notifier in the server, which producers wake: after an
  activity's or a client's create, append or close, the backend sends a Notify of the stream
  through the client's connection, at most one per stream every 100 ms and always one after the
  last write; after a workflow's writes the worker sends one per stream in the background. A
  failed Notify fails nothing: it is logged, and the next write sends another. A reader is not
  woken if a producer stops between a write and its Notify, until its Wait expires. With
  ``TEMPORAL_STREAMS_ENABLE_WORKFLOW_READS=false``, no Notify is sent.

There is no commit mode: a worker refuses to register a workflow that declares it.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import temporalio.bridge.temporal_sdk_bridge
import temporalio.client
import temporalio.runtime
from temporalio.bridge.proto.streams import RedisOptions
from temporalio.streams._crate import CrateStreamBackend
from temporalio.streams._flags import workflow_reads_enabled

__all__ = ["RedisStreamBackend"]


class RedisStreamBackend(CrateStreamBackend):
    """Stores streams in Redis."""

    def __init__(
        self,
        url: str,
        *,
        retention: timedelta = timedelta(days=7),
        name: str = "redis",
        runtime: temporalio.runtime.Runtime | None = None,
    ) -> None:
        """Store streams in the Redis at ``url``, keyed by the namespace of the client the backend
        is bound to, as the backend named ``name``. It connects on first use, with ``runtime`` or
        else its client's runtime, and that call fails if the deployment could lose stream data
        silently.
        """
        self.name = name
        self._options = RedisOptions(url=url)
        self._options.retention.FromTimedelta(retention)
        self._runtime = runtime
        self._client: temporalio.client.Client | None = None
        self._ref: temporalio.bridge.temporal_sdk_bridge.StreamsRef | None = None
        self._connecting: asyncio.Lock | None = None

    def bind(self, client: temporalio.client.Client) -> None:
        """Keep the client, whose namespace keys the streams, and through whose connection the
        backend calls the server for the streams' notifiers when workflow reads are switched on.
        Raises if a client of another namespace is bound already.
        """
        if not self._client:
            self._client = client
        elif self._client.namespace != client.namespace:
            raise ValueError(
                f"Redis stream backend {self.name!r} is bound to namespace "
                f"{self._client.namespace!r}, so cannot serve namespace {client.namespace!r}"
            )

    async def _streams(self) -> temporalio.bridge.temporal_sdk_bridge.StreamsRef:
        if not self._ref:
            self._connecting = self._connecting or asyncio.Lock()
            async with self._connecting:
                if not self._ref:
                    if not self._client:
                        raise RuntimeError(
                            f"Redis stream backend {self.name!r} is not bound to a client"
                        )
                    self._options.namespace = self._client.namespace
                    runtime = (
                        self._runtime
                        or self._client.service_client.config.runtime
                        or temporalio.runtime.Runtime.default()
                    )
                    bridge_client = None
                    if workflow_reads_enabled():
                        from temporalio.worker._worker import (
                            _extract_bridge_client_for_worker,
                        )

                        bridge_client = (
                            await _extract_bridge_client_for_worker(
                                self._client
                            )._connected_client()
                        )._ref
                    self._ref = (
                        await temporalio.bridge.temporal_sdk_bridge.new_redis_streams(
                            runtime._core_runtime._ref,
                            bridge_client,
                            self._options.SerializeToString(),
                        )
                    )
        return self._ref
