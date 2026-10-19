"""Streams stored in Redis, through a client plugin.

.. warning::
    This API is a prototype and will change.

A client connected with :py:class:`RedisStreamsPlugin` stores every stream it and its workers use
in Redis, instead of in Temporal:

.. code-block:: python

    client = await Client.connect(
        "localhost:7233",
        plugins=[RedisStreamsPlugin("redis://localhost:6379", retention=timedelta(days=7))],
    )

The calls are made by the temporalio-streams crate, through the bridge; the server never talks to
Redis. Workflow readers of a stream wait on its owner's notifier in the server, which producers
wake: after an activity's or a client's appends, the backend sends a Notify through the client's
connection, at most one per stream every 100 ms and always one after the last append; after a
workflow's activation appends, the worker sends one before completing the activation. A failed
Notify fails nothing: it is logged, and the next append sends another. A reader is not woken if a
producer stops between an append and its Notify. A stream is keyed by the client's namespace and its owner's kind and ID and its run: a
workflow's first run ID, or a Standalone Activity's run ID, which an append needs. Every append and
close goes through one Lua script, which checks and writes in one step. Producers that XADD to the
log directly are not supported. Every write refreshes the keys' time-to-live, the retention.

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
* The owner's worker writes a workflow's close, and only after Core reports that the server
  accepted the run's terminal completion. It writes none for a cron run that completes or fails,
  or for a failed run with a retry policy, since the chain may continue. A stream is left open,
  until its retention passes, when its workflow is terminated or times out, when a failed run
  with a retry policy is not retried (native closes it), or when the worker stops before writing
  the close.
* Nothing closes a Standalone Activity's stream when the activity ends: the worker cannot learn
  whether the server accepted the activity's completion, or whether a failed attempt will be
  retried. The activity must close its own stream, or it stays open until its retention passes.
* A topic filter is applied after reading: the reader receives every record.
* A workflow activity names its workflow's stream by its workflow's current run ID, which is the
  stream's key only in the chain's first run.

There is no commit mode: a worker refuses to register a workflow that declares it.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta

import temporalio.bridge.temporal_sdk_bridge
import temporalio.client
import temporalio.service
from temporalio.bridge.proto.streams import RedisOptions
from temporalio.streams._crate import CrateStreamBackend

__all__ = ["RedisStreamBackend", "RedisStreamsPlugin"]


class RedisStreamsPlugin(temporalio.client.Plugin):
    """A client plugin that makes the client, and the workers created from it, store streams in
    Redis.
    """

    def __init__(self, url: str, *, retention: timedelta = timedelta(days=7)) -> None:
        """Store streams in the Redis at ``url``, keeping each for ``retention`` after its last
        write.
        """
        self._url = url
        self._retention = retention

    def configure_client(
        self, config: temporalio.client.ClientConfig
    ) -> temporalio.client.ClientConfig:
        """Set the client's stream backend, keyed by the client's namespace."""
        config["stream_backend"] = RedisStreamBackend(
            self._url,
            config["service_client"],
            config["namespace"],
            retention=self._retention,
        )
        return config

    async def connect_service_client(
        self,
        config: temporalio.service.ConnectConfig,
        next: Callable[
            [temporalio.service.ConnectConfig],
            Awaitable[temporalio.service.ServiceClient],
        ],
    ) -> temporalio.service.ServiceClient:
        """Connect as usual: Redis is connected on the backend's first call, because the namespace
        that keys its streams is not known here.
        """
        return await next(config)


class RedisStreamBackend(CrateStreamBackend):
    """Stores streams in Redis."""

    def __init__(
        self,
        url: str,
        service_client: temporalio.service.ServiceClient,
        namespace: str,
        *,
        retention: timedelta = timedelta(days=7),
    ) -> None:
        """Store streams in the Redis at ``url``, keyed by ``namespace``, calling the server in
        that namespace through ``service_client``. It connects on first use, and that call fails if
        the deployment could lose stream data silently.
        """
        self._options = RedisOptions(url=url, namespace=namespace)
        self._options.retention.FromTimedelta(retention)
        self._service_client = service_client
        self._ref: temporalio.bridge.temporal_sdk_bridge.StreamsRef | None = None
        self._connecting: asyncio.Lock | None = None

    async def _streams(self) -> temporalio.bridge.temporal_sdk_bridge.StreamsRef:
        if not self._ref:
            self._connecting = self._connecting or asyncio.Lock()
            async with self._connecting:
                if not self._ref:
                    from temporalio.worker._worker import (
                        _extract_bridge_client_for_worker,
                    )

                    bridge_client = await _extract_bridge_client_for_worker(
                        temporalio.client.Client(
                            self._service_client, namespace=self._options.namespace
                        )
                    )._connected_client()
                    self._ref = (
                        await temporalio.bridge.temporal_sdk_bridge.new_redis_streams(
                            bridge_client._ref,
                            self._options.SerializeToString(),
                        )
                    )
        return self._ref
