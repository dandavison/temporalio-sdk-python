"""A :py:class:`temporalio.streams.StreamBackend` that stores streams in Redis.

.. warning::
    This API is a prototype and will change.

The calls are made by the temporalio-streams crate, through the bridge; the server never talks to
Redis. A stream is keyed by its owner's kind and ID and its run: a workflow's first run ID, or a
Standalone Activity's run ID, which an append needs. Every append and close goes through one Lua
script, which checks and writes in one step. Producers that XADD to the log directly are not
supported. Every write refreshes the keys' time-to-live, the retention.

The deployment must run Redis 6.2 or later with ``maxmemory-policy`` ``noeviction``.

Properties weaker than Temporal-native streams, all because Redis cannot see the server's state:

* A stale producer is fenced only once its successor has appended.
* Nothing writes workflow task commit records, stream creation records or append mode records, so
  a failed workflow task attempt's records, and its local activities', are not withdrawn if the
  attempt that succeeds appends nothing.
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

There is no commit mode.
"""

from __future__ import annotations

from datetime import timedelta

import temporalio.bridge.temporal_sdk_bridge
import temporalio.runtime
from temporalio.bridge.proto.streams import RedisOptions
from temporalio.streams._crate import CrateStreamBackend


class RedisStreamBackend(CrateStreamBackend):
    """Stores streams in Redis."""

    @staticmethod
    async def connect(
        url: str,
        namespace: str,
        *,
        retention: timedelta = timedelta(days=7),
        runtime: temporalio.runtime.Runtime | None = None,
    ) -> RedisStreamBackend:
        """Connect to Redis at ``url``, keying streams by ``namespace``.

        Fails if the deployment could lose stream data silently.
        """
        options = RedisOptions(url=url, namespace=namespace)
        options.retention.FromTimedelta(retention)
        runtime = runtime or temporalio.runtime.Runtime.default()
        return RedisStreamBackend(
            await temporalio.bridge.temporal_sdk_bridge.new_redis_streams(
                runtime._core_runtime._ref, options.SerializeToString()
            )
        )
