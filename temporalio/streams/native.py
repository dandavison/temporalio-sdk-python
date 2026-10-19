"""A :py:class:`temporalio.streams.backend.StreamBackend` that stores streams in Temporal: the
backend of every client that no plugin gives another one.

.. warning::
    This API is a prototype and will change.

The stream is embedded in its owner: a workflow run, or a Standalone Activity execution. Every
operation is a direct Nexus call to the stream service on the System Nexus Endpoint, through the
client's connection, which the server runs in a transaction on the owner. An append or close whose
owner does not exist fails as not found.

The server checks the owner's producers against the owner's state. On a workflow's stream, a
workflow task must be the run's current one, not speculative, and an activity task token must be
for the started attempt of one of the run's activities, with the stamp that attempt started with;
other activities' tokens are rejected. On a Standalone Activity's stream, only the activity's own
attempts are checked; workflows and other activities are not verified. Clients and local
activities are never verified. A task token from before the attempt stamp existed reads as stamp
0, which matches a first attempt.

The server records which attempt completed each workflow task, and closes the stream in the
transaction that ends its owner, unless the owner closed it first. A retry of a Standalone
Activity does not close it. The worker writes a workflow's close only when the workflow closes its
own stream.

The stream is not moved to the next run of a chain at continue-as-new, retry or cron: the next run
starts with no stream, so a chain's records do not continue across runs. A run that ends without
having a stream never gets one, so a read of it never reports a close.

Only native supports commit mode, which a workflow declares with
``@workflow.defn(stream_commit_mode=True)``: its own records travel in its workflow task
completion, and become visible when the task commits, so a failed task attempt's records are never
delivered. A run's first workflow append fixes its mode, so during a rolling deploy that changes the
declaration, a run's workflow tasks fail on the workers whose definition declares the other mode,
and the run makes progress only on workers whose definition matches.

The calls are made by the temporalio-streams crate, through the bridge.
"""

from __future__ import annotations

import temporalio.bridge.temporal_sdk_bridge
import temporalio.client
from temporalio.bridge.proto.streams import NativeOptions
from temporalio.streams._crate import CrateStreamBackend


class NativeStreamBackend(CrateStreamBackend):
    """Stores streams in Temporal, embedded in their owners, calling the server through a client's
    connection.
    """

    def __init__(self, client: temporalio.client.Client) -> None:
        """Create a backend for the client's connection and namespace. It connects on first use."""
        self._client = client
        self._ref: temporalio.bridge.temporal_sdk_bridge.StreamsRef | None = None

    async def _streams(self) -> temporalio.bridge.temporal_sdk_bridge.StreamsRef:
        if not self._ref:
            from temporalio.worker._worker import _extract_bridge_client_for_worker

            bridge_client = await _extract_bridge_client_for_worker(
                self._client
            )._connected_client()
            self._ref = temporalio.bridge.temporal_sdk_bridge.new_native_streams(
                bridge_client._ref,
                NativeOptions(namespace=self._client.namespace).SerializeToString(),
            )
        return self._ref
