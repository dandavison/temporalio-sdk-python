"""A :py:class:`temporalio.streams.StreamBackend` that stores streams in Temporal.

.. warning::
    This API is a prototype and will change.

The stream is embedded in its owner: a workflow run, or a Standalone Activity execution. Every
operation is a direct Nexus call to the stream service on the System Nexus Endpoint, which the
server runs in a transaction on the owner. An append or close whose owner does not exist fails as
not found.

The server checks the owner's producers against the owner's state. On a workflow's stream, a
workflow task must be the run's current one, not speculative, and an activity task token must be
for the started attempt of one of the run's activities, with the stamp that attempt started with;
other activities' tokens are rejected. On a Standalone Activity's stream, only the activity's own
attempts are checked; workflows and other activities are not verified. Clients and local
activities are never verified. A task token from before the attempt stamp existed reads as stamp
0, which matches a first attempt.

The server records which attempt completed each workflow task, and closes the stream in the
transaction that ends its owner. A retry of a Standalone Activity does not close it. The worker
never writes the close.

The stream is not moved to the next run of a chain at continue-as-new, retry or cron: the next run
starts with no stream, so a chain's records do not continue across runs.

With ``commit_mode``, a worker using the backend sends each workflow's own records in its workflow
task completion, and they become visible when the task commits; a failed task attempt's records
are never delivered. A run's first workflow append fixes its mode, so during a rolling deploy in
which workers disagree, a run's workflow tasks fail on the workers whose mode differs, and the run
makes progress only on workers whose mode matches.

The calls are made by the temporalio-streams crate, through the bridge.
"""

from __future__ import annotations

import temporalio.bridge.temporal_sdk_bridge
import temporalio.client
from temporalio.bridge.proto.streams import NativeOptions
from temporalio.streams._crate import CrateStreamBackend


class NativeStreamBackend(CrateStreamBackend):
    """Stores streams in Temporal, embedded in their owners."""

    @staticmethod
    async def connect(
        client: temporalio.client.Client, *, commit_mode: bool = False
    ) -> NativeStreamBackend:
        """Create a backend that calls the server through the client's connection.

        ``commit_mode`` makes a worker using the backend append workflows' own records in commit
        mode.
        """
        from temporalio.worker._worker import _extract_bridge_client_for_worker

        bridge_client = await _extract_bridge_client_for_worker(
            client
        )._connected_client()
        return NativeStreamBackend(
            temporalio.bridge.temporal_sdk_bridge.new_native_streams(
                bridge_client._ref,
                NativeOptions(
                    namespace=client.namespace, commit_mode=commit_mode
                ).SerializeToString(),
            )
        )
