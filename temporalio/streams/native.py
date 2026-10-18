"""A :py:class:`temporalio.streams.StreamBackend` that stores streams in Temporal.

.. warning::
    This API is a prototype and will change.

The stream is embedded in its owning workflow. Every operation is a direct Nexus call to the stream
service on the System Nexus Endpoint, which the server runs in a transaction on the owning
workflow. The server checks activity and workflow producers against the workflow's state, records
which attempt of each workflow task committed, and closes the stream when the workflow ends.

With ``commit_mode``, a worker using the backend sends each workflow's own records in its workflow
task completion, and they become visible when the task commits; a failed task attempt's records
are never delivered. A run's first workflow append fixes its mode, so during a rolling deploy in
which workers disagree, a run's workflow tasks fail on the workers whose mode differs.

The calls are made by the temporalio-streams crate, through the bridge.
"""

from __future__ import annotations

import temporalio.bridge.temporal_sdk_bridge
import temporalio.client
from temporalio.bridge.proto.streams import NativeOptions
from temporalio.streams._crate import CrateStreamBackend


class NativeStreamBackend(CrateStreamBackend):
    """Stores streams in Temporal, embedded in their owning workflows."""

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
