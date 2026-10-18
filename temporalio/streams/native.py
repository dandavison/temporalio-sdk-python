"""A :py:class:`temporalio.streams.StreamBackend` that stores streams in Temporal.

.. warning::
    This API is a prototype and will change.

The stream is embedded in its owning workflow. Every operation is a direct Nexus call to the stream
service on the System Nexus Endpoint, which the server runs in a transaction on the owning
workflow. The server checks activity and workflow producers against the workflow's state, records
which attempt of each workflow task committed, and closes the stream when the workflow ends.

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
    async def connect(client: temporalio.client.Client) -> NativeStreamBackend:
        """Create a backend that calls the server through the client's connection."""
        from temporalio.worker._worker import _extract_bridge_client_for_worker

        bridge_client = await _extract_bridge_client_for_worker(
            client
        )._connected_client()
        return NativeStreamBackend(
            temporalio.bridge.temporal_sdk_bridge.new_native_streams(
                bridge_client._ref,
                NativeOptions(namespace=client.namespace).SerializeToString(),
            )
        )
