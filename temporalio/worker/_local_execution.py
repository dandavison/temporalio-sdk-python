"""Local execution: running a worker's workflows in-process on a local server that the server
grants ownership of them.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

import temporalio.bridge.client
import temporalio.runtime
import temporalio.service


@dataclass(frozen=True)
class LocalExecution:
    """Runs the worker's workflows, and the activities they schedule, in this process.

    The worker serves its task queue from an in-process local server. For each new workflow run on
    the task queue, the local server takes ownership of the run from the server, runs it with this
    worker, and sends the run's history to the server at each sync interval and when the run
    closes, so the server shows the run while it runs. If the process dies, the server takes the
    run back once its ownership expires, after three sync intervals.

    The server must support local execution, and the connection to it must not use TLS. Requires
    the ``temporalio-localserver`` package (``temporalio[local]``).

    .. warning::
        This API is a prototype.
    """

    sync_interval: timedelta = timedelta(seconds=1)
    """How often a run's new history is sent to the server."""

    module: str | None = None
    """Path of the local-server module. Defaults to the module of the ``temporalio-localserver``
    package."""

    def _connect(
        self, service_client: temporalio.service._BridgeServiceClient
    ) -> temporalio.bridge.client.Client:
        config = service_client.config
        return temporalio.bridge.client.Client.connect_local_execution(
            (config.runtime or temporalio.runtime.Runtime.default())._core_runtime,
            config._to_bridge_config(),
            temporalio.bridge.client.LocalExecutionConfig(
                module=self.module or _package_module(),
                sync_interval_millis=int(self.sync_interval.total_seconds() * 1000),
            ),
        )


def _package_module() -> str:
    try:
        import temporalio_localserver
    except ImportError as err:
        raise RuntimeError(
            "LocalExecution requires the temporalio-localserver package: "
            "pip install 'temporalio[local]'"
        ) from err
    return str(temporalio_localserver.module_path())
