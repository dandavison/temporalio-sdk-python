"""The local-server module: a wasm build of the Temporal server's CHASM workflow implementation,
which the Temporal SDK runs in-process for :py:class:`temporalio.worker.LocalExecution`.

The module is built by ``scripts/build-local-server-module`` and is not checked in.
"""

from pathlib import Path


def module_path() -> Path:
    """Path of the local-server module."""
    path = Path(__file__).parent / "local-server.wasm"
    if not path.exists():
        raise FileNotFoundError(
            f"{path} does not exist: build it with scripts/build-local-server-module"
        )
    return path
