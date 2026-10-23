"""Fixtures of the stream tests: a Redis server per test, and a client for each backend.

``--streams-config=brook`` runs the tests in the Brook configuration: Redis only, with native
streams and workflow reads of streams switched off, in the SDK by the environment variables
``TEMPORAL_STREAMS_ENABLE_NATIVE`` and ``TEMPORAL_STREAMS_ENABLE_WORKFLOW_READS`` and, when the
server is started with ``dev-server --brook``, on the server. Tests that need native streams or
workflow reads are skipped.
"""

from __future__ import annotations

import inspect
import os
import socket
import subprocess
import time
from collections.abc import Iterator
from datetime import timedelta

import pytest
import pytest_asyncio

from temporalio.client import Client
from temporalio.contrib.redis_streams import RedisStreamBackend
from temporalio.streams import StreamBackends
from temporalio.streams.backend import NATIVE, default_backend

brook = False
"""Whether the tests run in the Brook configuration."""


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--streams-config",
        choices=["full", "brook"],
        default="full",
        help="Run the stream tests with everything on, or in the Brook configuration",
    )


def pytest_configure(config: pytest.Config) -> None:
    global brook
    brook = config.getoption("--streams-config") == "brook"
    if brook:
        os.environ["TEMPORAL_STREAMS_ENABLE_NATIVE"] = "false"
        os.environ["TEMPORAL_STREAMS_ENABLE_WORKFLOW_READS"] = "false"


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    if not brook:
        return
    skip = pytest.mark.skip(reason="needs native streams or workflow reads (Brook)")
    for item in items:
        if not isinstance(item, pytest.Function) or "tests/streams/" not in item.nodeid:
            continue
        params = getattr(item, "callspec", None)
        args = inspect.signature(item.function).parameters
        if (
            "test_workflow_reads" in item.nodeid
            or (params and params.params.get("stream_client") == "native")
            or ("client" in args and "stream_client" not in args)
        ):
            item.add_marker(skip)


@pytest.fixture
def redis_url() -> Iterator[str]:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = subprocess.Popen(
        ["redis-server", "--port", str(port), "--save", "", "--appendonly", "no"],
        stdout=subprocess.DEVNULL,
    )
    try:
        for _ in range(100):
            try:
                socket.create_connection(("127.0.0.1", port)).close()
                break
            except OSError:
                time.sleep(0.05)
        yield f"redis://127.0.0.1:{port}"
    finally:
        server.kill()
        server.wait()


def redis_client(
    client: Client, redis_url: str, retention: timedelta = timedelta(days=7)
) -> Client:
    """A client whose default stream backend is Redis."""
    return Client(
        client.service_client,
        namespace=client.namespace,
        stream_backends=StreamBackends(
            [RedisStreamBackend(redis_url, retention=retention)]
        ),
    )


@pytest_asyncio.fixture(params=["redis", "native"])  # type: ignore[reportUntypedFunctionDecorator]
async def stream_client(request: pytest.FixtureRequest, client: Client) -> Client:
    """A client, and so its workers, whose default stream backend is each backend: Redis, and
    native with no configuration.
    """
    if request.param == "redis":
        return redis_client(client, request.getfixturevalue("redis_url"))
    return client


def is_native(client: Client) -> bool:
    return default_backend(client) == NATIVE
