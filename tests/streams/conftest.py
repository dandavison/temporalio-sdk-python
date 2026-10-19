"""Fixtures of the stream tests: a Redis server per test, and a client for each backend."""

from __future__ import annotations

import socket
import subprocess
import time
from collections.abc import Iterator
from datetime import timedelta

import pytest
import pytest_asyncio

from temporalio.client import Client
from temporalio.contrib.redis_streams import RedisStreamsPlugin
from temporalio.streams.backend import client_backend
from temporalio.streams.native import NativeStreamBackend


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
    return Client(
        client.service_client,
        namespace=client.namespace,
        plugins=[RedisStreamsPlugin(redis_url, retention=retention)],
    )


@pytest_asyncio.fixture(params=["redis", "native"])  # type: ignore[reportUntypedFunctionDecorator]
async def stream_client(request: pytest.FixtureRequest, client: Client) -> Client:
    """A client, and so its workers, using each backend: Redis through the plugin, and native
    with no setup.
    """
    if request.param == "redis":
        return redis_client(client, request.getfixturevalue("redis_url"))
    return client


def is_native(client: Client) -> bool:
    return isinstance(client_backend(client), NativeStreamBackend)
