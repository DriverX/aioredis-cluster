import asyncio
import logging
import os
from typing import List, Tuple, Union

import pytest

from aioredis_cluster import create_cluster, create_redis_cluster
from aioredis_cluster.util import unused_port as _unused_port


def get_startup_nodes(nodes_str: str) -> List[Union[str, Tuple[str, int]]]:
    if not nodes_str:
        return []

    nodes: List[Union[str, Tuple[str, int]]] = []
    for node_str in nodes_str.split(","):
        node_str = node_str.strip()
        if not node_str.startswith("redis://"):
            nodes.append((node_str.split(":", 1)[0], int(node_str.split(":", 1)[1])))
        else:
            nodes.append(node_str)

    return nodes


STARTUP_NODES = tuple(get_startup_nodes(os.environ.get("REDIS_CLUSTER_STARTUP_NODES", "").strip()))


def pytest_runtest_setup(item):
    if not STARTUP_NODES:
        pytest.skip("Environment variable REDIS_CLUSTER_STARTUP_NODES is not defined")


def pytest_collection_modifyitems(items):
    for item in items:
        if not item.get_closest_marker("asyncio") and asyncio.iscoroutinefunction(item.function):
            item.add_marker(pytest.mark.asyncio)


@pytest.fixture
def unused_port():
    return _unused_port


@pytest.fixture
async def redis_cluster():
    _client = None
    _client_kwargs = None

    async def factory(**kwargs):
        nonlocal _client, _client_kwargs
        _client_kwargs = kwargs
        _client = await create_redis_cluster(STARTUP_NODES, **kwargs)
        return _client

    yield factory

    if _client:
        if _client.closed:
            _client = await factory(**_client_kwargs)

        try:
            for pool in await _client.all_masters():
                await pool.flushdb()

            _client.close()
            await _client.wait_closed()
        except Exception:
            logging.exception("Unable to cleanup redis cluster nodes")


@pytest.fixture
async def cluster():
    _client = None
    _client_kwargs = None

    async def factory(**kwargs):
        nonlocal _client, _client_kwargs
        _client_kwargs = kwargs
        _client = await create_cluster(STARTUP_NODES, **kwargs)
        return _client

    yield factory

    if _client:
        if _client.closed:
            _client = await factory(**_client_kwargs)

        try:
            for pool in await _client.all_masters():
                await pool.execute("FLUSHDB")

            _client.close()
            await _client.wait_closed()
        except Exception:
            logging.exception("Unable to cleanup redis cluster nodes")
