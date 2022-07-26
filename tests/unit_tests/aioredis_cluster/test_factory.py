import warnings

import mock
import pytest

from aioredis_cluster.factory import create_cluster, create_redis_cluster
from aioredis_cluster.structs import Address


@pytest.fixture
def cluster_fix():
    def factory():
        cluster = mock.NonCallableMock()
        cluster._init = mock.AsyncMock()
        cluster.wait_closed = mock.AsyncMock()
        return cluster

    return factory


async def test_create_cluster__defaults(mocker, cluster_fix):
    cluster = cluster_fix()
    mocked_cluster_cls = mocker.patch(
        create_cluster.__module__ + ".Cluster",
        return_value=cluster,
    )

    result = await create_cluster(
        [
            "redis://redis1?db=1",
            ("redis2", 6380),
        ]
    )

    assert result is cluster
    mocked_cluster_cls.assert_called_once()
    assert len(mocked_cluster_cls.call_args[0][0]) == 2
    assert Address("redis1", 6379) in mocked_cluster_cls.call_args[0][0]
    assert Address("redis2", 6380) in mocked_cluster_cls.call_args[0][0]
    assert mocked_cluster_cls.call_args == mock.call(
        mock.ANY,
        retry_min_delay=None,
        retry_max_delay=None,
        max_attempts=None,
        state_reload_interval=None,
        follow_cluster=None,
        idle_connection_timeout=None,
        username=None,
        password=None,
        encoding=None,
        pool_minsize=None,
        pool_maxsize=None,
        connect_timeout=None,
        attempt_timeout=None,
        ssl=None,
    )
    cluster._init.assert_called_once()
    cluster.wait_closed.assert_not_called()


async def test_create_cluster__parametrized(mocker, cluster_fix):
    cluster = cluster_fix()
    mocked_cluster_cls = mocker.patch(
        create_cluster.__module__ + ".Cluster",
        return_value=cluster,
    )

    result = await create_cluster(
        ("redis://redis1?db=1",),
        retry_min_delay=1.2,
        retry_max_delay=2.3,
        max_attempts=8,
        attempt_timeout=5.5,
        state_reload_frequency=7.8,
        state_reload_interval=8.7,
        follow_cluster=True,
        idle_connection_timeout=1.1,
        username="USER",
        password="PASSWD",
        encoding="utf-8",
        pool_minsize=1,
        pool_maxsize=5,
        connect_timeout=3.5,
    )

    assert result is cluster
    mocked_cluster_cls.assert_called_once()
    assert mocked_cluster_cls.call_args == mock.call(
        [
            Address("redis1", 6379),
        ],
        retry_min_delay=1.2,
        retry_max_delay=2.3,
        max_attempts=8,
        attempt_timeout=5.5,
        state_reload_interval=8.7,
        follow_cluster=True,
        idle_connection_timeout=1.1,
        username="USER",
        password="PASSWD",
        encoding="utf-8",
        pool_minsize=1,
        pool_maxsize=5,
        connect_timeout=3.5,
        ssl=None,
    )


async def test_create_cluster__warnings(mocker, cluster_fix):
    cluster = cluster_fix()
    mocker.patch(
        create_cluster.__module__ + ".Cluster",
        return_value=cluster,
    )

    with warnings.catch_warnings(record=True) as ws:
        await create_cluster(
            ("redis://redis1?db=1",),
            state_reload_frequency=7.8,
        )

    assert len(ws) >= 1
    found_warnings = [w for w in ws if "state_reload_frequency" in str(w.message)]
    assert len(found_warnings) == 1
    assert issubclass(found_warnings[0].category, DeprecationWarning)


async def test_create_redis_cluster(mocker, cluster_fix):
    cluster = cluster_fix()
    mocker.patch(
        create_redis_cluster.__module__ + ".Cluster",
        return_value=cluster,
    )
    commands_factory = mock.Mock()

    result = await create_redis_cluster(
        ["redis://redis1"],
        cluster_commands_factory=commands_factory,
    )

    assert result is commands_factory.return_value
    commands_factory.assert_called_once_with(cluster)
