from unittest import mock

import pytest
from aioredis.commands import ContextRedis

from aioredis_cluster.commands.commands import RedisCluster


async def test_await_commands(mocker):
    mocked_conn_is_cluster = mocker.patch(
        RedisCluster.__module__ + ".conn_is_cluster", return_value=True
    )
    redis = RedisCluster(mock.NonCallableMock())

    with pytest.raises(RuntimeError, match="Can't use for cluster"):
        await redis

    mocked_conn_is_cluster.return_value = False

    result = await redis

    assert isinstance(result, ContextRedis)
