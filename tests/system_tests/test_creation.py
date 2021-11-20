import pytest

from aioredis_cluster import create_cluster, create_redis_cluster


async def test_redis_cluster__unavailable_nodes(unused_port):
    with pytest.raises(OSError):
        await create_redis_cluster(["redis://localhost:{}".format(unused_port())])

    with pytest.raises(OSError):
        await create_redis_cluster(["redis://127.0.0.1:{}".format(unused_port())])


async def test_cluster__unavailable_nodes(unused_port):
    with pytest.raises(OSError):
        await create_cluster(["redis://localhost:{}".format(unused_port())])

    with pytest.raises(OSError):
        await create_cluster(["redis://127.0.0.1:{}".format(unused_port())])
