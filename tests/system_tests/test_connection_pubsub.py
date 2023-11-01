from string import ascii_letters

import pytest

from aioredis_cluster.errors import MovedError


async def test_moved_with_pubsub(cluster):
    c = await cluster()
    redis = await c.keys_master("a")
    await redis.ssubscribe("a")

    with pytest.raises(MovedError):
        for b in ascii_letters:
            await redis.ssubscribe(b)

    redis.close()
    await redis.wait_closed()
