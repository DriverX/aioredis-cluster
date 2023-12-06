import asyncio
from string import ascii_letters
from typing import Awaitable, Callable

import pytest

from aioredis_cluster import Cluster


@pytest.mark.redis_version(gte="7.0.0")
@pytest.mark.timeout(2)
async def test_moved_with_pubsub(cluster: Callable[[], Awaitable[Cluster]]):
    client = await cluster()
    redis = await client.keys_master("a")
    await redis.ssubscribe("a")

    assert "a" in redis.sharded_pubsub_channels
    assert b"a" in redis.sharded_pubsub_channels

    channels_dump = {}
    for char in ascii_letters:
        await redis.ssubscribe(char)
        # Channel objects creates immediately and close and removed after received MOVED reply
        channels_dump[char] = redis.sharded_pubsub_channels[char]

    await asyncio.sleep(0.01)

    # check number of created Channel objects
    assert len(channels_dump) == len(ascii_letters)

    # check of closed Channels after MovedError reply received
    num_of_closed = 0
    for channel in channels_dump.values():
        if not channel.is_active:
            num_of_closed += 1

    assert num_of_closed > 0
    assert len(redis.sharded_pubsub_channels) < len(channels_dump)

    redis.close()
    await redis.wait_closed()
