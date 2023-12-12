import asyncio
from string import ascii_letters
from typing import Awaitable, Callable

import pytest

from aioredis_cluster import Cluster
from aioredis_cluster.compat.asyncio import timeout


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

    client.close()
    await client.wait_closed()


@pytest.mark.redis_version(gte="7.0.0")
@pytest.mark.timeout(2)
async def test_immediately_resubscribe(cluster: Callable[[], Awaitable[Cluster]]):
    client = await cluster()
    redis = await client.keys_master("chan")
    for i in range(10):
        await redis.ssubscribe("chan")
        await redis.sunsubscribe("chan")
    chan = (await redis.ssubscribe("chan"))[0]

    chan_get_task = asyncio.ensure_future(chan.get())

    await client.execute("spublish", "chan", "msg1")
    await client.execute("spublish", "chan", "msg2")
    await client.execute("spublish", "chan", "msg3")
    # wait 50ms until Redis message delivery
    await asyncio.sleep(0.05)

    assert chan_get_task.done() is True
    assert chan_get_task.result() == b"msg1"

    # message must be already exists in internal queue
    async with timeout(0):
        msg2 = await chan.get()
    assert msg2 == b"msg2"

    async with timeout(0):
        msg3 = await chan.get()
    assert msg3 == b"msg3"

    # no more messages
    with pytest.raises(asyncio.TimeoutError):
        async with timeout(0):
            await chan.get()

    assert chan.is_active is True

    await redis.sunsubscribe("chan")

    assert chan.is_active is False

    client.close()
    await client.wait_closed()
