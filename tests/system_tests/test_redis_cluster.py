import asyncio

import pytest

from aioredis_cluster import RedisCluster, RedisClusterError
from aioredis_cluster.aioredis import Channel
from aioredis_cluster.commands.commands import _blocked_methods


async def test_blocked(redis_cluster):
    cl = await redis_cluster()

    for method_name in _blocked_methods:
        method = getattr(cl, method_name)
        with pytest.raises(NotImplementedError, match="Command is blocked"):
            await method()


async def test_connection(redis_cluster):
    cl = await redis_cluster()

    result = await cl.echo("TEST")
    assert result == b"TEST"

    result = await cl.ping()
    assert result == b"PONG"


async def test_general(redis_cluster):
    """General test for frequentluy used commands"""

    cl = await redis_cluster()

    result = await cl.delete("foo")

    assert result == 0

    await cl.set("foo", "bar")
    result = await cl.delete("foo")

    assert result == 1

    await cl.mset("key1:{slot}", "value1", "key2:{slot}", "value2")
    result = await cl.exists("key1:{slot}", "key2:{slot}", "key3:{slot}")

    assert result == 2

    result = await cl.ttl("key1:{slot}")
    assert result == -1

    result = await cl.pttl("key2:{slot}")
    assert result == -1

    result = await cl.ttl("key3:{slot}")
    assert result == -2

    result = await cl.rename("key1:{slot}", "key3:{slot}")
    assert result == 1

    with pytest.raises(RedisClusterError, match="all keys must map"):
        await cl.rename("key1", "key2")

    result = await cl.touch("key1:{slot}", "key2:{slot}", "key3:{slot}")
    assert result == 2

    await cl.incr("count1")
    for i in range(2):
        await cl.incr("count2")
    for i in range(3):
        await cl.incr("count3")
    for i in range(4):
        await cl.incr("count4")
    for i in range(5):
        await cl.incrby("count5", 2)

    result = await cl.get("count1")
    assert result == b"1"
    result = await cl.get("count2")
    assert result == b"2"
    result = await cl.get("count3")
    assert result == b"3"
    result = await cl.get("count4")
    assert result == b"4"
    result = await cl.get("count5")
    assert result == b"10"

    result = await cl.mget("key1:{slot}", "key2:{slot}", "key3:{slot}")
    assert result == [None, b"value2", b"value1"]

    for i in range(10):
        result = await cl.info()
        assert result["replication"]["role"] == "master"


async def test_eval(redis_cluster):
    cl = await redis_cluster()

    await cl.delete("key:{eval}", "value:{eval}")

    script = "return 42"
    res = await cl.eval(script)
    assert res == 42

    key, value = b"key:{eval}", b"value:{eval}"
    script = """
    if redis.call('setnx', KEYS[1], ARGV[1]) == 1
    then
        return 'foo'
    else
        return 'bar'
    end
    """
    res = await cl.eval(script, keys=[key], args=[value])
    assert res == b"foo"
    res = await cl.eval(script, keys=[key], args=[value])
    assert res == b"bar"

    script = "return 42"
    with pytest.raises(TypeError):
        await cl.eval(script, keys="not:list")

    with pytest.raises(TypeError):
        await cl.eval(script, keys=["valid", None])
    with pytest.raises(TypeError):
        await cl.eval(script, args=["valid", None])
    with pytest.raises(TypeError):
        await cl.eval(None)


async def test_blocking_commands(redis_cluster):
    cl = await redis_cluster()

    b1fut = asyncio.ensure_future(cl.blpop("blpop_key1{slot}", "blpop_key2{slot}", timeout=10))
    b2fut = asyncio.ensure_future(cl.blpop("blpop_key2{slot}", "blpop_key1{slot}", timeout=10))
    await cl.rpush("blpop_key2{slot}", "1")
    await cl.lpush("blpop_key1{slot}", "2")
    b1_result = await b1fut
    b2_result = await b2fut

    assert b1_result == [b"blpop_key2{slot}", b"1"]
    assert b2_result == [b"blpop_key1{slot}", b"2"]


@pytest.mark.redis_version(gte="7.0.0")
async def test_sharded_pubsub(redis_cluster):
    cl1: RedisCluster = await redis_cluster()
    cl2: RedisCluster = await redis_cluster()

    channels = await cl1.ssubscribe("channel1")
    assert len(channels) == 1
    assert isinstance(channels[0], Channel) is True
    ch1: Channel = channels[0]

    channels = await cl1.ssubscribe("channel2")
    assert len(channels) == 1
    ch2: Channel = channels[0]

    assert len(cl1.sharded_pubsub_channels) == 2
    assert cl1.in_pubsub == 1
    assert len(cl1.channels) == 0
    assert len(cl1.patterns) == 0

    await cl2.spublish("channel1", "hello")
    await cl2.spublish("channel2", "world")

    msg1 = await ch1.get()
    assert msg1 == b"hello"

    msg2 = await ch2.get()
    assert msg2 == b"world"

    await cl1.sunsubscribe("channel1")
    await cl1.sunsubscribe("channel2")

    assert len(cl1.sharded_pubsub_channels) == 0
    assert cl1.in_pubsub == 1


@pytest.mark.redis_version(gte="7.0.0")
async def test_sharded_pubsub__multiple_subscribe(redis_cluster):
    cl1: RedisCluster = await redis_cluster()
    cl2: RedisCluster = await redis_cluster()

    ch1: Channel = (await cl1.ssubscribe("channel:{shard_key}:1"))[0]
    ch2: Channel = (await cl1.ssubscribe("channel:{shard_key}:2"))[0]
    ch3: Channel = (await cl1.ssubscribe("channel:{shard_key}:3"))[0]

    assert len(cl1.sharded_pubsub_channels) == 3
    assert cl1.in_pubsub == 1

    shard_pool = await cl1.keys_master("{shard_key}")
    assert len(shard_pool.sharded_pubsub_channels) == 3

    await cl2.spublish("channel:{shard_key}:1", "msg1")
    await cl2.spublish("channel:{shard_key}:2", "msg2")
    await cl2.spublish("channel:{shard_key}:3", "msg3")
    await cl2.spublish("channel:{shard_key}:4", "msg4")

    msg1 = await ch1.get()
    assert msg1 == b"msg1"

    msg2 = await ch2.get()
    assert msg2 == b"msg2"

    msg3 = await ch3.get()
    assert msg3 == b"msg3"
