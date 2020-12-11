import pytest

from aioredis_cluster import RedisClusterError
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
