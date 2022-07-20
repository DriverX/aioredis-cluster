import asyncio
import functools
import time

import aredis
import redis.asyncio as redispy
import uvloop

import aioredis_cluster


def timer(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        tic = time.perf_counter()
        await func(*args, **kwargs)
        toc = time.perf_counter()
        return f"{func.__name__}: {toc - tic:.4f}"

    return wrapper


@timer
async def set_str(prefix, client, gather, data):
    if gather:
        for _ in range(count // 100):
            await asyncio.gather(
                *(
                    asyncio.create_task(client.set(f"{prefix}:bench:str_{i}", data))
                    for i in range(100)
                )
            )
    else:
        for i in range(count):
            await client.set(f"{prefix}:bench:str_{i}", data)


@timer
async def set_int(prefix, client, gather, data):
    if gather:
        for _ in range(count // 100):
            await asyncio.gather(
                *(
                    asyncio.create_task(client.set(f"{prefix}:bench:int_{i}", data))
                    for i in range(100)
                )
            )
    else:
        for i in range(count):
            await client.set(f"{prefix}:bench:int_{i}", data)


@timer
async def get_str(prefix, client, gather):
    if gather:
        for _ in range(count // 100):
            await asyncio.gather(
                *(asyncio.create_task(client.get(f"{prefix}:bench:str_{i}")) for i in range(100))
            )
    else:
        for i in range(count):
            await client.get(f"{prefix}:bench:str_{i}")


@timer
async def get_int(prefix, client, gather):
    if gather:
        for _ in range(count // 100):
            await asyncio.gather(
                *(asyncio.create_task(client.get(f"{prefix}:bench:int_{i}")) for i in range(100))
            )
    else:
        for i in range(count):
            await client.get(f"{prefix}:bench:int_{i}")


@timer
async def hset(prefix, client, gather, data):
    if gather:
        for _ in range(count // 100):
            await asyncio.gather(
                *(
                    asyncio.create_task(client.hset(f"{prefix}:bench:hset", str(i), data))
                    for i in range(100)
                )
            )
    else:
        for i in range(count):
            await client.hset(f"{prefix}:bench:hset", str(i), data)


@timer
async def hget(prefix, client, gather):
    if gather:
        for _ in range(count // 100):
            await asyncio.gather(
                *(
                    asyncio.create_task(client.hget(f"{prefix}:bench:hset", str(i)))
                    for i in range(100)
                )
            )
    else:
        for i in range(count):
            await client.hget(f"{prefix}:bench:hset", str(i))


@timer
async def incr(prefix, client, gather):
    if gather:
        for _ in range(count // 100):
            await asyncio.gather(
                *(asyncio.create_task(client.incr(f"{prefix}:bench:incr")) for i in range(100))
            )
    else:
        for i in range(count):
            await client.incr(f"{prefix}:bench:incr")


@timer
async def lpush(prefix, client, gather, data):
    if gather:
        for _ in range(count // 100):
            await asyncio.gather(
                *(
                    asyncio.create_task(client.lpush(f"{prefix}:bench:lpush", data))
                    for i in range(100)
                )
            )
    else:
        for i in range(count):
            await client.lpush(f"{prefix}:bench:lpush", data)


@timer
async def lrange_300(prefix, client, gather):
    if gather:
        for _ in range(count // 100):
            await asyncio.gather(
                *(
                    asyncio.create_task(client.lrange(f"{prefix}:bench:lpush", i, i + 300))
                    for i in range(100)
                )
            )
    else:
        for i in range(count):
            await client.lrange(f"{prefix}:bench:lpush", i, i + 300)


@timer
async def lpop(prefix, client, gather):
    if gather:
        for _ in range(count // 100):
            await asyncio.gather(
                *(asyncio.create_task(client.lpop(f"{prefix}:bench:lpush")) for i in range(100))
            )
    else:
        for i in range(count):
            await client.lpop("bench:lpush")


@timer
async def warmup(prefix, client):
    await asyncio.gather(
        *(asyncio.create_task(client.exists(f"{prefix}:bench:warmup_{i}")) for i in range(100))
    )


@timer
async def run(prefix, client, gather):
    data_str = "a" * size
    data_int = int("1" * size)

    if gather is False:
        for ret in await asyncio.gather(
            asyncio.create_task(set_str(prefix, client, gather, data_str)),
            asyncio.create_task(set_int(prefix, client, gather, data_int)),
            asyncio.create_task(hset(prefix, client, gather, data_str)),
            asyncio.create_task(incr(prefix, client, gather)),
            asyncio.create_task(lpush(prefix, client, gather, data_int)),
        ):
            print(ret)
        for ret in await asyncio.gather(
            asyncio.create_task(get_str(prefix, client, gather)),
            asyncio.create_task(get_int(prefix, client, gather)),
            asyncio.create_task(hget(prefix, client, gather)),
            asyncio.create_task(lrange_300(prefix, client, gather)),
            asyncio.create_task(lpop(prefix, client, gather)),
        ):
            print(ret)
    else:
        print(await set_str(prefix, client, gather, data_str))
        print(await set_int(prefix, client, gather, data_int))
        print(await hset(prefix, client, gather, data_str))
        print(await incr(prefix, client, gather))
        print(await lpush(prefix, client, gather, data_int))

        print(await get_str(prefix, client, gather))
        print(await get_int(prefix, client, gather))
        print(await hget(prefix, client, gather))
        print(await lrange_300(prefix, client, gather))
        print(await lpop(prefix, client, gather))


async def main(loop, gather=None):
    arc = aredis.StrictRedisCluster(
        host=host,
        port=port,
        password=password,
        max_connections=2**31,
        max_connections_per_node=2**31,
        readonly=False,
        reinitialize_steps=count,
        skip_full_coverage_check=True,
        decode_responses=False,
        max_idle_time=count,
        idle_check_interval=count,
    )
    now = int(time.time())
    prefix = f"aredis:{now}"
    print(f"{loop} {gather} {await warmup(prefix, arc)} aredis")
    print(await run(prefix, arc, gather=gather))
    arc.connection_pool.disconnect()

    aiorc = await aioredis_cluster.create_redis_cluster(
        [(host, port)],
        password=password,
        state_reload_interval=count,
        idle_connection_timeout=count,
        pool_maxsize=2**31,
    )
    prefix = f"aioredis-cluster:{now}"
    print(f"{loop} {gather} {await warmup(prefix, aiorc)} aioredis-cluster")
    print(await run(prefix, aiorc, gather=gather))
    aiorc.close()
    await aiorc.wait_closed()

    async with redispy.RedisCluster(
        host=host,
        port=port,
        password=password,
        reinitialize_steps=count,
        read_from_replicas=False,
        decode_responses=False,
        max_connections=2**31,
    ) as rca:
        prefix = f"redispy:{now}"
        print(f"{loop} {gather} {await warmup(prefix, rca)} redispy")
        print(await run(prefix, rca, gather=gather))


if __name__ == "__main__":
    # host = "localhost"
    # port = 16379
    host = "redis-cluster62"
    port = 7000
    password = None

    count = 10000
    size = 256

    asyncio.run(main("asyncio"))
    asyncio.run(main("asyncio", gather=False))
    asyncio.run(main("asyncio", gather=True))

    uvloop.install()

    asyncio.run(main("uvloop"))
    asyncio.run(main("uvloop", gather=False))
    asyncio.run(main("uvloop", gather=True))
