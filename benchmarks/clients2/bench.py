import asyncio
import logging
import sys
import time
from typing import Sequence

from redis.asyncio.cluster import RedisCluster

import aioredis_cluster

BENCH_PREFIX = f"bench:clients2:{int(time.time())}:"
KEY_16B = "0123456789abcdef"
KEY_1KB = KEY_16B * 64
KEY_64KB = KEY_1KB * 64
KEY_1MB = KEY_1KB * 1024
KEY_16MB = KEY_1MB * 16

logger = logging.getLogger(__name__)


class Timer:
    def __init__(self) -> None:
        self.start_t = 0.0
        self.stop_t = 0.0
        self.elapsed = 0.0

    def __repr__(self) -> str:
        return f"<Timer start_t={self.start_t} stop_t={self.stop_t} elapsed={self.elapsed}>"

    def __enter__(self) -> None:
        self.start_t = time.perf_counter()

    def __exit__(self, t, v, tb):
        self.stop_t = time.perf_counter()
        self.elapsed = self.stop_t - self.start_t


class BaseCases:
    _prefix: str

    def name(self) -> str:
        return self.__class__.__name__

    async def test_incr(self) -> None:
        pass

    async def test_set_16b(self) -> None:
        pass

    async def test_get_16b(self) -> None:
        pass

    async def test_set_1kb(self) -> None:
        pass

    async def test_set_64kb(self) -> None:
        pass

    async def test_set_1mb(self) -> None:
        pass

    async def test_get_1mb(self) -> None:
        pass

    async def test_set_16mb(self) -> None:
        pass

    async def test_get_16mb(self) -> None:
        pass

    def key_incr(self) -> str:
        return self._prefix + ":incr"

    def key_16b(self, idx: int) -> str:
        return f"{self._prefix}:16b:{idx}"

    def key_1kb(self, idx: int) -> str:
        return f"{self._prefix}:1kb:{idx}"

    def key_64kb(self, idx: int) -> str:
        return f"{self._prefix}:64kb:{idx}"

    def key_1mb(self, idx: int) -> str:
        return f"{self._prefix}:1mb:{idx}"

    def key_16mb(self, idx: int) -> str:
        return f"{self._prefix}:16mb:{idx}"


class AioredisClusterCases(BaseCases):
    def __init__(self, client: aioredis_cluster.RedisCluster) -> None:
        self._client = client
        self._prefix = BENCH_PREFIX + self.__class__.__name__

    async def test_incr(self) -> None:
        key = self.key_incr()
        for i in range(10000):
            value = await self._client.incr(key)
        assert value == 10000, value

    async def test_set_16b(self) -> None:
        for i in range(10000):
            key = self.key_16b(i)
            await self._client.set(key, KEY_16B)

    async def test_get_16b(self) -> None:
        for i in range(10000):
            key = self.key_16b(i)
            await self._client.get(key)

    async def test_set_1kb(self) -> None:
        for i in range(10000):
            key = self.key_1kb(i)
            await self._client.set(key, KEY_1KB)

    async def test_set_64kb(self) -> None:
        for j in range(10):
            for i in range(1000):
                key = self.key_64kb(i)
                await self._client.set(key, KEY_64KB)

    async def test_set_1mb(self) -> None:
        for j in range(100):
            for i in range(10):
                key = self.key_1mb(i)
                await self._client.set(key, KEY_1MB)

    async def test_get_1mb(self) -> None:
        for j in range(1000):
            for i in range(10):
                key = self.key_1mb(i)
                await self._client.get(key)

    async def test_set_16mb(self) -> None:
        for j in range(10):
            for i in range(10):
                key = self.key_16mb(i)
                await self._client.set(key, KEY_16MB)

    async def test_get_16mb(self) -> None:
        for j in range(100):
            for i in range(10):
                key = self.key_16mb(i)
                await self._client.get(key)


class RedisPyCases(BaseCases):
    def __init__(self, client: RedisCluster) -> None:
        self._client = client
        self._prefix = BENCH_PREFIX + self.__class__.__name__

    async def test_incr(self) -> None:
        key = self.key_incr()
        for i in range(10000):
            value = await self._client.incr(key)
        assert value == 10000, value

    async def test_set_16b(self) -> None:
        for i in range(10000):
            key = self.key_16b(i)
            await self._client.set(key, KEY_16B)

    async def test_get_16b(self) -> None:
        for i in range(10000):
            key = self.key_16b(i)
            await self._client.get(key)

    async def test_set_1kb(self) -> None:
        for i in range(10000):
            key = self.key_1kb(i)
            await self._client.set(key, KEY_1KB)

    async def test_set_64kb(self) -> None:
        for j in range(10):
            for i in range(1000):
                key = self.key_64kb(i)
                await self._client.set(key, KEY_64KB)

    async def test_set_1mb(self) -> None:
        for j in range(100):
            for i in range(10):
                key = self.key_1mb(i)
                await self._client.set(key, KEY_1MB)

    async def test_get_1mb(self) -> None:
        for j in range(1000):
            for i in range(10):
                key = self.key_1mb(i)
                await self._client.get(key)

    async def test_set_16mb(self) -> None:
        for j in range(10):
            for i in range(10):
                key = self.key_16mb(i)
                await self._client.set(key, KEY_16MB)

    async def test_get_16mb(self) -> None:
        for j in range(100):
            for i in range(10):
                key = self.key_16mb(i)
                await self._client.get(key)


async def run_cases(cases: BaseCases) -> None:
    cases_names = (
        "incr",
        "set_16b",
        "get_16b",
        "set_1kb",
        "set_64kb",
        "set_1mb",
        "get_1mb",
        "set_16mb",
        "get_16mb",
    )
    for case in cases_names:
        attr = "test_" + case
        case_t = Timer()
        with case_t:
            await getattr(cases, attr)()
        print(f"{cases.name()}.{attr}: {case_t.elapsed:.03f} secs")


async def bench_aioredis_cluster(
    *,
    startup_nodes: Sequence[str],
):
    client = await aioredis_cluster.create_redis_cluster(startup_nodes)
    try:
        cases = AioredisClusterCases(client)
        await run_cases(cases)
    finally:
        client.close()
        await client.wait_closed()


async def bench_redis_py(
    *,
    startup_nodes: Sequence[str],
):
    client = RedisCluster.from_url(startup_nodes[0])
    await client.initialize()
    try:
        cases = RedisPyCases(client)
        await run_cases(cases)
    finally:
        await client.close()


def main():
    startup_nodes = [sys.argv[1]]

    logging.basicConfig(
        level=logging.WARNING,
    )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        bench_aioredis_cluster(
            startup_nodes=startup_nodes,
        )
    )
    loop.run_until_complete(
        bench_redis_py(
            startup_nodes=startup_nodes,
        )
    )
    loop.close()


if __name__ == "__main__":
    main()
