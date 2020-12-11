import asyncio

import aioredis

from aioredis_cluster.errors import ConnectTimeoutError


__all__ = [
    "ConnectionsPool",
]


class ConnectionsPool(aioredis.ConnectionsPool):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # Workaround https://github.com/aio-libs/aioredis/pull/802
        # aioredis_cluster required python>=3.6.5, where asyncio.Lock already fixed
        self._cond = asyncio.Condition(lock=asyncio.Lock())

    async def _create_new_connection(self, address) -> aioredis.RedisConnection:
        try:
            return await super()._create_new_connection(address)
        except asyncio.TimeoutError:
            raise ConnectTimeoutError(address)
