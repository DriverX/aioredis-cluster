from typing import Awaitable, Callable, Protocol, Tuple, TypeVar, Union

from aioredis_cluster.abc import AbcConnection, AbcPool
from aioredis_cluster.aioredis import Redis

TRedis = TypeVar("TRedis", bound=Redis)

BytesOrStr = Union[bytes, str]
AioredisAddress = Union[str, Tuple[str, int]]
CommandsFactory = Callable[[AbcConnection], TRedis]
PoolerBatchCallback = Callable[[AbcPool], Awaitable[None]]
PoolCreator = Callable[[AioredisAddress], Awaitable[AbcPool]]
PubsubResponse = Tuple[bytes, bytes, int]


class PClosableConnection(Protocol):
    @property
    def closed(self) -> bool:
        ...

    def close(self) -> None:
        ...

    async def wait_closed(self) -> None:
        ...
