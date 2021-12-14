from typing import Awaitable, Callable, Tuple, TypeVar, Union

from aioredis.commands import Redis

from aioredis_cluster.abc import AbcConnection, AbcPool


TRedis = TypeVar("TRedis", bound=Redis)

BytesOrStr = Union[bytes, str]
AioredisAddress = Union[str, Tuple[str, int]]
CommandsFactory = Callable[[AbcConnection], TRedis]
PoolerBatchCallback = Callable[[AbcPool], Awaitable[None]]
PoolCreator = Callable[[AioredisAddress], Awaitable[AbcPool]]
PubsubResponse = Tuple[bytes, bytes, int]
