from typing import Awaitable, Callable, List, Tuple, Union

from aioredis.commands import Redis

from aioredis_cluster.abc import AbcConnection, AbcPool


AioredisAddress = Union[str, Tuple[str, int]]
SlotsResponse = List[List]
CommandsFactory = Callable[[AbcConnection], Redis]
PoolerBatchCallback = Callable[[AbcPool], Awaitable[None]]
PoolCreator = Callable[[AioredisAddress], Awaitable[AbcPool]]
PubsubResponse = Tuple[bytes, bytes, int]
