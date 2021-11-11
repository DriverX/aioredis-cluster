from typing import Awaitable, Callable, List, Tuple, Union, Protocol

from aioredis.commands import Redis

from aioredis_cluster.abc import AbcConnection, AbcPool


AioredisAddress = Union[str, Tuple[str, int]]
SlotsResponse = List[List]
CommandsFactory = Callable[[AbcConnection], Redis]
PoolerBatchCallback = Callable[[AbcPool], Awaitable[None]]
PubsubResponse = Tuple[bytes, bytes, int]


class PoolCreator(Protocol):
    def __call__(
        self,
        addr: AioredisAddress,
        pool_minsize: int = None,
        pool_maxsize: int = None,
    ) -> Awaitable[AbcPool]:
        ...
