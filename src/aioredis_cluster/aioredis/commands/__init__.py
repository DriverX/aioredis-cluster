import asyncio
from typing import Any, Optional, Tuple, Union

from aioredis_cluster.aioredis.connection import create_connection
from aioredis_cluster.aioredis.pool import create_pool

try:
    from aioredis.commands import (
        ContextRedis,
        GeoMember,
        GeoPoint,
        MultiExec,
        Pipeline,
        Redis,
    )
except ImportError:
    from aioredis_cluster._aioredis.commands import (
        ContextRedis,
        GeoMember,
        GeoPoint,
        MultiExec,
        Pipeline,
        Redis,
    )


(ContextRedis,)


__all__ = (
    "create_redis",
    "create_redis_pool",
    "Redis",
    "Pipeline",
    "MultiExec",
    "GeoPoint",
    "GeoMember",
)


async def create_redis(
    address: Union[Tuple[str, int], str],
    *,
    db: Optional[int] = None,
    password: Optional[str] = None,
    ssl: Optional[Any] = None,
    encoding: Optional[str] = None,
    commands_factory: Redis = Redis,
    parser=None,
    timeout: Optional[float] = None,
    connection_cls=None,
    loop: Optional[asyncio.AbstractEventLoop] = None,
) -> Redis:
    """Creates high-level Redis interface.

    This function is a coroutine.
    """
    conn = await create_connection(
        address,
        db=db,
        password=password,
        ssl=ssl,
        encoding=encoding,
        parser=parser,
        timeout=timeout,
        connection_cls=connection_cls,
    )
    return commands_factory(conn)


async def create_redis_pool(
    address: Union[Tuple[str, int], str],
    *,
    db: Optional[int] = None,
    password: Optional[str] = None,
    ssl: Optional[Any] = None,
    encoding: Optional[str] = None,
    commands_factory: Redis = Redis,
    minsize: int = 1,
    maxsize: int = 10,
    parser=None,
    timeout: Optional[float] = None,
    pool_cls=None,
    connection_cls=None,
    loop: Optional[asyncio.AbstractEventLoop] = None,
) -> Redis:
    """Creates high-level Redis interface.

    This function is a coroutine.
    """
    pool = await create_pool(
        address,
        db=db,
        password=password,
        ssl=ssl,
        encoding=encoding,
        minsize=minsize,
        maxsize=maxsize,
        parser=parser,
        create_connection_timeout=timeout,
        pool_cls=pool_cls,
        connection_cls=connection_cls,
    )
    return commands_factory(pool)
