import asyncio
import ssl
from typing import List, Tuple, Type, Union

from .abc import AbcConnection, AbcPool
from .util import parse_url


try:
    from aioredis.pool import ConnectionsPool
except ImportError:
    from .._aioredis.pool import ConnectionsPool


async def create_pool(
    address: Union[str, Tuple[str, int], List],
    *,
    db: int = None,
    password: str = None,
    ssl: Union[bool, ssl.SSLContext] = None,
    encoding: str = None,
    minsize: int = 1,
    maxsize: int = 10,
    parser=None,
    create_connection_timeout: float = None,
    pool_cls: Type[AbcPool] = None,
    connection_cls: Type[AbcConnection] = None,
    loop=None,
):
    # FIXME: rewrite docstring
    """Creates Redis Pool.

    By default it creates pool of Redis instances, but it is
    also possible to create pool of plain connections by passing
    ``lambda conn: conn`` as commands_factory.

    *commands_factory* parameter is deprecated since v0.2.9

    All arguments are the same as for create_connection.

    Returns RedisPool instance or a pool_cls if it is given.
    """
    if pool_cls:
        assert issubclass(pool_cls, AbcPool), "pool_class does not meet the AbcPool contract"
        cls = pool_cls
    else:
        cls = ConnectionsPool
    if isinstance(address, str):
        address, options = parse_url(address)
        db = options.setdefault("db", db)
        password = options.setdefault("password", password)
        encoding = options.setdefault("encoding", encoding)
        create_connection_timeout = options.setdefault("timeout", create_connection_timeout)
        if "ssl" in options:
            assert options["ssl"] or (not options["ssl"] and not ssl), (
                "Conflicting ssl options are set",
                options["ssl"],
                ssl,
            )
            ssl = ssl or options["ssl"]
        # TODO: minsize/maxsize

    pool = cls(
        address,
        db,
        password,
        encoding,
        minsize=minsize,
        maxsize=maxsize,
        ssl=ssl,
        parser=parser,
        create_connection_timeout=create_connection_timeout,
        connection_cls=connection_cls,
    )
    try:
        await pool._fill_free(override_min=False)
    except (asyncio.CancelledError, Exception):
        pool.close()
        await pool.wait_closed()
        raise
    return pool
