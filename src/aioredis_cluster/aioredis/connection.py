import asyncio
import logging
import socket
import ssl
from typing import List, Tuple, Type, Union

from .abc import AbcConnection
from .util import parse_url

try:
    from aioredis.stream import open_connection, open_unix_connection
except ImportError:
    from .._aioredis.stream import open_connection, open_unix_connection
try:
    from aioredis.connection import MAX_CHUNK_SIZE, RedisConnection
except ImportError:
    from .._aioredis.connection import MAX_CHUNK_SIZE, RedisConnection


logger = logging.getLogger(__name__)


async def create_connection(
    address: Union[str, Tuple[str, int], List],
    *,
    db: int = None,
    username: str = None,
    password: str = None,
    ssl: Union[bool, ssl.SSLContext] = None,
    encoding: str = None,
    parser=None,
    timeout: float = None,
    connection_cls: Type[AbcConnection] = None,
    loop=None,
) -> AbcConnection:
    """Creates redis connection.

    Opens connection to Redis server specified by address argument.
    Address argument can be one of the following:
    * A tuple representing (host, port) pair for TCP connections;
    * A string representing either Redis URI or unix domain socket path.

    SSL argument is passed through to asyncio.create_connection.
    By default SSL/TLS is not used.

    By default any timeout is applied at the connection stage, however
    you can set a limitted time used trying to open a connection via
    the `timeout` Kw.

    Encoding argument can be used to decode byte-replies to strings.
    By default no decoding is done.

    Parser parameter can be used to pass custom Redis protocol parser class.
    By default hiredis.Reader is used (unless it is missing or platform
    is not CPython).

    Return value is RedisConnection instance or a connection_cls if it is
    given.

    This function is a coroutine.
    """
    assert isinstance(address, (tuple, list, str)), "tuple or str expected"
    if isinstance(address, str):
        address, options = parse_url(address)
        logger.debug("Parsed Redis URI %r", address)
        db = options.setdefault("db", db)
        username = options.setdefault("username", username)
        password = options.setdefault("password", password)
        encoding = options.setdefault("encoding", encoding)
        timeout = options.setdefault("timeout", timeout)
        if "ssl" in options:
            assert options["ssl"] or (not options["ssl"] and not ssl), (
                "Conflicting ssl options are set",
                options["ssl"],
                ssl,
            )
            ssl = ssl or options["ssl"]

    if timeout is not None and timeout <= 0:
        raise ValueError("Timeout has to be None or a number greater than 0")

    if connection_cls:
        assert issubclass(
            connection_cls, AbcConnection
        ), "connection_class does not meet the AbcConnection contract"
        cls = connection_cls
    else:
        cls = RedisConnection

    if isinstance(address, (list, tuple)):
        host, port = address
        logger.debug("Creating tcp connection to %r", address)
        reader, writer = await asyncio.wait_for(
            open_connection(host, port, limit=MAX_CHUNK_SIZE, ssl=ssl), timeout
        )
        sock = writer.transport.get_extra_info("socket")
        if sock is not None:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            address = sock.getpeername()
        address = tuple(address[:2])  # type: ignore
    else:
        logger.debug("Creating unix connection to %r", address)
        reader, writer = await asyncio.wait_for(
            open_unix_connection(address, ssl=ssl, limit=MAX_CHUNK_SIZE), timeout
        )
        sock = writer.transport.get_extra_info("socket")
        if sock is not None:
            address = sock.getpeername()

    conn = cls(reader, writer, encoding=encoding, address=address, parser=parser)

    try:
        if password is not None:
            if username is not None:
                await conn.auth_with_username(username, password)
            else:
                await conn.auth(password)
        if db is not None:
            await conn.select(db)
    except (asyncio.CancelledError, Exception):
        conn.close()
        await conn.wait_closed()
        raise
    return conn
