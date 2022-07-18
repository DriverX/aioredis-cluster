import asyncio
import collections
import logging
import types
from typing import Deque, List, Optional, Set, Tuple, Type, Union

from aioredis_cluster._aioredis.pool import (
    _AsyncConnectionContextManager,
    _ConnectionContextManager,
)
from aioredis_cluster._aioredis.util import CloseEvent
from aioredis_cluster.abc import AbcConnection, AbcPool
from aioredis_cluster.aioredis import PoolClosedError, create_connection
from aioredis_cluster.command_info.commands import (
    BLOCKING_COMMANDS,
    PUBSUB_COMMANDS,
)
from aioredis_cluster.connection import RedisConnection
from aioredis_cluster.errors import ConnectTimeoutError


logger = logging.getLogger(__name__)


TBytesOrStr = Union[bytes, str]


class ConnectionsPool(AbcPool):
    """Redis connections pool for cluster."""

    def __init__(
        self,
        address: Union[str, Tuple[str, int]],
        db: int = None,
        password=None,
        encoding=None,
        *,
        minsize: int,
        maxsize: int,
        ssl=None,
        parser=None,
        create_connection_timeout: float = None,
        connection_cls: Optional[Type[AbcConnection]] = None,
        loop: asyncio.AbstractEventLoop = None,
        readonly: bool = None,
    ):
        if not isinstance(minsize, int) or minsize < 0:
            raise ValueError(f"minsize must be int >= 0 ({minsize!r}, {type(minsize)}")

        if maxsize is None:
            raise TypeError("Arbitrary pool size is disallowed.")

        if not isinstance(maxsize, int) or maxsize <= 0:
            raise ValueError(f"maxsize must be int > 0 ({maxsize!r}, {type(maxsize)})")

        if minsize > maxsize:
            raise ValueError(f"Invalid pool min/max sizes minsize={minsize}, maxsize={maxsize}")

        self._address = address
        if db is None:
            db = 0
        elif db != 0:
            raise ValueError("`db` for cluster must be 0")
        self._db = db
        self._password = password
        if readonly is None:
            readonly = False
        self._readonly = readonly
        self._ssl = ssl
        self._encoding = encoding
        self._parser_class = parser
        self._minsize = minsize
        self._maxsize = maxsize
        self._create_connection_timeout = create_connection_timeout
        self._pool: Deque[AbcConnection] = collections.deque(maxlen=maxsize)
        self._released: Deque[AbcConnection] = collections.deque(maxlen=maxsize)
        self._used: Set[AbcConnection] = set()
        self._acquiring = 0
        self._conn_waiters_count = 0
        self._release_conn_waiter: Optional[asyncio.Future] = None
        self._cond = asyncio.Condition(lock=asyncio.Lock())
        self._close_state = CloseEvent(self._do_close)
        self._pubsub_conn: Optional[AbcConnection] = None
        if connection_cls is None:
            connection_cls = RedisConnection
        self._connection_cls = connection_cls

    def __repr__(self) -> str:
        pubsub_exists = 0
        if self._pubsub_conn and not self._pubsub_conn.closed:
            pubsub_exists = 1

        return (
            f"<{self.__class__.__name__} "
            f"address:{self.address}, "
            f"db:{self.db}, "
            f"minsize:{self.minsize}, "
            f"maxsize:{self.maxsize}, "
            f"acquiring:{self._acquiring}, "
            f"conn_waiters:{self._conn_waiters_count}, "
            f"used:{len(self._used)}, "
            f"pubsub:{pubsub_exists}, "
            f"free:{self.freesize}, "
            f"size:{self.size + pubsub_exists}>"
        )

    @property
    def minsize(self) -> int:
        """Minimum pool size."""
        return self._minsize

    @property
    def maxsize(self) -> int:
        """Maximum pool size."""
        return self._maxsize

    @property
    def size(self) -> int:
        """Current pool size."""
        return self.freesize + len(self._used) + self._acquiring

    @property
    def freesize(self) -> int:
        """Current number of free connections."""
        return len(self._pool)

    @property
    def address(self):
        return self._address

    async def clear(self) -> None:
        """Clear pool connections.

        Close and remove all free connections.
        """
        async with self._cond:
            await self._do_clear()

    async def _do_clear(self):
        waiters = []
        while self._pool:
            conn = self._pool.popleft()
            conn.close()
            waiters.append(conn.wait_closed())
        await asyncio.gather(*waiters)

    async def _do_close(self):
        if self._release_conn_waiter:
            await self._release_conn_waiter

        async with self._cond:
            assert not self._acquiring, self._acquiring
            waiters: List[asyncio.Future] = []
            while self._pool:
                conn = self._pool.popleft()
                conn.close()
                waiters.append(conn.wait_closed())
            for conn in self._used:
                conn.close()
                waiters.append(conn.wait_closed())
            if self._pubsub_conn:
                self._pubsub_conn.close()
                waiters.append(self._pubsub_conn.wait_closed())
                self._pubsub_conn = None
            await asyncio.gather(*waiters)
            logger.info("Closed %d connection(s)", len(waiters))

    def close(self) -> None:
        """Close all free and in-progress connections and mark pool as closed."""
        if not self._close_state.is_set():
            self._close_state.set()

    @property
    def closed(self) -> bool:
        """True if pool is closed."""
        return self._close_state.is_set()

    async def wait_closed(self) -> None:
        """Wait until pool gets closed."""
        await self._close_state.wait()

    @property
    def db(self) -> int:
        """Currently selected db index."""
        return self._db

    @property
    def encoding(self) -> Optional[str]:
        """Current set codec or None."""
        return self._encoding

    async def execute(self, command: TBytesOrStr, *args, **kw):
        """Executes redis command in a free connection and returns
        future waiting for result.

        Picks connection from free pool and send command through
        that connection.
        If no connection is found, returns coroutine waiting for
        free connection to execute command.
        """
        command = command.upper().strip()
        if command in PUBSUB_COMMANDS:
            raise ValueError(f"PUB/SUB command {command!r} is prohibited for use with .execute()")

        if command in BLOCKING_COMMANDS:
            coro = self._wait_execute(self._address, command, args, kw)
            res = await self._check_result(coro, command, args, kw)
        else:
            conn, address = self.get_connection(command, args)
            if conn is not None:
                fut = conn.execute(command, *args, **kw)
                res = await self._check_result(fut, command, args, kw)
            else:
                coro = self._wait_execute(address, command, args, kw)
                res = await self._check_result(coro, command, args, kw)
        return res

    async def execute_pubsub(self, command: TBytesOrStr, *channels):
        """Executes Redis (p)subscribe/(p)unsubscribe commands.

        ConnectionsPool picks separate connection for pub/sub
        and uses it until explicitly closed or disconnected
        (unsubscribing from all channels/patterns will leave connection
         locked for pub/sub use).

        There is no auto-reconnect for this PUB/SUB connection.

        Returns asyncio.gather coroutine waiting for all channels/patterns
        to receive answers.
        """

        self._check_closed()

        if self._pubsub_conn is None or self._pubsub_conn.closed:
            async with self._cond:
                self._check_closed()
                if self._pubsub_conn and self._pubsub_conn.closed:
                    closing_conn = self._pubsub_conn
                    self._pubsub_conn = None
                    await closing_conn.wait_closed()
                if self._pubsub_conn is None:
                    self._pubsub_conn = await self._create_new_connection(self._address)

        res = await self._pubsub_conn.execute_pubsub(command, *channels)
        return res

    def get_connection(self, command: TBytesOrStr, args=()):
        """Get free connection from pool.

        Returns connection.
        """
        # TODO: find a better way to determine if connection is free
        #       and not havily used.

        command = command.upper().strip()
        is_pubsub = command in PUBSUB_COMMANDS
        if is_pubsub:
            if self._pubsub_conn and not self._pubsub_conn.closed:
                return self._pubsub_conn, self._pubsub_conn.address
            return None, self._address

        for i in range(self.freesize):
            conn = self._pool[0]
            self._pool.rotate(1)
            if conn.closed:  # or conn._waiters: (eg: busy connection)
                continue
            if conn.in_pubsub:
                continue
            if is_pubsub:
                self._pubsub_conn = conn
                self._pool.remove(conn)
            return conn, conn.address
        return None, self._address  # figure out

    def _check_result(self, fut, *data):
        """Hook to check result or catch exception (like MovedError).

        This method can be coroutine.
        """
        return fut

    async def _wait_execute(self, address, command, args, kw):
        """Acquire connection and execute command."""
        conn = await self.acquire(command, args)
        try:
            return await conn.execute(command, *args, **kw)
        finally:
            self.release(conn)

    def _check_closed(self) -> None:
        if self.closed:
            raise PoolClosedError("Pool is closed")

    async def select(self, db):
        """For cluster implementation this method is unavailable"""
        raise NotImplementedError("Feature is blocked in cluster mode")

    async def auth(self, password) -> None:
        self._password = password
        async with self._cond:
            for conn in tuple(self._pool):
                await conn.auth(password)

    @property
    def readonly(self) -> bool:
        return self._readonly

    async def set_readonly(self, value: bool) -> None:
        self._readonly = value
        async with self._cond:
            for conn in tuple(self._pool):
                await conn.set_readonly(value)

    @property
    def in_pubsub(self):
        if self._pubsub_conn and not self._pubsub_conn.closed:
            return self._pubsub_conn.in_pubsub
        return 0

    @property
    def pubsub_channels(self):
        if self._pubsub_conn and not self._pubsub_conn.closed:
            return self._pubsub_conn.pubsub_channels
        return types.MappingProxyType({})

    @property
    def pubsub_patterns(self):
        if self._pubsub_conn and not self._pubsub_conn.closed:
            return self._pubsub_conn.pubsub_patterns
        return types.MappingProxyType({})

    async def acquire(self, command=None, args=()) -> AbcConnection:
        """Acquires a connection from free pool.

        Creates new connection if needed.
        """
        self._check_closed()

        async with self._cond:
            self._check_closed()

            acquire_after_wait = False
            while True:
                try:
                    await self._fill_free(override_min=True)
                except (asyncio.CancelledError, Exception):
                    if acquire_after_wait:
                        self._cond.notify(1)
                    raise

                if self.freesize:
                    if self._conn_waiters_count == 0 or acquire_after_wait:
                        conn = self._pool.popleft()
                        assert not conn.closed, conn
                        assert conn not in self._used, (conn, self._used)
                        self._used.add(conn)
                        return conn

                self._conn_waiters_count += 1
                try:
                    await self._cond.wait()
                finally:
                    self._conn_waiters_count -= 1
                acquire_after_wait = True

    def release(self, conn: AbcConnection) -> None:
        """Returns used connection back into pool.

        When returned connection has db index that differs from one in pool
        the connection will be closed and dropped.
        When queue of free connections is full the connection will be dropped.
        """
        assert conn in self._used, ("Invalid connection, maybe from other pool", conn)
        self._released.append(conn)
        if not conn.closed:
            if conn.in_transaction:
                logger.warning("Connection %r is in transaction, closing it.", conn)
                conn.close()
            elif conn.in_pubsub:
                logger.warning("Connection %r is in subscribe mode, closing it.", conn)
                conn.close()
            elif conn._waiters:
                logger.warning("Connection %r has pending commands, closing it.", conn)
                conn.close()
            elif conn.readonly != self._readonly:
                conn.close()
            elif conn.db == self.db:
                if self.freesize >= self.maxsize:
                    # consider this connection as old and close it.
                    conn.close()
            else:
                conn.close()

        if self._release_conn_waiter is None:
            self._release_conn_waiter = asyncio.ensure_future(self._wakeup())

    async def _drop_closed(self) -> None:
        close_waiters: Set[asyncio.Future] = set()
        for i in range(self.freesize):
            conn = self._pool[0]
            if conn.closed:
                self._pool.popleft()
                close_waiters.add(asyncio.ensure_future(conn.wait_closed()))
            else:
                self._pool.rotate(-1)
        if close_waiters:
            await asyncio.wait(close_waiters)

    async def _fill_free(self, *, override_min):
        # drop closed connections first
        await self._drop_closed()
        while self.size < self.minsize:
            self._acquiring += 1
            try:
                conn = await self._create_new_connection(self._address)
                # check the healthy of that connection, if
                # something went wrong just trigger the Exception
                await conn.execute(b"PING")
                self._pool.append(conn)
            finally:
                self._acquiring -= 1
                # connection may be closed at yield point
                await self._drop_closed()

        if not self._pool and override_min:
            while not self._pool and self.size < self.maxsize:
                self._acquiring += 1
                try:
                    conn = await self._create_new_connection(self._address)
                    self._pool.append(conn)
                finally:
                    self._acquiring -= 1
                    # connection may be closed at yield point
                    await self._drop_closed()

    async def _create_new_connection(self, address) -> AbcConnection:
        try:
            conn: AbcConnection = await create_connection(
                address,
                db=self._db,
                password=self._password,
                ssl=self._ssl,
                encoding=self._encoding,
                parser=self._parser_class,
                timeout=self._create_connection_timeout,
                connection_cls=self._connection_cls,
            )
        except asyncio.TimeoutError:
            raise ConnectTimeoutError(address)

        if self._readonly:
            try:
                await conn.set_readonly(self._readonly)
            except (asyncio.CancelledError, Exception):
                conn.close()
                await conn.wait_closed()
                raise

        return conn

    async def _wakeup(self) -> None:
        async with self._cond:
            logger.debug("Released %d connections", len(self._released))
            self._release_conn_waiter = None
            self._pool.extend(self._released)
            self._used.difference_update(self._released)
            self._cond.notify(len(self._released))
            self._released.clear()

    def __enter__(self):
        raise RuntimeError("'await' should be used as a context manager expression")

    def __exit__(self, *args):
        pass  # pragma: nocover

    def __await__(self):
        # To make `with await pool` work
        conn = yield from self.acquire().__await__()
        return _ConnectionContextManager(self, conn)

    def get(self):
        """Return async context manager for working with connection.

        async with pool.get() as conn:
            await conn.execute('get', 'my-key')
        """
        return _AsyncConnectionContextManager(self)
