import asyncio
import collections
import logging
import types
from typing import Deque, Set, Tuple, Union

from aioredis_cluster._aioredis.connection import _PUBSUB_COMMANDS
from aioredis_cluster._aioredis.pool import (
    _AsyncConnectionContextManager,
    _ConnectionContextManager,
)
from aioredis_cluster._aioredis.util import CloseEvent
from aioredis_cluster.abc import AbcPool
from aioredis_cluster.aioredis import (
    PoolClosedError,
    RedisConnection,
    create_connection,
)
from aioredis_cluster.errors import ConnectTimeoutError


logger = logging.getLogger(__name__)


class ConnectionsPool(AbcPool):
    """Redis connections pool."""

    def __init__(
        self,
        address: Union[str, Tuple[str, int]],
        db=None,
        password=None,
        encoding=None,
        *,
        minsize,
        maxsize,
        ssl=None,
        parser=None,
        create_connection_timeout: float = None,
        connection_cls=None,
        loop: asyncio.AbstractEventLoop = None,
    ):
        assert isinstance(minsize, int) and minsize >= 0, (
            "minsize must be int >= 0",
            minsize,
            type(minsize),
        )
        assert maxsize is not None, "Arbitrary pool size is disallowed."
        assert isinstance(maxsize, int) and maxsize > 0, (
            "maxsize must be int > 0",
            maxsize,
            type(maxsize),
        )
        assert minsize <= maxsize, ("Invalid pool min/max sizes", minsize, maxsize)
        self._address = address
        self._db = db
        self._password = password
        self._ssl = ssl
        self._encoding = encoding
        self._parser_class = parser
        self._minsize = minsize
        self._create_connection_timeout = create_connection_timeout
        self._pool: Deque[RedisConnection] = collections.deque(maxlen=maxsize)
        self._released: Deque[RedisConnection] = collections.deque(maxlen=maxsize)
        self._used: Set[RedisConnection] = set()
        self._acquiring = 0
        self._conn_waiters_count = 0
        self._cond = asyncio.Condition(lock=asyncio.Lock())
        self._close_state = CloseEvent(self._do_close)
        self._pubsub_conn = None
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
            f"size:{self.size}>"
        )

    @property
    def minsize(self):
        """Minimum pool size."""
        return self._minsize

    @property
    def maxsize(self):
        """Maximum pool size."""
        return self._pool.maxlen

    @property
    def size(self):
        """Current pool size."""
        return self.freesize + len(self._used) + self._acquiring

    @property
    def freesize(self):
        """Current number of free connections."""
        return len(self._pool)

    @property
    def address(self):
        return self._address

    async def clear(self):
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
        async with self._cond:
            assert not self._acquiring, self._acquiring
            waiters = []
            while self._pool:
                conn = self._pool.popleft()
                conn.close()
                waiters.append(conn.wait_closed())
            for conn in self._used:
                conn.close()
                waiters.append(conn.wait_closed())
            if self._pubsub_conn:
                self._pubsub_conn.close()
                waiters.append(conn.wait_closed())
            await asyncio.gather(*waiters)
            # TODO: close _pubsub_conn connection
            logger.info("Closed %d connection(s)", len(waiters))

    def close(self):
        """Close all free and in-progress connections and mark pool as closed."""
        if not self._close_state.is_set():
            self._close_state.set()

    @property
    def closed(self):
        """True if pool is closed."""
        return self._close_state.is_set()

    async def wait_closed(self):
        """Wait until pool gets closed."""
        await self._close_state.wait()

    @property
    def db(self):
        """Currently selected db index."""
        return self._db or 0

    @property
    def encoding(self):
        """Current set codec or None."""
        return self._encoding

    def execute(self, command, *args, **kw):
        """Executes redis command in a free connection and returns
        future waiting for result.

        Picks connection from free pool and send command through
        that connection.
        If no connection is found, returns coroutine waiting for
        free connection to execute command.
        """
        conn, address = self.get_connection(command, args)
        if conn is not None:
            fut = conn.execute(command, *args, **kw)
            return self._check_result(fut, command, args, kw)
        else:
            coro = self._wait_execute(address, command, args, kw)
            return self._check_result(coro, command, args, kw)

    def execute_pubsub(self, command, *channels):
        """Executes Redis (p)subscribe/(p)unsubscribe commands.

        ConnectionsPool picks separate connection for pub/sub
        and uses it until explicitly closed or disconnected
        (unsubscribing from all channels/patterns will leave connection
         locked for pub/sub use).

        There is no auto-reconnect for this PUB/SUB connection.

        Returns asyncio.gather coroutine waiting for all channels/patterns
        to receive answers.
        """
        # before_pubsub_conn = self._pubsub_conn
        conn, address = self.get_connection(command)
        # logger.info(
        #     "PUBSUB (%s) conn %r, before_pubsub_conn=%r, self._pubsub_conn=%r",
        #     command,
        #     id(conn),
        #     id(before_pubsub_conn),
        #     id(self._pubsub_conn),
        # )
        if conn is not None:
            return conn.execute_pubsub(command, *channels)
        else:
            return self._wait_execute_pubsub(address, command, channels, {})

    def get_connection(self, command, args=()):
        """Get free connection from pool.

        Returns connection.
        """
        # TODO: find a better way to determine if connection is free
        #       and not havily used.
        command = command.upper().strip()
        is_pubsub = command in _PUBSUB_COMMANDS
        if is_pubsub and self._pubsub_conn:
            if not self._pubsub_conn.closed:
                return self._pubsub_conn, self._pubsub_conn.address
            # self._used.remove(self._pubsub_conn)
            self._pubsub_conn = None
        for i in range(self.freesize):
            conn = self._pool[0]
            self._pool.rotate(1)
            if conn.closed:  # or conn._waiters: (eg: busy connection)
                continue
            if conn.in_pubsub:
                continue
            if is_pubsub:
                logger.info("PUBSUB from pool")
                self._pubsub_conn = conn
                self._pool.remove(conn)
                # self._used.add(conn)
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

    async def _wait_execute_pubsub(self, address, command, args, kw):
        logger.info("Wait PUBSUB conn")
        if self.closed:
            raise PoolClosedError("Pool is closed")
        assert self._pubsub_conn is None or self._pubsub_conn.closed, (
            "Expected no or closed connection",
            self._pubsub_conn,
        )
        async with self._cond:
            if self.closed:
                raise PoolClosedError("Pool is closed")
            if self._pubsub_conn and self._pubsub_conn.closed:
                self._pubsub_conn = None
                # self._used.remove(self._pubsub_conn)
            conn = await self._create_new_connection(address)
            self._pubsub_conn = conn
            # self._used.add(conn)
            return await conn.execute_pubsub(command, *args, **kw)

    async def select(self, db):
        """Changes db index for all free connections.

        All previously acquired connections will be closed when released.
        """
        res = True
        async with self._cond:
            for i in range(self.freesize):
                res = res and (await self._pool[i].select(db))
            self._db = db
        return res

    async def auth(self, password):
        self._password = password
        async with self._cond:
            for i in range(self.freesize):
                await self._pool[i].auth(password)

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

    async def acquire(self, command=None, args=()):
        """Acquires a connection from free pool.

        Creates new connection if needed.
        """
        if self.closed:
            raise PoolClosedError("Pool is closed")

        async with self._cond:
            if self.closed:
                raise PoolClosedError("Pool is closed")

            count = 0
            acquire_after_wait = False
            while True:
                count += 1
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

                logger.info("wait %s, self._conn_waiters_count %s", count, self._conn_waiters_count)
                self._conn_waiters_count += 1
                try:
                    await self._cond.wait()
                finally:
                    self._conn_waiters_count -= 1
                acquire_after_wait = True

                # if (self._acquiring == 0 or acquire_after_wait) and self.freesize:
                #     conn = self._pool.popleft()
                #     assert not conn.closed, conn
                #     assert conn not in self._used, (conn, self._used)
                #     self._used.add(conn)
                #     return conn
                # else:

    def release(self, conn):
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
            elif conn.db == self.db:
                if not (self.maxsize and self.freesize < self.maxsize):
                    # consider this connection as old and close it.
                    conn.close()
            else:
                conn.close()
        # FIXME: check event loop is not closed
        asyncio.ensure_future(self._wakeup())

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
        # address = self._address
        while self.size < self.minsize:
            self._acquiring += 1
            try:
                conn = await self._create_new_connection(self._address)
                # check the healthy of that connection, if
                # something went wrong just trigger the Exception
                await conn.execute("ping")
                self._pool.append(conn)
            finally:
                self._acquiring -= 1
                # connection may be closed at yield point
                await self._drop_closed()
        if self.freesize:
            return
        if override_min:
            while not self._pool and self.size < self.maxsize:
                self._acquiring += 1
                try:
                    conn = await self._create_new_connection(self._address)
                    self._pool.append(conn)
                finally:
                    self._acquiring -= 1
                    # connection may be closed at yield point
                    await self._drop_closed()

    async def _create_new_connection(self, address) -> RedisConnection:
        try:
            return await create_connection(
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

    async def _wakeup(self):
        async with self._cond:
            if self._released:
                self._pool.extend(self._released)
                self._used.difference_update(self._released)
                self._released.clear()
            self._cond.notify()

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
