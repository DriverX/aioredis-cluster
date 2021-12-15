import asyncio
import logging
from collections import ChainMap
from time import monotonic
from types import MappingProxyType
from typing import (
    Any,
    AnyStr,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
)

from aioredis import Redis, create_pool
from aioredis.errors import ProtocolError, ReplyError
from async_timeout import timeout as atimeout

from aioredis_cluster.abc import AbcChannel, AbcCluster, AbcPool
from aioredis_cluster.command_exec import (
    ExecuteContext,
    ExecuteFailProps,
    ExecuteProps,
)
from aioredis_cluster.command_info import CommandInfo, extract_keys
from aioredis_cluster.commands import RedisCluster
from aioredis_cluster.connection import ConnectionsPool
from aioredis_cluster.crc import key_slot
from aioredis_cluster.errors import (
    AskError,
    ClusterClosedError,
    ClusterDownError,
    ConnectTimeoutError,
    LoadingError,
    MovedError,
    RedisClusterError,
    TryAgainError,
    UncoveredSlotError,
    closed_errors,
    network_errors,
)
from aioredis_cluster.log import logger
from aioredis_cluster.manager import ClusterManager, ClusterState
from aioredis_cluster.pooler import Pooler
from aioredis_cluster.structs import Address, ClusterNode
from aioredis_cluster.typedef import (
    AioredisAddress,
    BytesOrStr,
    CommandsFactory,
    PubsubResponse,
)
from aioredis_cluster.util import (
    ensure_bytes,
    iter_ensure_bytes,
    retry_backoff,
)


__all__ = (
    "AbcCluster",
    "Cluster",
)


class Cluster(AbcCluster):
    _connection_errors = network_errors + closed_errors + (asyncio.TimeoutError,)

    POOL_MINSIZE = 1
    POOL_MAXSIZE = 10

    MAX_ATTEMPTS = 10
    RETRY_MIN_DELAY = 0.05  # 50ms
    RETRY_MAX_DELAY = 1.0
    ATTEMPT_TIMEOUT = 5.0

    CONNECT_TIMEOUT = 1.0

    def __init__(
        self,
        startup_nodes: Sequence[Address],
        *,
        # failover options
        retry_min_delay: float = None,
        retry_max_delay: float = None,
        max_attempts: int = None,
        attempt_timeout: float = None,
        # manager options
        state_reload_interval: float = None,
        follow_cluster: bool = None,
        # pool options
        idle_connection_timeout: float = None,
        # node client options
        password: str = None,
        encoding: str = None,
        pool_minsize: int = None,
        pool_maxsize: int = None,
        commands_factory: CommandsFactory = None,
        connect_timeout: float = None,
        pool_cls: Type[AbcPool] = None,
    ) -> None:
        if len(startup_nodes) < 1:
            raise ValueError("startup_nodes must be one at least")

        if retry_min_delay is None:
            retry_min_delay = self.RETRY_MIN_DELAY
        elif retry_min_delay < 0:
            raise ValueError("min_delay value is negative")
        self._retry_min_delay = retry_min_delay

        if retry_max_delay is None:
            retry_max_delay = self.RETRY_MAX_DELAY
        elif retry_max_delay < 0:
            raise ValueError("max_delay value is negative")
        elif retry_max_delay < retry_min_delay:
            logger.warning(
                "retry_max_delay < retry_min_delay: %s < %s", retry_max_delay, retry_min_delay
            )
            retry_max_delay = retry_min_delay
        self._retry_max_delay = retry_max_delay

        if max_attempts is None:
            max_attempts = self.MAX_ATTEMPTS
        elif max_attempts == 0:
            # probably infinity (CAUTION!!!)
            max_attempts = (1 << 64) - 1
        elif max_attempts < 0:
            raise ValueError("max_attempts must be >= 0")
        self._max_attempts = max_attempts

        if attempt_timeout is None:
            attempt_timeout = self.ATTEMPT_TIMEOUT
        elif attempt_timeout <= 0:
            raise ValueError("attempt_timeout must be > 0")
        self._attempt_timeout = float(attempt_timeout)

        self._password = password
        self._encoding = encoding

        if pool_minsize is None:
            pool_minsize = self.POOL_MINSIZE
        if pool_maxsize is None:
            pool_maxsize = self.POOL_MAXSIZE

        if pool_minsize < 1 or pool_maxsize < 1:
            raise ValueError("pool_minsize and pool_maxsize must be greater than 1")

        if pool_maxsize < pool_minsize:
            logger.warning(
                "pool_maxsize less than pool_minsize: %s < %s", pool_maxsize, pool_minsize
            )
            pool_maxsize = pool_minsize

        self._pool_minsize = pool_minsize
        self._pool_maxsize = pool_maxsize

        if commands_factory is None:
            commands_factory = RedisCluster

        self._commands_factory = commands_factory

        if connect_timeout is None:
            connect_timeout = self.CONNECT_TIMEOUT
        self._connect_timeout = connect_timeout

        if pool_cls is None:
            pool_cls = ConnectionsPool
        self._pool_cls = pool_cls

        self._pooler = Pooler(self._create_default_pool, reap_frequency=idle_connection_timeout)

        self._manager = ClusterManager(
            startup_nodes,
            self._pooler,
            state_reload_interval=state_reload_interval,
            follow_cluster=follow_cluster,
            execute_timeout=self._attempt_timeout,
        )

        self._loop = asyncio.get_event_loop()
        self._closing: Optional[asyncio.Task] = None
        self._closing_event = asyncio.Event()
        self._closed = False

    def __repr__(self) -> str:
        state_stats = ""
        if self._manager._state:
            state_stats = self._manager._state.repr_stats()
        return f"<{type(self).__name__} {state_stats}>"

    async def execute(self, *args, **kwargs) -> Any:
        """Execute redis command."""

        ctx = self._make_exec_context(args, kwargs)

        keys = self._extract_command_keys(ctx.cmd_info, ctx.cmd)
        if keys:
            ctx.slot = self.determine_slot(*keys)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Determined slot for %r is %d", ctx.cmd_for_repr(), ctx.slot)

        exec_fail_props: Optional[ExecuteFailProps] = None

        while ctx.attempt < ctx.max_attempts:
            self._check_closed()

            ctx.attempt += 1

            state = await self._manager.get_state()
            exec_props = self._make_execute_props(state, ctx, exec_fail_props)

            if exec_props.reload_state_required:
                self._manager.require_reload_state()

            node_addr = exec_props.node_addr

            # reset previous execute fail properties
            prev_exec_fail_props = exec_fail_props
            exec_fail_props = None

            try:
                result = await self._try_execute(ctx, exec_props, prev_exec_fail_props)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                exec_fail_props = ExecuteFailProps(
                    node_addr=node_addr,
                    error=e,
                )

            if exec_fail_props:
                await self._on_execute_fail(ctx, exec_fail_props)
                continue

            break

        return result

    async def execute_pubsub(self, *args, **kwargs) -> List[PubsubResponse]:
        """Execute Redis (p)subscribe/(p)unsubscribe commands."""

        ctx = self._make_exec_context(args, kwargs)

        # get first pattern and calculate slot
        channel_name = ctx.cmd[1]
        is_pattern = ctx.cmd_name in {"PSUBSCRIBE", "PUNSUBSCRIBE"}
        is_unsubscribe = ctx.cmd_name in {"UNSUBSCRIBE", "PUNSUBSCRIBE"}
        ctx.slot = key_slot(channel_name)

        exec_fail_props: Optional[ExecuteFailProps] = None

        result: List[List]
        while ctx.attempt < ctx.max_attempts:
            self._check_closed()

            ctx.attempt += 1
            exec_fail_props = None

            state = await self._manager.get_state()

            node_addr = self._pooler.get_pubsub_addr(channel_name, is_pattern)

            # if unsuscribe command and no node found for pattern
            # probably pubsub connection is already close
            if is_unsubscribe and node_addr is None:
                result = [[ctx.cmd[0], channel_name, 0]]
                break

            if node_addr is None:
                try:
                    node_addr = state.random_slot_node(ctx.slot).addr
                except UncoveredSlotError:
                    logger.warning("No any node found by slot %d", ctx.slot)
                    node_addr = state.random_node().addr

            try:
                pool = await self._pooler.ensure_pool(node_addr)

                async with atimeout(self._attempt_timeout):
                    result = await pool.execute_pubsub(*ctx.cmd, **ctx.kwargs)

                if is_unsubscribe:
                    self._pooler.remove_pubsub_channel(channel_name, is_pattern)
                else:
                    self._pooler.add_pubsub_channel(node_addr, channel_name, is_pattern)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                exec_fail_props = ExecuteFailProps(
                    node_addr=node_addr,
                    error=e,
                )

            if exec_fail_props:
                await self._on_execute_fail(ctx, exec_fail_props)
                continue

            break

        return [(cmd, name, count) for cmd, name, count in result]

    def close(self) -> None:
        """Perform connection(s) close and resources cleanup."""

        self._closed = True

        if self._closing is None:
            self._closing = self._loop.create_task(self._do_close())
            self._closing.add_done_callback(lambda f: self._closing_event.set())

    async def wait_closed(self) -> None:
        """
        Coroutine waiting until all resources are closed/released/cleaned up.
        """

        await self._closing_event.wait()

    @property
    def closed(self) -> bool:
        """Flag indicating if connection is closing or already closed."""
        return self._closed

    @property
    def db(self) -> int:
        """Current selected DB index. Always 0 for cluster"""

        return 0

    @property
    def encoding(self) -> Optional[str]:
        """Current set connection codec."""

        return self._encoding

    @property
    def in_pubsub(self) -> int:
        """Returns number of subscribed channels.

        Can be tested as bool indicating Pub/Sub mode state.
        """

        return sum(p.in_pubsub for p in self._pooler.pools())

    @property
    def pubsub_channels(self) -> Mapping[str, AbcChannel]:
        """Read-only channels dict."""

        return MappingProxyType(ChainMap(*(p.pubsub_channels for p in self._pooler.pools())))

    @property
    def pubsub_patterns(self) -> Mapping[str, AbcChannel]:
        """Read-only patterns dict."""

        return MappingProxyType(ChainMap(*(p.pubsub_patterns for p in self._pooler.pools())))

    @property
    def address(self) -> Tuple[str, int]:
        """Connection address."""

        addr = self._manager._startup_nodes[0]
        return addr.host, addr.port

    async def auth(self, password: str) -> None:
        self._check_closed()

        self._password = password

        async def authorize(pool) -> None:
            nonlocal password
            await pool.auth(password)

        await self._pooler.batch_op(authorize)

    def determine_slot(self, first_key: bytes, *keys: bytes) -> int:
        slot: int = key_slot(first_key)
        for k in keys:
            if slot != key_slot(k):
                raise RedisClusterError("all keys must map to the same key slot")

        return slot

    async def all_masters(self) -> List[Redis]:
        ctx = self._make_exec_context((b"PING",), {})

        exec_fail_props: Optional[ExecuteFailProps] = None
        pools: List[Redis] = []
        while ctx.attempt < ctx.max_attempts:
            self._check_closed()

            ctx.attempt += 1

            state = await self._manager.get_state()
            exec_fail_props = None
            execute_timeout = self._attempt_timeout
            pools = []
            try:
                for node in state._data.masters:
                    pool = await self._pooler.ensure_pool(node.addr)
                    start_exec_t = monotonic()

                    await self._pool_execute(pool, ctx.cmd, ctx.kwargs, timeout=execute_timeout)

                    execute_timeout = max(0, execute_timeout - (monotonic() - start_exec_t))
                    pools.append(self._commands_factory(pool))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                exec_fail_props = ExecuteFailProps(
                    node_addr=node.addr,
                    error=e,
                )

            if exec_fail_props:
                await self._on_execute_fail(ctx, exec_fail_props)
                continue

            break

        return pools

    async def keys_master(self, key: AnyStr, *keys: AnyStr) -> Redis:
        self._check_closed()

        slot = self.determine_slot(ensure_bytes(key), *iter_ensure_bytes(keys))

        ctx = self._make_exec_context((b"EXISTS", key), {})

        exec_fail_props: Optional[ExecuteFailProps] = None
        while ctx.attempt < ctx.max_attempts:
            self._check_closed()

            ctx.attempt += 1

            state = await self._manager.get_state()
            try:
                node = state.slot_master(slot)
            except UncoveredSlotError:
                logger.warning("No master node found by slot %d", slot)
                self._manager.require_reload_state()
                raise

            exec_fail_props = None
            try:
                pool = await self._pooler.ensure_pool(node.addr)
                await self._pool_execute(pool, ctx.cmd, ctx.kwargs, timeout=self._attempt_timeout)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                exec_fail_props = ExecuteFailProps(
                    node_addr=node.addr,
                    error=e,
                )

            if exec_fail_props:
                await self._on_execute_fail(ctx, exec_fail_props)
                continue

            break

        return self._commands_factory(pool)

    async def get_master_node_by_keys(self, key: AnyStr, *keys: AnyStr) -> ClusterNode:
        slot = self.determine_slot(ensure_bytes(key), *iter_ensure_bytes(keys))
        state = await self._manager.get_state()
        try:
            node = state.slot_master(slot)
        except UncoveredSlotError:
            logger.warning("No master node found by slot %d", slot)
            self._manager.require_reload_state()
            raise

        return node

    async def create_pool_by_addr(
        self,
        addr: Address,
        *,
        minsize: int = None,
        maxsize: int = None,
    ) -> Redis:
        state = await self._manager.get_state()
        if state.has_addr(addr) is False:
            raise ValueError(f"Unknown node address {addr}")

        opts: Dict[str, Any] = {}
        if minsize is not None:
            opts["minsize"] = minsize
        if maxsize is not None:
            opts["maxsize"] = maxsize

        pool = await self._create_pool((addr.host, addr.port), opts)

        return self._commands_factory(pool)

    async def get_cluster_state(self) -> ClusterState:
        return await self._manager.get_state()

    def extract_keys(self, command_seq: Sequence[BytesOrStr]) -> List[bytes]:
        if len(command_seq) < 1:
            raise ValueError("No command")
        command_seq_bytes = tuple(iter_ensure_bytes(command_seq))
        cmd_info = self._manager.commands.get_info(command_seq_bytes[0])
        keys = extract_keys(cmd_info, command_seq_bytes)
        return keys

    async def _init(self) -> None:
        await self._manager._init()

    async def _do_close(self) -> None:
        await self._manager.close()
        await self._pooler.close()

    def _check_closed(self) -> None:
        if self._closed:
            raise ClusterClosedError()

    def _make_exec_context(self, args: Sequence, kwargs) -> ExecuteContext:
        cmd_info = self._manager.commands.get_info(args[0])
        if cmd_info.is_unknown():
            logger.warning("No info found for command %r", cmd_info.name)
        ctx = ExecuteContext(
            cmd=list(iter_ensure_bytes(args)),
            cmd_info=cmd_info,
            kwargs=kwargs,
            max_attempts=self._max_attempts,
        )
        return ctx

    def _extract_command_keys(
        self,
        cmd_info: CommandInfo,
        command_seq: Sequence[bytes],
    ) -> List[bytes]:
        if cmd_info.is_unknown():
            keys = []
        else:
            keys = extract_keys(cmd_info, command_seq)
        return keys

    async def _execute_retry_slowdown(self, attempt: int, max_attempts: int) -> None:
        # first two tries run immediately
        if attempt <= 1:
            return

        delay = retry_backoff(attempt - 1, self._retry_min_delay, self._retry_max_delay)
        logger.info("[%d/%d] Retry was slowed down by %.02fms", attempt, max_attempts, delay * 1000)
        await asyncio.sleep(delay)

    def _make_execute_props(
        self,
        state: ClusterState,
        ctx: ExecuteContext,
        fail_props: ExecuteFailProps = None,
    ) -> ExecuteProps:
        exec_props = ExecuteProps()

        node_addr: Address

        if fail_props:
            # reraise exception for simplify classification
            # instead of many isinstance conditions
            try:
                raise fail_props.error
            except self._connection_errors:
                if ctx.attempt <= 2 and ctx.slot is not None:
                    replica = state.random_slot_replica(ctx.slot)
                    if replica is not None:
                        node_addr = replica.addr
                    else:
                        node_addr = state.random_node().addr
                else:
                    node_addr = state.random_node().addr
            except MovedError as e:
                node_addr = Address(e.info.host, e.info.port)
            except AskError as e:
                node_addr = Address(e.info.host, e.info.port)
                exec_props.asking = e.info.ask
            except (ClusterDownError, TryAgainError, LoadingError, ProtocolError):
                node_addr = state.random_node().addr
            except Exception as e:
                # usualy never be done here
                logger.exception("Uncaught exception on execute: %r", e)
                raise
            logger.info("New node to execute: %s", node_addr)
        else:
            if ctx.slot is not None:
                try:
                    node = state.slot_master(ctx.slot)
                except UncoveredSlotError:
                    logger.warning("No node found by slot %d", ctx.slot)

                    # probably cluster is corrupted and
                    # we need try to recover cluster state
                    exec_props.reload_state_required = True
                    node = state.random_master()
                node_addr = node.addr
            else:
                node_addr = state.random_master().addr
            logger.debug("Defined node to command: %s", node_addr)

        exec_props.node_addr = node_addr

        return exec_props

    async def _try_execute(
        self, ctx: ExecuteContext, props: ExecuteProps, fail_props: Optional[ExecuteFailProps]
    ) -> Any:
        node_addr = props.node_addr

        attempt_log_prefix = ""
        if ctx.attempt > 1:
            attempt_log_prefix = f"[{ctx.attempt}/{ctx.max_attempts}] "

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("%sExecute %r on %s", attempt_log_prefix, ctx.cmd_for_repr(), node_addr)

        pool = await self._pooler.ensure_pool(node_addr)

        pool_size = pool.size
        if pool_size >= pool.maxsize and pool.freesize == 0:
            logger.warning(
                "ConnectionPool to %s size limit reached (minsize:%s, maxsize:%s, current:%s])",
                node_addr,
                pool.minsize,
                pool.maxsize,
                pool_size,
            )

        if props.asking:
            logger.info("Send ASKING to %s for command %r", node_addr, ctx.cmd_name)

            result = await self._conn_execute(
                pool,
                ctx.cmd,
                ctx.kwargs,
                timeout=self._attempt_timeout,
                asking=True,
            )
        else:
            if ctx.cmd_info.is_blocking():
                result = await self._conn_execute(
                    pool,
                    ctx.cmd,
                    ctx.kwargs,
                    timeout=self._attempt_timeout,
                )
            else:
                result = await self._pool_execute(
                    pool,
                    ctx.cmd,
                    ctx.kwargs,
                    timeout=self._attempt_timeout,
                )

        return result

    async def _on_execute_fail(self, ctx: ExecuteContext, fail_props: ExecuteFailProps) -> None:
        # classify error for logging and
        # set mark to reload cluster state if needed
        try:
            raise fail_props.error
        except network_errors as e:
            logger.warning("Connection problem with %s: %r", fail_props.node_addr, e)
            self._manager.require_reload_state()
        except closed_errors as e:
            logger.warning("Connection is closed: %r", e)
            self._manager.require_reload_state()
        except ConnectTimeoutError as e:
            logger.warning("Connect to node is timed out: %s", e)
            self._manager.require_reload_state()
        except ClusterDownError as e:
            logger.warning("Cluster is down: %s", e)
            self._manager.require_reload_state()
        except TryAgainError as e:
            logger.warning("Try again error: %s", e)
            self._manager.require_reload_state()
        except MovedError as e:
            logger.info("MOVED reply: %s", e)
            self._manager.require_reload_state()
        except AskError as e:
            logger.info("ASK reply: %s", e)
        except LoadingError as e:
            logger.warning("Cluster node %s is loading: %s", fail_props.node_addr, e)
            self._manager.require_reload_state()
        except ProtocolError as e:
            logger.warning("Redis protocol error: %s", e)
            self._manager.require_reload_state()
        except ReplyError as e:
            # all other reply error we must propagate to caller
            logger.warning("Reply error: %s", e)
            raise
        except asyncio.TimeoutError:
            is_readonly = ctx.cmd_info.is_readonly()
            if is_readonly:
                logger.warning(
                    "Read-Only command %s to %s is timed out", ctx.cmd_name, fail_props.node_addr
                )
            else:
                logger.warning(
                    "Non-idempotent command %s to %s is timed out. " "Abort command",
                    ctx.cmd_name,
                    fail_props.node_addr,
                )

            # node probably down
            self._manager.require_reload_state()

            # abort non-idempotent commands
            if not is_readonly:
                raise

        except Exception as e:
            logger.exception("Unexpected error: %r", e)
            raise

        if ctx.attempt >= ctx.max_attempts:
            raise fail_props.error

        # slowdown retry calls
        await self._execute_retry_slowdown(ctx.attempt, ctx.max_attempts)

    async def _create_default_pool(self, addr: AioredisAddress) -> AbcPool:
        return await self._create_pool(addr)

    async def _create_pool(
        self,
        addr: AioredisAddress,
        opts: Dict[str, Any] = None,
    ) -> AbcPool:
        if opts is None:
            opts = {}

        default_opts = dict(
            pool_cls=self._pool_cls,
            password=self._password,
            encoding=self._encoding,
            minsize=self._pool_minsize,
            maxsize=self._pool_maxsize,
            create_connection_timeout=self._connect_timeout,
        )

        pool = await create_pool(addr, **{**default_opts, **opts})

        return pool

    async def _conn_execute(
        self,
        pool: AbcPool,
        args: Sequence,
        kwargs: Dict,
        *,
        timeout: float = None,
        asking: bool = False,
    ) -> Any:
        result: Any

        async with pool.get() as conn:
            try:
                async with atimeout(timeout):
                    if asking:
                        # emulate command pipeline
                        results = await asyncio.gather(
                            conn.execute(b"ASKING"),
                            conn.execute(*args, **kwargs),
                            return_exceptions=True,
                        )
                        # raise first error
                        for result in results:
                            if isinstance(result, BaseException):
                                raise result

                        result = results[1]
                    else:
                        result = await conn.execute(*args, **kwargs)

                    return result

            except asyncio.TimeoutError:
                logger.warning(
                    "Execute command %s on %s is timed out. Closing connection",
                    args[0],
                    pool.address,
                )
                conn.close()
                raise

    async def _pool_execute(
        self,
        pool: AbcPool,
        args: Sequence,
        kwargs: Dict,
        *,
        timeout: float = None,
    ) -> Any:
        async with atimeout(timeout):
            result = await pool.execute(*args, **kwargs)
        return result
