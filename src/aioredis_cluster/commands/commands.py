import random
from functools import wraps
from typing import AnyStr, Callable, List, Optional, Set, Type, TypeVar, cast

from aioredis_cluster.abc import AbcCluster
from aioredis_cluster.aioredis.abc import AbcConnection
from aioredis_cluster.aioredis.commands import Redis
from aioredis_cluster.aioredis.util import _NOTSET

from .cluster import ClusterCommandsMixin
from .custom import StreamCustomCommandsMixin


__all__ = (
    "Redis",
    "RedisCluster",
    "conn_is_cluster",
)

_T = TypeVar("_T")


def conn_is_cluster(conn: AbcConnection) -> bool:
    return isinstance(conn, AbcCluster)


def blocked_for_cluster(method: Callable[..., _T]) -> Callable[..., _T]:
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        if conn_is_cluster(self.connection):
            raise NotImplementedError("Command is blocked in cluster mode")

        return cast(_T, method(self, *args, **kwargs))

    return wrapper


class RedisCluster(ClusterCommandsMixin, StreamCustomCommandsMixin, Redis):
    # proxy methods to connection (cluster instance)
    async def all_masters(self) -> List[Redis]:
        self._only_for_cluster()

        return await self.connection.all_masters()

    async def slot_master(self, slot: int) -> Redis:
        self._only_for_cluster()

        return await self.connection.slot_master(slot)

    async def keys_master(self, key: AnyStr, *keys: AnyStr) -> Redis:
        self._only_for_cluster()

        return await self.connection.keys_master(key, *keys)

    # special behavior for commands
    async def randomkey(self, *, encoding=_NOTSET) -> Optional[bytes]:
        if not conn_is_cluster(self.connection):
            return await super().randomkey()

        pools = await self.connection.all_masters()
        for _ in range(min([10, len(pools)])):
            result = await random.choice(pools).randomkey()
            if result is not None:
                return result

        return None

    @property
    def cluster_connection(self) -> AbcCluster:
        self._only_for_cluster()

        return cast(AbcCluster, self.connection)

    def _only_for_cluster(self):
        if not conn_is_cluster(self.connection):
            raise RuntimeError("Connection is not a Cluster instance")

    def __await__(self):
        if conn_is_cluster(self.connection):
            raise RuntimeError("Can't use for cluster")
        return super().__await__()


_blocked_methods = [
    "cluster_add_slots",
    "cluster_count_failure_reports",
    "cluster_count_key_in_slots",
    "cluster_del_slots",
    "cluster_failover",
    "cluster_forget",
    "cluster_get_keys_in_slots",
    "cluster_meet",
    "cluster_replicate",
    "cluster_reset",
    "cluster_save_config",
    "cluster_set_config_epoch",
    "cluster_setslot",
    "cluster_readonly",
    "cluster_readwrite",
    "client_setname",
    "shutdown",
    "slaveof",
    "script_kill",
    "move",
    # 'bitop_and',
    # 'bitop_or',
    # 'bitop_xor',
    # 'bitop_not',
    "select",
    "flushall",
    "flushdb",
    "script_load",
    "script_flush",
    "script_exists",
    "scan",
    "iscan",
    "quit",
    "swapdb",
    "migrate",
    "migrate_keys",
    # 'rename',
    # 'renamenx',
    "wait",
    "bgrewriteaof",
    "bgsave",
    "config_rewrite",
    "config_set",
    "config_resetstat",
    "save",
    "sync",
    # not implemented yet
    # you must get node pool by explicit way, ex:
    #   pool = await cluster.keys_master('key')
    #   pipe = pool.pipeline() #  or pool.multi_exec()
    "pipeline",
    "multi_exec",
]


def wrap_blocked_methods(cls: Type, methods: List[str]) -> None:
    ready: Set[str] = set()
    for name in methods:
        if name in ready:
            raise RuntimeError(f"{name!r} already wrapped")

        method = getattr(cls, name)
        setattr(cls, name, blocked_for_cluster(method))

        ready.add(name)


wrap_blocked_methods(RedisCluster, _blocked_methods)
