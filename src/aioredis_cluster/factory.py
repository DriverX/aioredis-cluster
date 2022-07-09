import random
import ssl
import warnings
from typing import Any, List, Optional, Sequence, Union

from aioredis_cluster import aioredis
from aioredis_cluster._aioredis.util import parse_url
from aioredis_cluster.abc import AbcCluster
from aioredis_cluster.cluster import Cluster
from aioredis_cluster.commands import RedisCluster, conn_is_cluster
from aioredis_cluster.structs import Address
from aioredis_cluster.typedef import AioredisAddress, CommandsFactory


__all__ = (
    "create_cluster",
    "create_redis_cluster",
)


async def create_cluster(
    startup_nodes: Sequence[AioredisAddress],
    *,
    # failover options
    retry_min_delay: float = None,
    retry_max_delay: float = None,
    max_attempts: int = None,
    attempt_timeout: float = None,
    # manager options
    state_reload_frequency: float = None,
    state_reload_interval: float = None,
    follow_cluster: bool = None,
    # pool options
    idle_connection_timeout: float = None,
    # node client options
    password: str = None,
    encoding: str = None,
    pool_minsize: int = None,
    pool_maxsize: int = None,
    connect_timeout: float = None,
    ssl: Optional[Union[bool, ssl.SSLContext]] = None,
) -> AbcCluster:
    corrected_nodes: List[Address] = []
    for mixed_addr in startup_nodes:
        if isinstance(mixed_addr, str):
            parsed_addr, options = parse_url(mixed_addr)
            addr = Address(parsed_addr[0], parsed_addr[1])
        else:
            addr = Address(mixed_addr[0], mixed_addr[1])
        corrected_nodes.append(addr)

    # shuffle startup nodes for every instance
    # for distribute initial load to cluster
    if len(corrected_nodes) > 1:
        random.shuffle(corrected_nodes)

    if state_reload_frequency is not None:
        warnings.warn(
            "`state_reload_frequency` is deprecated and is no affect anything",
            DeprecationWarning,
        )

    cluster = Cluster(
        corrected_nodes,
        retry_min_delay=retry_min_delay,
        retry_max_delay=retry_max_delay,
        max_attempts=max_attempts,
        state_reload_interval=state_reload_interval,
        follow_cluster=follow_cluster,
        idle_connection_timeout=idle_connection_timeout,
        password=password,
        encoding=encoding,
        pool_minsize=pool_minsize,
        pool_maxsize=pool_maxsize,
        connect_timeout=connect_timeout,
        attempt_timeout=attempt_timeout,
        ssl=ssl,
    )
    try:
        await cluster._init()
    except Exception:
        cluster.close()
        await cluster.wait_closed()
        raise

    return cluster


async def create_redis_cluster(
    startup_nodes: Sequence[AioredisAddress],
    *,
    cluster_commands_factory: CommandsFactory = None,
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
    connect_timeout: float = None,
    ssl: Optional[Union[bool, ssl.SSLContext]] = None,
) -> RedisCluster:
    if cluster_commands_factory is None:
        cluster_commands_factory = RedisCluster
    cluster = await create_cluster(
        startup_nodes,
        retry_min_delay=retry_min_delay,
        retry_max_delay=retry_max_delay,
        max_attempts=max_attempts,
        state_reload_interval=state_reload_interval,
        follow_cluster=follow_cluster,
        idle_connection_timeout=idle_connection_timeout,
        password=password,
        encoding=encoding,
        pool_minsize=pool_minsize,
        pool_maxsize=pool_maxsize,
        connect_timeout=connect_timeout,
        attempt_timeout=attempt_timeout,
        ssl=ssl,
    )
    return cluster_commands_factory(cluster)


def is_redis_cluster(obj: Any) -> bool:
    if isinstance(obj, aioredis.Redis):
        return conn_is_cluster(obj.connection)
    return False
