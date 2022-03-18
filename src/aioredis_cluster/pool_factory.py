from typing import (
    Any,
    Dict,
    Optional,
)

from aioredis import create_pool
from aioredis_cluster.abc import AbcPool
from aioredis_cluster.typedef import (
    AioredisAddress,
)


class PoolFactory:
    def __init__(self, default_pool_opts: Optional[Dict[str, Any]] = None):
        if default_pool_opts is None:
            default_pool_opts = {}
        self._default_pool_opts: Dict[str, Any] = default_pool_opts

    async def create_pool(self, addr: AioredisAddress, opts: Dict[str, Any] = None) -> AbcPool:
        if opts is None:
            opts = {}
        pool = await create_pool(addr, **{**self._default_pool_opts, **opts})

        return pool
