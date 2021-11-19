from abc import abstractmethod
from typing import AnyStr, List

from aioredis import Redis
from aioredis.abc import AbcChannel, AbcConnection, AbcPool

from aioredis_cluster.cluster_state import ClusterState
from aioredis_cluster.structs import Address, ClusterNode


__all__ = [
    "AbcConnection",
    "AbcPool",
    "AbcChannel",
    "AbcCluster",
]


class AbcCluster(AbcConnection):
    @abstractmethod
    def determine_slot(self, first_key: bytes, *keys: bytes) -> int:
        pass

    @abstractmethod
    async def all_masters(self) -> List[Redis]:
        pass

    @abstractmethod
    async def keys_master(self, key: AnyStr, *keys: AnyStr) -> Redis:
        pass

    @abstractmethod
    async def get_master_node_by_keys(self, key: AnyStr, *keys: AnyStr) -> ClusterNode:
        pass

    @abstractmethod
    async def create_pool_by_addr(
        self,
        addr: Address,
        *,
        minsize: int = None,
        maxsize: int = None,
    ) -> Redis:
        pass

    @abstractmethod
    async def get_cluster_state(self) -> ClusterState:
        pass
