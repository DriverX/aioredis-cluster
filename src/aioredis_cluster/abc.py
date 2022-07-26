from abc import abstractmethod
from typing import AnyStr, AsyncContextManager, List, Sequence, Union

from aioredis_cluster.aioredis import Redis
from aioredis_cluster.aioredis.abc import AbcChannel
from aioredis_cluster.aioredis.abc import AbcConnection as _AbcConnection
from aioredis_cluster.aioredis.abc import AbcPool as _AbcPool
from aioredis_cluster.cluster_state import ClusterState
from aioredis_cluster.structs import Address, ClusterNode


__all__ = [
    "AbcConnection",
    "AbcPool",
    "AbcChannel",
    "AbcCluster",
]


BytesOrStr = Union[bytes, str]


class AbcCluster(_AbcConnection):
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

    @abstractmethod
    def extract_keys(self, command_seq: Sequence[BytesOrStr]) -> List[bytes]:
        pass


class AbcConnection(_AbcConnection):
    @property
    @abstractmethod
    def readonly(self) -> bool:
        pass

    @abstractmethod
    async def set_readonly(self, value: bool) -> None:
        pass

    @abstractmethod
    async def auth_with_username(self, username: str, password: str) -> None:
        pass


class AbcPool(_AbcPool):
    @abstractmethod
    def get(self) -> AsyncContextManager[Redis]:
        pass

    @property
    @abstractmethod
    def size(self) -> int:
        pass

    @property
    @abstractmethod
    def minsize(self) -> int:
        pass

    @property
    @abstractmethod
    def maxsize(self) -> int:
        pass

    @property
    @abstractmethod
    def freesize(self) -> int:
        pass

    @property
    @abstractmethod
    def readonly(self) -> bool:
        pass

    @abstractmethod
    async def set_readonly(self, value: bool) -> None:
        pass

    @abstractmethod
    async def auth_with_username(self, username: str, password: str) -> None:
        pass
