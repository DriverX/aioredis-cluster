import datetime
import enum
import random
import time
from typing import Any, Dict, List, Optional

from aioredis_cluster.errors import ClusterStateError, UncoveredSlotError
from aioredis_cluster.structs import Address, ClusterNode, ClusterSlot


__all__ = (
    "NodeClusterState",
    "ClusterState",
)


@enum.unique
class NodeClusterState(enum.Enum):
    UNKNOWN = "unknown"
    OK = "ok"
    FAIL = "fail"

    @classmethod
    def _missing_(cls, value: Any) -> "NodeClusterState":
        return cls.UNKNOWN


class _ClusterStateData:
    """
    ClusterStateData only for internals
    """

    def __init__(self) -> None:
        self.state: NodeClusterState
        self.state_from: Address
        self.nodes: Dict[Address, ClusterNode] = {}
        self.addrs: List[Address] = []
        self.masters: List[ClusterNode] = []
        # master address -> list of replicas
        self.replicas: Dict[Address, List[ClusterNode]] = {}
        self.slots: List[ClusterSlot] = []
        self.created_at = datetime.datetime.now()
        self.created_at_local = time.monotonic()


class ClusterState:
    def __init__(self, data: _ClusterStateData):
        self._data = data

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.repr_stats()}>"

    def repr_stats(self) -> str:
        data = self._data
        num_of_replicas = sum(len(rs) for rs in data.replicas.values())
        repr_parts = [
            f"state:{data.state.value}",
            f"state_from:{data.state_from}",
            f"created:{data.created_at.isoformat()}",
            f"masters:{len(data.masters)}",
            f"replicas:{num_of_replicas}",
            f"slot_ranges:{len(data.slots)}",
        ]
        return ", ".join(repr_parts)

    @property
    def state(self) -> NodeClusterState:
        return self._data.state

    @property
    def state_from(self) -> Address:
        return self._data.state_from

    def find_slot(self, slot: int) -> ClusterSlot:
        slots = self._data.slots
        # binary search
        lo = 0
        hi = len(slots)
        while lo < hi:
            mid = (lo + hi) // 2
            cl_slot = slots[mid]

            if cl_slot.end < slot:
                lo = mid + 1
            else:
                hi = mid

        if lo >= len(slots):
            raise UncoveredSlotError(slot)

        cl_slot = slots[lo]
        if not cl_slot.in_range(slot):
            raise UncoveredSlotError(slot)

        return cl_slot

    def slot_master(self, slot: int) -> ClusterNode:
        return self.find_slot(slot).master

    def slot_nodes(self, slot: int) -> List[ClusterNode]:
        cl_slot = self.find_slot(slot)
        nodes = [cl_slot.master]
        replicas = self._data.replicas[cl_slot.master.addr]
        nodes.extend(replicas)
        return nodes

    def random_slot_node(self, slot: int) -> ClusterNode:
        return random.choice(self.slot_nodes(slot))

    def random_slot_replica(self, slot: int) -> Optional[ClusterNode]:
        cl_slot = self.find_slot(slot)
        replicas = self._data.replicas[cl_slot.master.addr]
        if not replicas:
            return None
        return random.choice(replicas)

    def random_master(self) -> ClusterNode:
        if not self._data.masters:
            raise ClusterStateError("no initialized masters")

        return random.choice(self._data.masters)

    def random_node(self) -> ClusterNode:
        if not self._data.addrs:
            raise ClusterStateError("no initialized nodes")

        addr = random.choice(self._data.addrs)
        return self._data.nodes[addr]

    def has_addr(self, addr: Address) -> bool:
        return addr in self._data.nodes

    def master_replicas(self, addr: Address) -> List[ClusterNode]:
        try:
            return list(self._data.replicas[addr])
        except KeyError:
            raise KeyError(f"No master with address {addr}")

    def masters(self) -> List[ClusterNode]:
        return list(self._data.masters)
