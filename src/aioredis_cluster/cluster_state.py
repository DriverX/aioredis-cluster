import dataclasses
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


CLUSTER_INFO_STATE_KEY = "cluster_state"
CLUSTER_INFO_CURRENT_EPOCH_KEY = "cluster_current_epoch"
CLUSTER_INFO_SLOTS_ASSIGNED = "cluster_slots_assigned"


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
        self.current_epoch: int
        self.slots_assigned: int
        self.nodes: Dict[Address, ClusterNode] = {}
        self.addrs: List[Address] = []
        self.masters: List[ClusterNode] = []
        # master address -> list of replicas
        self.replicas: Dict[Address, List[ClusterNode]] = {}
        self.slots: List[ClusterSlot] = []
        self.reload_id: int
        self.created_at = datetime.datetime.now()
        self.created_at_local = time.monotonic()


@dataclasses.dataclass
class ClusterInfo:
    state: NodeClusterState
    current_epoch: int
    slots_assigned: int


def parse_cluster_info(info: str) -> ClusterInfo:
    state: Optional[NodeClusterState] = None
    current_epoch: Optional[int] = None
    slots_assigned: Optional[int] = None
    for line in info.strip().splitlines():
        key, value = line.split(":", 1)
        if key == CLUSTER_INFO_STATE_KEY:
            state = NodeClusterState(value)
        elif key == CLUSTER_INFO_CURRENT_EPOCH_KEY:
            current_epoch = int(value)
        elif key == CLUSTER_INFO_SLOTS_ASSIGNED:
            slots_assigned = int(value)

    if state is None or current_epoch is None or slots_assigned is None:
        raise ValueError("Invalid cluster info")

    return ClusterInfo(
        state=state,
        current_epoch=current_epoch,
        slots_assigned=slots_assigned,
    )


@dataclasses.dataclass
class ClusterStateCandidate:
    node: Address
    cluster_info: ClusterInfo
    slots_response: List[List] = dataclasses.field(repr=False)


class ClusterState:
    def __init__(self, data: _ClusterStateData):
        self._data = data
        self._cached_repr: Optional[str] = None

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.repr_stats()}>"

    def repr_stats(self) -> str:
        if self._cached_repr is None:
            data = self._data
            num_of_replicas = sum(len(rs) for rs in data.replicas.values())
            repr_parts = [
                f"state:{data.state.value}",
                f"state_from:{data.state_from}",
                f"reload_id:{data.reload_id}",
                f"created:{data.created_at.isoformat()}",
                f"nodes:{len(data.nodes)}",
                f"masters:{len(data.masters)}",
                f"replicas:{num_of_replicas}",
                f"slots_assigned:{data.slots_assigned}",
                f"slot_ranges:{len(data.slots)}",
                f"current_epoch:{data.current_epoch}",
            ]
            self._cached_repr = ", ".join(repr_parts)
        return self._cached_repr

    @property
    def state(self) -> NodeClusterState:
        return self._data.state

    @property
    def state_from(self) -> Address:
        return self._data.state_from

    @property
    def current_epoch(self) -> int:
        return self._data.current_epoch

    @property
    def slots_assigned(self) -> int:
        return self._data.slots_assigned

    @property
    def reload_id(self) -> int:
        return self._data.reload_id

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
