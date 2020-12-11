import asyncio
import random
import time
from contextlib import suppress
from operator import attrgetter
from typing import Dict, List, Optional, Sequence, Set, Tuple

from aioredis_cluster.command_info import (
    CommandsRegistry,
    create_registry,
    default_registry,
)
from aioredis_cluster.errors import (
    ClusterStateError,
    RedisError,
    UncoveredSlotError,
    network_errors,
)
from aioredis_cluster.log import logger
from aioredis_cluster.pooler import Pooler
from aioredis_cluster.structs import Address, ClusterNode, ClusterSlot
from aioredis_cluster.typedef import SlotsResponse


__all__ = [
    "ClusterState",
    "ClusterManager",
    "slots_ranges",
    "create_cluster_state",
]


def slots_ranges(slots: Sequence[ClusterSlot]) -> List[Tuple[int, int]]:
    return [(s.begin, s.end) for s in slots]


class ClusterState:
    __slots__ = (
        "nodes",
        "addrs",
        "masters",
        "replicas",
        "slots",
        "created_at",
    )

    def __init__(self) -> None:
        self.nodes: Dict[Address, ClusterNode] = {}
        self.addrs: List[Address] = []
        self.masters: List[ClusterNode] = []
        self.replicas: List[ClusterNode] = []
        self.slots: List[ClusterSlot] = []
        self.created_at = time.time()

    def __repr__(self) -> str:
        return "<{} {}>".format(type(self).__name__, self.repr_stats())

    def repr_stats(self) -> str:
        repr_parts = [
            "created:{}".format(self.created_at),
            "masters:{}".format(len(self.masters)),
            "replicas:{}".format(len(self.replicas)),
            "slot ranges:{}".format(len(self.slots)),
        ]
        return ", ".join(repr_parts)

    def find_slot(self, slot: int) -> ClusterSlot:
        # binary search
        lo = 0
        hi = len(self.slots)
        while lo < hi:
            mid = (lo + hi) // 2
            cl_slot = self.slots[mid]

            if cl_slot.end < slot:
                lo = mid + 1
            else:
                hi = mid

        if lo >= len(self.slots):
            raise UncoveredSlotError(slot)

        cl_slot = self.slots[lo]
        if not cl_slot.in_range(slot):
            raise UncoveredSlotError(slot)

        return cl_slot

    def slot_master(self, slot: int) -> ClusterNode:
        return self.find_slot(slot).master

    def slot_nodes(self, slot: int) -> List[ClusterNode]:
        cl_slot = self.find_slot(slot)
        nodes = [cl_slot.master]
        nodes.extend(cl_slot.replicas)
        return nodes

    def random_slot_node(self, slot: int) -> ClusterNode:
        return random.choice(self.slot_nodes(slot))

    def random_slot_replica(self, slot: int) -> Optional[ClusterNode]:
        replicas = self.find_slot(slot).replicas
        if not replicas:
            return None
        return random.choice(replicas)

    def random_master(self) -> ClusterNode:
        if not self.masters:
            raise ClusterStateError("no initialized masters")

        return random.choice(self.masters)

    def random_node(self) -> ClusterNode:
        if not self.addrs:
            raise ClusterStateError("no initialized nodes")

        addr = random.choice(self.addrs)
        return self.nodes[addr]


def create_cluster_state(slots_resp: SlotsResponse) -> ClusterState:
    state = ClusterState()

    nodes: Dict[Address, ClusterNode] = {}
    masters_set: Set[ClusterNode] = set()
    replicas_set: Set[ClusterNode] = set()

    for slot_spec in slots_resp:
        slot_begin = int(slot_spec[0])
        slot_end = int(slot_spec[1])
        slot_nodes: List[ClusterNode] = []
        for node_num, node_spec in enumerate(slot_spec[2:], 1):
            node_addr = Address(node_spec[0], int(node_spec[1]))
            if node_addr not in nodes:
                node = ClusterNode(node_addr, node_spec[2])
                nodes[node_addr] = node
            else:
                node = nodes[node_addr]

            if node_num == 1 and node not in masters_set:
                masters_set.add(node)
            elif node_num > 1 and node not in replicas_set:
                replicas_set.add(node)
            slot_nodes.append(node)

        state.slots.append(
            ClusterSlot(
                begin=slot_begin, end=slot_end, master=slot_nodes[0], replicas=slot_nodes[1:]
            )
        )

    state.slots.sort(key=attrgetter("begin", "end"))
    state.nodes = nodes
    state.masters = list(masters_set)
    state.replicas = list(replicas_set)
    state.addrs = list(nodes.keys())

    return state


class ClusterManager:
    STATE_RELOAD_INTERVAL = 300.0

    def __init__(
        self,
        startup_nodes: Sequence[Address],
        pooler: Pooler,
        *,
        state_reload_interval: float = None,
        follow_cluster: bool = None,
    ) -> None:
        self._startup_nodes = list(startup_nodes)
        self._pooler = pooler
        self._state: Optional[ClusterState] = None
        self._commands = default_registry
        if state_reload_interval is None:
            state_reload_interval = self.STATE_RELOAD_INTERVAL
        self._reload_interval = state_reload_interval
        if follow_cluster is None:
            follow_cluster = False
        self._follow_cluster = follow_cluster

        self._reload_count = 0
        self._reload_event = asyncio.Event()
        loop = asyncio.get_event_loop()
        self._reloader_task = loop.create_task(self._state_reloader())

    @property
    def commands(self) -> CommandsRegistry:
        return self._commands

    def require_reload_state(self) -> None:
        self._reload_event.set()

    async def reload_state(self) -> ClusterState:
        self._reload_count += 1
        return await self._load_state(self._reload_count)

    async def get_state(self) -> ClusterState:
        if self._state:
            state = self._state
        else:
            state = await self.reload_state()
        return state

    async def close(self) -> None:
        if self._reloader_task:
            self._reloader_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._reloader_task

    async def _load_slots(self, addrs: Sequence[Address]) -> SlotsResponse:
        first_err: Optional[Exception] = None

        logger.debug("Trying to obtain cluster slots from addrs: %r", addrs)

        # get first successful cluster slots response
        for addr in addrs:
            try:
                pool = await self._pooler.ensure_pool(addr)
                slots_resp = await pool.execute(b"CLUSTER", b"SLOTS", encoding="utf-8")
            except Exception as e:
                if first_err is None:
                    first_err = e
                logger.warning("Unable to get cluster slots from %s: %r", addr, e)
                continue

            logger.debug("Cluster slots successful loaded from %s: %r", addr, slots_resp)

            break
        else:
            if first_err is not None:
                logger.error("No available hosts to load cluster slots")
                raise first_err

        return slots_resp

    async def _state_reloader(self) -> None:
        while True:
            auto_reload = False

            try:
                await asyncio.wait_for(self._reload_event.wait(), self._reload_interval)
            except asyncio.TimeoutError:
                auto_reload = True

            self._reload_count += 1
            reload_id = self._reload_count

            if auto_reload:
                logger.info("Start cluster state auto reload (%d)", reload_id)
            else:
                logger.info("Start loading cluster state (%d)", reload_id)

            try:
                await self._load_state(reload_id)
            except asyncio.CancelledError:
                raise
            except network_errors + (RedisError,) as e:
                logger.warning("Unable to load cluster state: %r (%d)", e, reload_id)
            except Exception as e:
                logger.exception(
                    "Unexpected error while loading cluster state: %r (%d)", e, reload_id
                )
            else:
                logger.info("Cluster state successful loaded (%d)", reload_id)

            await asyncio.sleep(0.1)
            self._reload_event.clear()

    def _get_init_addrs(self, reload_id: int) -> List[Address]:
        if self._follow_cluster and reload_id > 1 and self._state and self._state.addrs:
            addrs = list(self._state.addrs)
            random.shuffle(addrs)
        else:
            addrs = self._startup_nodes
        return addrs

    async def _load_state(self, reload_id: int) -> ClusterState:
        commands: Optional[CommandsRegistry] = None

        init_addrs = self._get_init_addrs(reload_id)
        slots_resp = await self._load_slots(init_addrs)
        state = create_cluster_state(slots_resp)

        # initialize connections pool for every master node in new state
        for node in state.masters:
            await self._pooler.ensure_pool(node.addr)

        # choose random master node and load command specs from node
        pool = await self._pooler.ensure_pool(state.random_master().addr)
        # fetch commands only for first cluster state load
        if reload_id == 1:
            raw_commands = await pool.execute(b"COMMAND", encoding="utf-8")
            commands = create_registry(raw_commands)
            logger.debug("Found %d supported commands in cluster", commands.size())

        # assign initial cluster state and commands
        self._state = state
        if commands is not None:
            self._commands = commands
        logger.info(
            "Loaded state: "
            "%d slots ranges, "
            "%d cluster nodes, "
            "%d masters, "
            "%d replicas "
            "(reload_id=%d)",
            len(state.slots),
            len(state.nodes),
            len(state.masters),
            len(state.replicas),
            reload_id,
        )

        return self._state

    async def _init(self) -> None:
        logger.info(
            "Initialize cluster with %d startup nodes: %r",
            len(self._startup_nodes),
            self._startup_nodes,
        )

        await self.reload_state()

        logger.info("Cluster successful initialized")
