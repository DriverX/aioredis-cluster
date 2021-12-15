import asyncio
import logging
import random
from contextlib import suppress
from operator import attrgetter
from typing import Dict, List, Optional, Sequence, Set, Tuple

import async_timeout

from aioredis_cluster.cluster_state import (
    ClusterState,
    NodeClusterState,
    _ClusterStateData,
)
from aioredis_cluster.command_info import (
    CommandsRegistry,
    create_registry,
    default_registry,
)
from aioredis_cluster.errors import RedisError, network_errors
from aioredis_cluster.log import logger
from aioredis_cluster.pooler import Pooler
from aioredis_cluster.structs import Address, ClusterNode, ClusterSlot
from aioredis_cluster.util import parse_info


__all__ = (
    "ClusterManager",
    "slots_ranges",
    "create_cluster_state",
)


SlotsResponse = List[List]
CLUSTER_INFO_STATE_KEY = "cluster_state"


def slots_ranges(slots: Sequence[ClusterSlot]) -> List[Tuple[int, int]]:
    return [(s.begin, s.end) for s in slots]


def create_cluster_state(
    slots_resp: SlotsResponse,
    cluster_info: Dict[str, str],
    state_from: Address,
) -> ClusterState:
    state_data = _ClusterStateData()
    state_data.state = NodeClusterState(cluster_info[CLUSTER_INFO_STATE_KEY])
    state_data.state_from = state_from

    nodes: Dict[Address, ClusterNode] = {}
    master_replicas: Dict[Address, List[ClusterNode]] = {}
    masters_set: Set[ClusterNode] = set()
    replicas_set: Set[ClusterNode] = set()

    for slot_spec in slots_resp:
        master_node: Optional[ClusterNode] = None
        slot_begin = int(slot_spec[0])
        slot_end = int(slot_spec[1])
        node_num = 0
        for node_num, node_spec in enumerate(slot_spec[2:], 1):
            node_addr = Address(node_spec[0], int(node_spec[1]))
            if node_addr not in nodes:
                node = ClusterNode(node_addr, node_spec[2])
                nodes[node_addr] = node
            else:
                node = nodes[node_addr]

            # first element is always master
            if node_num == 1:
                masters_set.add(node)
                master_node = node
                if master_node.addr not in master_replicas:
                    master_replicas[master_node.addr] = []
            elif node_num > 1 and node not in replicas_set:
                assert master_node is not None
                replicas_set.add(node)
                master_replicas[master_node.addr].append(node)

        if node_num == 0:
            logger.error(
                "CLUSTER SLOTS returns slot range %r without master node",
                (slot_begin, slot_end),
            )
        else:
            assert master_node is not None
            state_data.slots.append(
                ClusterSlot(
                    begin=slot_begin,
                    end=slot_end,
                    master=master_node,
                )
            )

    state_data.slots.sort(key=attrgetter("begin", "end"))
    state_data.nodes = nodes
    state_data.masters = list(masters_set)
    state_data.replicas = master_replicas
    state_data.addrs = list(nodes.keys())

    return ClusterState(state_data)


class ClusterManager:
    STATE_RELOAD_INTERVAL = 300.0
    EXECUTE_TIMEOUT = 5.0

    def __init__(
        self,
        startup_nodes: Sequence[Address],
        pooler: Pooler,
        *,
        state_reload_interval: float = None,
        follow_cluster: bool = None,
        execute_timeout: float = None,
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

        if execute_timeout is None:
            execute_timeout = self.EXECUTE_TIMEOUT
        elif execute_timeout < 0:
            raise ValueError("execute_timeout cannot be negative")

        if execute_timeout > self._reload_interval:
            execute_timeout = self._reload_interval

        self._execute_timeout = execute_timeout

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

    async def _fetch_state(self, addrs: Sequence[Address]) -> ClusterState:
        if len(addrs) == 0:
            raise RuntimeError("no addrs to fetch cluster state")

        last_err: Optional[BaseException] = None

        if len(addrs) > 10:
            # choose first minimum ten addrs
            # addrs probable randomized
            addrs = addrs[: max(10, len(addrs) // 2)]

        logger.debug("Trying to obtain cluster state from addrs: %r", addrs)

        # get first successful cluster slots response
        for addr in addrs:
            logger.info("Obtain cluster state from %s", addr)
            try:
                pool = await self._pooler.ensure_pool(addr)
                async with async_timeout.timeout(self._execute_timeout):
                    # ensure one connection behaviour
                    async with pool.get() as conn:
                        raw_cluster_info: str = await conn.execute(
                            b"CLUSTER", b"INFO", encoding="utf-8"
                        )
                        cluster_info = parse_info(raw_cluster_info)
                        slots_resp = await conn.execute(b"CLUSTER", b"SLOTS", encoding="utf-8")

            except asyncio.TimeoutError as e:
                last_err = e
                logger.warning("Getting cluster state from %s is timed out", addr)
                continue
            except Exception as e:
                last_err = e
                logger.warning("Unable to get cluster state from %s: %r", addr, e)
                continue

            if cluster_info[CLUSTER_INFO_STATE_KEY] != NodeClusterState.OK.value:
                logger.warning(
                    'Node %s was return not "ok" cluster state "%s". Try next node',
                    addr,
                    cluster_info[CLUSTER_INFO_STATE_KEY],
                )
                continue

            logger.debug("Cluster state successful loaded from %s: %r", addr, slots_resp)

            break
        else:
            if last_err is not None:
                logger.error("No available hosts to load cluster slots. Tried hosts: %r", addrs)
                raise last_err

        state = create_cluster_state(slots_resp, cluster_info, addr)

        if state.state is not NodeClusterState.OK:
            logger.warning(
                (
                    "Cluster probably broken. Tried %d nodes and "
                    'apply not "ok" (%s) cluster state from %s'
                ),
                len(addrs),
                state.state.value,
                addr,
            )

        return state

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
        if self._follow_cluster and reload_id > 1 and self._state and self._state._data.addrs:
            addrs = list(self._state._data.addrs)
            random.shuffle(addrs)
        else:
            addrs = self._startup_nodes
        return addrs

    async def _load_state(self, reload_id: int) -> ClusterState:
        commands: Optional[CommandsRegistry] = None

        init_addrs = self._get_init_addrs(reload_id)
        state = await self._fetch_state(init_addrs)

        # initialize connections pool for every master node in new state
        for node in state._data.masters:
            await self._pooler.ensure_pool(node.addr)

        # choose random master node and load command specs from node
        pool = await self._pooler.ensure_pool(state.random_master().addr)
        # fetch commands only for first cluster state load
        if reload_id == 1:
            async with async_timeout.timeout(self._execute_timeout):
                raw_commands = await pool.execute(b"COMMAND", encoding="utf-8")
            commands = create_registry(raw_commands)
            logger.debug("Found %d supported commands in cluster", commands.size())

        # assign initial cluster state and commands
        self._state = state
        if commands is not None:
            self._commands = commands

        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "Loaded state: %s (reload_id=%d)",
                state.repr_stats(),
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
