import asyncio
import logging
import random
from contextlib import suppress
from operator import attrgetter
from typing import Dict, List, Optional, Sequence, Set, Tuple

import async_timeout

from aioredis_cluster.abc import AbcConnection
from aioredis_cluster.cluster_state import (
    ClusterState,
    ClusterStateCandidate,
    NodeClusterState,
    _ClusterStateData,
    parse_cluster_info,
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


__all__ = (
    "ClusterManager",
    "slots_ranges",
    "create_cluster_state",
)


SlotsResponse = List[List]
CLUSTER_INFO_STATE_KEY = "cluster_state"
CLUSTER_INFO_CURRENT_EPOCH_KEY = "cluster_current_epoch"
CLUSTER_INFO_SLOTS_ASSIGNED = "cluster_slots_assigned"


def slots_ranges(slots: Sequence[ClusterSlot]) -> List[Tuple[int, int]]:
    return [(s.begin, s.end) for s in slots]


def create_cluster_state(
    state_candidate: ClusterStateCandidate,
    reload_id: int,
) -> ClusterState:
    state_data = _ClusterStateData()
    state_data.state = state_candidate.cluster_info.state
    state_data.state_from = state_candidate.node
    state_data.current_epoch = state_candidate.cluster_info.current_epoch
    state_data.slots_assigned = state_candidate.cluster_info.slots_assigned
    state_data.reload_id = reload_id

    nodes: Dict[Address, ClusterNode] = {}
    master_replicas: Dict[Address, List[ClusterNode]] = {}
    masters_set: Set[ClusterNode] = set()
    replicas_set: Set[ClusterNode] = set()

    for slot_spec in state_candidate.slots_response:
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


class ClusterManagerError(RedisError):
    pass


class ClusterUnavailableError(ClusterManagerError):
    pass


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
        reload_state_candidates: int = None,
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
        if reload_state_candidates is None:
            reload_state_candidates = 2
        elif reload_state_candidates < 1:
            raise ValueError("`reload_state_candidates` cannot be lower than 1")
        self._reload_state_candidates = reload_state_candidates

        self._reload_count = 0
        self._reload_event = asyncio.Event()
        self._loop = asyncio.get_event_loop()
        self._reloader_task = self._loop.create_task(self._state_reloader())

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

    async def _fetch_state(
        self,
        addrs: Sequence[Address],
        reload_id: int,
        current_state: Optional[ClusterState],
    ) -> ClusterState:
        if len(addrs) == 0:
            raise RuntimeError("no addrs to fetch cluster state")

        last_err: Optional[BaseException] = None
        conn: AbcConnection

        if len(addrs) > 10:
            # choose first minimum ten addrs
            # addrs probable randomized
            addrs = addrs[: max(10, len(addrs) // 2)]

        logger.debug("Trying to obtain cluster state from addrs: %r", addrs)

        state_candidates: List[ClusterStateCandidate] = []

        # get first successful cluster slots response
        for addr in addrs:
            logger.info("Obtain cluster state from %s", addr)
            tail_timeout = self._execute_timeout
            execution_start_t = self._loop.time()
            try:
                pool = await self._pooler.ensure_pool(addr)
                try:
                    async with async_timeout.timeout(tail_timeout):
                        conn = await pool.acquire()
                except asyncio.TimeoutError as e:
                    last_err = e
                    logger.warning("Acquire connection from pool %s is timed out", addr)
                    continue

                tail_timeout -= self._loop.time() - execution_start_t

                try:
                    async with async_timeout.timeout(tail_timeout):
                        # ensure one connection behaviour
                        raw_cluster_info: str = await conn.execute(
                            b"CLUSTER",
                            b"INFO",
                            encoding="utf-8",
                        )
                        cluster_info = parse_cluster_info(raw_cluster_info)
                        slots_resp = await conn.execute(
                            b"CLUSTER",
                            b"SLOTS",
                            encoding="utf-8",
                        )
                except asyncio.TimeoutError as e:
                    last_err = e
                    logger.warning("Getting cluster state from %s is timed out", addr)
                    conn.close()
                    await conn.wait_closed()
                    continue
                finally:
                    pool.release(conn)

            except Exception as e:
                last_err = e
                logger.warning("Unable to get cluster state from %s: %r", addr, e)
                continue

            if cluster_info.state is not NodeClusterState.OK:
                logger.warning(
                    'Node %s was return not "ok" cluster state "%s". Try next node',
                    addr,
                    cluster_info.state,
                )
                continue

            state_candidate = ClusterStateCandidate(
                node=addr,
                cluster_info=cluster_info,
                slots_response=slots_resp,
            )
            state_candidates.append(state_candidate)

            logger.debug(
                "Cluster state candidate #%d successful loaded from %s: info:%r slots:%r",
                len(state_candidates) + 1,
                addr,
                cluster_info,
                slots_resp,
            )

            if len(state_candidates) >= self._reload_state_candidates:
                break
        else:
            if not state_candidates and last_err is not None:
                logger.error("No available hosts to load cluster slots. Tried hosts: %r", addrs)
                raise last_err

        if not state_candidates:
            raise ClusterUnavailableError()
        else:
            state_candidate = self._choose_state_candidate(state_candidates)
            if (
                current_state is not None
                and current_state.current_epoch == state_candidate.cluster_info.current_epoch
                and current_state.slots_assigned == state_candidate.cluster_info.slots_assigned
            ):
                state = current_state
            else:
                state = create_cluster_state(state_candidate, reload_id)

        return state

    def _choose_state_candidate(
        self,
        state_candidates: Sequence[ClusterStateCandidate],
    ) -> ClusterStateCandidate:
        iter_state_candidates = iter(state_candidates)
        current = next(iter_state_candidates)
        for candidate in iter_state_candidates:
            info = candidate.cluster_info
            current_info = current.cluster_info
            if info.current_epoch > current_info.current_epoch:
                current = candidate
            elif (
                info.current_epoch == current_info.current_epoch
                and info.slots_assigned > current_info.slots_assigned
            ):
                current = candidate
        return current

    async def _state_reloader(self) -> None:
        while True:
            auto_reload = False

            reload_interval_jitter = (
                random.randint(0, int(self._reload_interval * 1000 * 0.1)) / 1000
            )
            reload_interval = self._reload_interval * 0.9 + reload_interval_jitter
            logger.debug("Cluster state auto reload after %.03f sec", reload_interval)

            try:
                async with async_timeout.timeout(reload_interval):
                    await self._reload_event.wait()
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
            except ClusterUnavailableError:
                logger.warning(
                    "Unable to load any state from cluster. "
                    "Probably cluster is completely unavailable"
                )
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
        state = await self._fetch_state(init_addrs, reload_id, self._state)

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
                "Loaded cluster state: %s (reload_id=%d)",
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
