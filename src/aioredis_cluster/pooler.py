import asyncio
import dataclasses
from typing import Dict, List, NamedTuple, Optional, Sequence, Set

from aioredis_cluster.abc import AbcPool
from aioredis_cluster.log import logger
from aioredis_cluster.pool import POOL_IDLE_CONNECTIOM_TIMEOUT
from aioredis_cluster.resource_lock import ResourceLock
from aioredis_cluster.structs import Address
from aioredis_cluster.typedef import PoolCreator, PoolerBatchCallback

__all__ = ("Pooler",)


@dataclasses.dataclass
class PoolHolder:
    pool: AbcPool
    generation: int


class PubSubChannel(NamedTuple):
    name: bytes
    is_pattern: bool
    is_sharded: bool


class Pooler:
    REAP_FREQUENCY = POOL_IDLE_CONNECTIOM_TIMEOUT

    def __init__(
        self,
        pool_creator: PoolCreator,
        *,
        reap_frequency: float = None,
    ) -> None:
        self._create_pool = pool_creator
        self._nodes: Dict[Address, PoolHolder] = {}
        self._creation_lock: ResourceLock[Address] = ResourceLock()
        self._pubsub_channels: Dict[PubSubChannel, Address] = {}
        self._pubsub_addrs: Dict[Address, Set[PubSubChannel]] = {}

        if reap_frequency is None:
            reap_frequency = self.REAP_FREQUENCY
        self._reap_frequency = reap_frequency
        self._reap_calls = 0

        self._reaper_task = None
        if self._reap_frequency >= 0:
            loop = asyncio.get_running_loop()
            self._reaper_task = loop.create_task(self._reaper())

        self._closed = False

    async def batch_op(self, op: PoolerBatchCallback) -> None:
        for pool in self.pools():
            if pool.closed:
                continue

            await op(pool)

    async def ensure_pool(self, addr: Address) -> AbcPool:
        if addr in self._nodes and self._nodes[addr].pool.closed:
            self._erase_addr(addr)

        if addr not in self._nodes:
            async with self._creation_lock(addr):
                if addr not in self._nodes:
                    logger.debug("Create connections pool for %s", addr)
                    pool = await self._create_pool((addr.host, addr.port))
                    self._nodes[addr] = PoolHolder(pool, self._reap_calls)
                    self._pubsub_addrs[addr] = set()

        holder = self._nodes[addr]
        holder.generation = self._reap_calls
        return holder.pool

    async def close_only(self, addrs: Sequence[Address]) -> None:
        collected = []
        for addr in addrs:
            if addr not in self._nodes:
                continue

            holder = self._nodes[addr]

            self._erase_addr(addr)

            holder.pool.close()
            collected.append(holder.pool)

        if collected:
            await asyncio.wait([p.wait_closed() for p in collected])
            logger.info("%d connections pools was closed", len(collected))

    @property
    def closed(self) -> bool:
        return self._closed

    def pools(self) -> List[AbcPool]:
        return [h.pool for h in self._nodes.values()]

    def add_pubsub_channel(
        self,
        addr: Address,
        channel_name: bytes,
        *,
        is_pattern: bool = False,
        is_sharded: bool = False,
    ) -> bool:
        if addr not in self._pubsub_addrs:
            return False

        channel_obj = PubSubChannel(channel_name, is_pattern, is_sharded)
        if channel_obj in self._pubsub_channels:
            return False

        self._pubsub_channels[channel_obj] = addr
        self._pubsub_addrs[addr].add(channel_obj)
        return True

    def remove_pubsub_channel(
        self,
        channel_name: bytes,
        *,
        is_pattern: bool = False,
        is_sharded: bool = False,
    ) -> bool:
        channel_obj = PubSubChannel(channel_name, is_pattern, is_sharded)
        addr = self._pubsub_channels.pop(channel_obj, None)
        if addr:
            if addr in self._pubsub_addrs:
                self._pubsub_addrs[addr].remove(channel_obj)
            return True
        return False

    def get_pubsub_addr(
        self,
        channel_name: bytes,
        *,
        is_pattern: bool = False,
        is_sharded: bool = False,
    ) -> Optional[Address]:
        channel_obj = PubSubChannel(channel_name, is_pattern, is_sharded)
        return self._pubsub_channels.get(channel_obj)

    async def close(self) -> None:
        if self._closed:
            return

        self._closed = True

        if self._reaper_task:
            self._reaper_task.cancel()
            await asyncio.wait([self._reaper_task])

        addrs = tuple(self._nodes.keys())
        pools = tuple(h.pool for h in self._nodes.values())
        self._nodes.clear()
        self._pubsub_channels.clear()
        self._pubsub_addrs.clear()

        if addrs:
            logger.info("Close connections pools for: %s", addrs)
            for pool in pools:
                pool.close()

            await asyncio.wait([pool.wait_closed() for pool in pools])

    async def _reap_pools(self) -> List[AbcPool]:
        current_gen = self._reap_calls
        self._reap_calls += 1

        collected = []
        try:
            for addr, h in tuple(self._nodes.items()):
                if h.generation < current_gen:
                    h.pool.close()

                    # cleanup collections
                    self._erase_addr(addr)

                    collected.append(h.pool)
        except Exception as e:
            logger.error("Unexpected error while collect outdate pools: %r", e)

        if collected:
            for pool in collected:
                try:
                    await pool.wait_closed()
                except (asyncio.CancelledError, SystemExit, KeyboardInterrupt):
                    raise
                except BaseException as e:
                    logger.error("Unexpected error while pool closing: %r", e)

            logger.info("%d idle connections pools reaped", len(collected))

        return collected

    async def _reaper(self) -> None:
        while True:
            await asyncio.sleep(self._reap_frequency)
            await self._reap_pools()

    def _erase_addr(self, addr: Address) -> None:
        del self._nodes[addr]
        channels = self._pubsub_addrs.pop(addr)
        for ch in channels:
            del self._pubsub_channels[ch]
