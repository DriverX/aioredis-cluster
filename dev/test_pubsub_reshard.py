import argparse
import asyncio
import logging
import random
import signal
from collections import deque
from itertools import cycle
from typing import Counter, Deque, Dict, Mapping, Optional, Sequence

from aioredis_cluster import Cluster, ClusterNode, RedisCluster, create_redis_cluster
from aioredis_cluster.aioredis import Channel
from aioredis_cluster.compat.asyncio import timeout
from aioredis_cluster.crc import key_slot

logger = logging.getLogger(__name__)


async def tick_log(
    tick: float,
    routines_counters: Mapping[int, Counter[str]],
    global_counters: Counter[str],
) -> None:
    count = 0
    last = False
    while True:
        try:
            await asyncio.sleep(tick)
        except asyncio.CancelledError:
            last = True

        count += 1
        logger.info("tick %d", count)
        logger.info("tick %d: %r", count, global_counters)
        routines = sorted(
            routines_counters.items(),
            key=lambda item: item[0],
            reverse=False,
        )
        for routine_id, counters in routines:
            logger.info("tick %d: %s: %r", count, routine_id, counters)

        if last:
            break


def get_channel_name(routine_id: int) -> str:
    return f"ch:{routine_id}:{{shard}}"


class ChannelNotClosedError(Exception):
    pass


async def subscribe_routine(
    *,
    redis: RedisCluster,
    routine_id: int,
    counters: Counter[str],
    global_counters: Counter[str],
):
    await asyncio.sleep(0.5)
    ch_name = get_channel_name(routine_id)
    while True:
        counters["routine:subscribes"] += 1
        prev_ch: Optional[Channel] = None
        try:
            pool = await redis.keys_master(ch_name)
            global_counters["subscribe_in_fly"] += 1
            try:
                ch: Channel = (await pool.ssubscribe(ch_name))[0]
                if prev_ch is not None and ch is prev_ch:
                    logger.error("%s: Previous Channel is current: %r", routine_id, ch)
                prev_ch = ch
                # logger.info('Wait channel %s', ch_name)
                try:
                    async with timeout(1.0):
                        res = await ch.get()
                except asyncio.TimeoutError:
                    counters["timeouts"] += 1
                    global_counters["timeouts"] += 1
                    # logger.warning("%s: ch.get() is timed out", routine_id)
                else:
                    if res is None:
                        counters["msg:received:None"] += 1
                        global_counters["msg:received:None"] += 1
                    else:
                        counters["msg:received"] += 1
                        global_counters["msg:received"] += 1
                await pool.sunsubscribe(ch_name)
            finally:
                global_counters["subscribe_in_fly"] -= 1

            if not ch._queue.closed:
                raise ChannelNotClosedError()

        except asyncio.CancelledError:
            break
        except Exception as e:
            counters["errors"] += 1
            counters[f"errors:{type(e).__name__}"] += 1
            global_counters["errors"] += 1
            global_counters[f"errors:{type(e).__name__}"] += 1
            logger.error("%s: Channel exception: %r", routine_id, e)


async def publish_routine(
    *,
    redis: RedisCluster,
    routines: Sequence[int],
    global_counters: Counter[str],
):
    routines_cycle = cycle(routines)
    for routine_id in routines_cycle:
        await asyncio.sleep(random.uniform(0.001, 0.1))
        channel_name = get_channel_name(routine_id)
        await redis.spublish(channel_name, f"msg:{routine_id}")
        global_counters["published_msgs"] += 1


async def cluster_move_slot(
    *,
    slot: int,
    node_src: RedisCluster,
    node_src_info: ClusterNode,
    node_dest: RedisCluster,
    node_dest_info: ClusterNode,
) -> None:
    await node_dest.cluster_setslot(slot, "IMPORTING", node_src_info.node_id)
    await node_src.cluster_setslot(slot, "MIGRATING", node_dest_info.node_id)
    while True:
        keys = await node_src.cluster_get_keys_in_slots(slot, 100)
        if not keys:
            break
        await node_src.migrate_keys(
            node_dest_info.addr.host,
            node_dest_info.addr.port,
            keys,
            0,
            5000,
        )
    await node_dest.cluster_setslot(slot, "NODE", node_dest_info.node_id)
    await node_src.cluster_setslot(slot, "NODE", node_dest_info.node_id)


async def reshard_routine(
    *,
    redis: RedisCluster,
    global_counters: Counter[str],
):
    cluster: Cluster = redis.connection
    cluster_state = await cluster.get_cluster_state()

    moving_slot = key_slot(b"shard")
    master1 = await cluster.keys_master(b"shard")

    # search another master
    count = 0
    while True:
        count += 1
        key = f"key:{count}"
        master2 = await cluster.keys_master(key)
        if master2.address != master1.address:
            break

    master1_info = cluster_state.addr_node(master1.address)
    master2_info = cluster_state.addr_node(master2.address)
    logger.info("Master1 info - %r", master1_info)
    logger.info("Master2 info - %r", master2_info)

    node_src = master1
    node_src_info = master1_info
    node_dest = master2
    node_dest_info = master2_info

    while True:
        await asyncio.sleep(random.uniform(2.0, 5.0))
        global_counters["reshards"] += 1
        logger.info(
            "Start moving slot %s from %s to %s", moving_slot, node_src.address, node_dest.address
        )
        move_slot_task = asyncio.create_task(
            cluster_move_slot(
                slot=moving_slot,
                node_src=node_src,
                node_src_info=node_src_info,
                node_dest=node_dest,
                node_dest_info=node_dest_info,
            )
        )
        try:
            await asyncio.shield(move_slot_task)
        except asyncio.CancelledError:
            await move_slot_task
            raise
        except Exception as e:
            logger.exception("Unexpected error on reshard: %r", e)
            break

        # swap nodes
        node_src, node_src_info, node_dest, node_dest_info = (
            node_dest,
            node_dest_info,
            node_src,
            node_src_info,
        )


async def async_main(args: argparse.Namespace) -> None:
    loop = asyncio.get_event_loop()
    node_addr: str = args.node

    global_counters: Counter[str] = Counter()
    routines_counters: Dict[int, Counter[str]] = {}

    tick_task = loop.create_task(tick_log(5.0, routines_counters, global_counters))

    redis = await create_redis_cluster(
        [node_addr],
        pool_minsize=1,
        pool_maxsize=1,
        connect_timeout=1.0,
        follow_cluster=True,
    )
    routine_tasks: Deque[asyncio.Task] = deque()

    reshard_routine_task = asyncio.create_task(
        reshard_routine(
            redis=redis,
            global_counters=global_counters,
        )
    )
    routine_tasks.append(reshard_routine_task)

    try:
        for routine_id in range(1, args.routines + 1):
            counters = routines_counters[routine_id] = Counter()
            routine_task = asyncio.create_task(
                subscribe_routine(
                    redis=redis,
                    routine_id=routine_id,
                    counters=counters,
                    global_counters=global_counters,
                )
            )
            routine_tasks.append(routine_task)

        publish_routine_task = asyncio.create_task(
            publish_routine(
                redis=redis,
                routines=list(range(1, args.routines + 1)),
                global_counters=global_counters,
            )
        )
        routine_tasks.append(publish_routine_task)

        logger.info("Routines %d", len(routine_tasks))

        await asyncio.sleep(args.wait)
    finally:
        logger.info("Cancel %d routines", len(routine_tasks))
        for rt in routine_tasks:
            if not rt.done():
                rt.cancel()
        await asyncio.wait(routine_tasks)

        logger.info("Close redis client")
        redis.close()
        await redis.wait_closed()

        logger.info("Cancel tick task")
        tick_task.cancel()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("node")
    parser.add_argument("--wait", type=float, default=30)
    parser.add_argument("--routines", type=int, default=10)
    args = parser.parse_args()
    if args.routines < 1:
        parser.error("--routines must be positive int")
    if args.wait < 0:
        parser.error("--wait must be positive float or int")

    try:
        import uvloop
    except ImportError:
        pass
    else:
        uvloop.install()

    logging.basicConfig(level=logging.DEBUG)

    loop = asyncio.get_event_loop()
    main_task = loop.create_task(async_main(args))

    main_task.add_done_callback(lambda f: loop.stop())
    loop.add_signal_handler(signal.SIGINT, lambda: loop.stop())
    loop.add_signal_handler(signal.SIGTERM, lambda: loop.stop())

    try:
        loop.run_forever()
    finally:
        if not main_task.done() and not main_task.cancelled():
            main_task.cancel()
            loop.run_until_complete(main_task)
        loop.close()


if __name__ == "__main__":
    main()
