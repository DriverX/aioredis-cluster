import argparse
import asyncio
import logging
import signal
from collections import deque
from typing import Counter, Deque, Dict, Mapping, Optional

import async_timeout

from aioredis_cluster import RedisCluster, create_redis_cluster
from aioredis_cluster.aioredis import Channel

logger = logging.getLogger(__name__)


async def tick_log(
    tick: float,
    routines_counters: Mapping[int, Counter[str]],
    global_counters: Counter[str],
) -> None:
    count = 0
    while True:
        await asyncio.sleep(tick)
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


async def subscribe_routine(
    *,
    redis: RedisCluster,
    routine_id: int,
    counters: Counter[str],
    global_counters: Counter[str],
):
    await asyncio.sleep(0.5)
    ch_name = f"ch:{routine_id}:{{shard}}"
    while True:
        counters["routine:subscribes"] += 1
        prev_ch: Optional[Channel] = None
        try:
            pool = await redis.keys_master(ch_name)
            global_counters["subscribe_in_fly"] += 1
            try:
                ch: Channel = (await pool.subscribe(ch_name))[0]
                if prev_ch is not None and ch is prev_ch:
                    logger.error("%s: Previous Channel is current: %r", routine_id, ch)
                prev_ch = ch
                # logger.info('Wait channel %s', ch_name)
                try:
                    async with async_timeout.timeout(1.0):
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
                await pool.unsubscribe(ch_name)
            finally:
                global_counters["subscribe_in_fly"] -= 1
            assert ch.is_active is False
        except asyncio.CancelledError:
            break
        except Exception as e:
            counters["errors"] += 1
            counters[f"errors:{type(e).__name__}"] += 1
            global_counters["errors"] += 1
            global_counters[f"errors:{type(e).__name__}"] += 1
            logger.error("%s: Channel exception: %r", routine_id, e)


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

    logging.basicConfig(level=logging.INFO)

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
            loop.run_until_complete(asyncio.wait([main_task]))
        loop.close()


if __name__ == "__main__":
    main()
