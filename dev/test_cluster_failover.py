import argparse
import asyncio
import logging
import random
from collections import deque
from typing import Counter, Deque, Dict, Mapping, Optional, Set

from aioredis import Redis

from aioredis_cluster import create_redis_cluster


logger = logging.getLogger(__name__)


async def tick_log(routines_counters: Mapping[int, Counter]) -> None:
    count = 0
    while True:
        await asyncio.sleep(5)
        count += 1
        logger.info("tick %d", count)
        routines = sorted(
            routines_counters.items(),
            key=lambda item: item[1]["redis_count"],
            reverse=True,
        )
        for routine_id, counters in routines:
            logger.info("tick %d: %s: %r", count, routine_id, counters)


async def routine(
    routine_id: int,
    redis: Redis,
    *,
    routines_counters: Dict[int, Counter[str]],
) -> None:
    logger.info("%d: Routine created", routine_id)
    counters = routines_counters[routine_id] = Counter()
    while True:
        await routine_work(redis, routine_id, counters)


async def routine_work(redis: Redis, routine_id: int, counters: Counter[str]) -> None:
    count: Optional[int] = None
    try:
        counters["local_count"] += 1
        key = f"{{r{routine_id}}}:counter"
        # if random.random() >= 0.5:
        if True or routine_id % 2 == 0:
            pool = await redis.keys_master(key)
            with (await pool) as redis_conn:
                count = await redis_conn.incr(key)
        else:
            count = await redis.incr(key)
        if count is not None:
            counters["redis_count"] = count
        # if counters["local_count"] % 1000 == 0:
        #     logger.info(
        #         "%d: local_count=%d, redis_count=%d",
        #         routine_id,
        #         counters["local_count"],
        #         count,
        #     )
    except asyncio.CancelledError:
        logger.warning("%d: aborted, local_count=%d", routine_id, counters["local_count"])
        raise
    except asyncio.TimeoutError:
        logger.error(
            "%d: Acquire connnect and execute is timed out, last count - %s", routine_id, count
        )
    except Exception as e:
        logger.exception("%d: Redis error: %r", routine_id, e)
        # await asyncio.sleep(0.001)
    # await asyncio.sleep(random.random())


def create_routines(
    num: int,
    redis: Redis,
    *,
    start_id: int = 1,
    routines_counters: Dict[int, Counter[str]],
) -> Deque[asyncio.Task]:
    loop = asyncio.get_event_loop()
    tasks: Deque[asyncio.Task] = deque()
    for i in range(start_id, start_id + num):
        tasks.append(loop.create_task(routine(i, redis, routines_counters=routines_counters)))
    return tasks


async def start_commands_gun(
    *,
    redis: Redis,
    routines_num: int,
    routines_counters: Dict[int, Counter[str]],
) -> None:
    routines_id_pool: asyncio.Queue[int] = asyncio.LifoQueue(maxsize=routines_num)
    for routine_id in range(1, routines_num + 1):
        routines_id_pool.put_nowait(routine_id)
        routines_counters[routine_id] = Counter()
        await routine_work(
            redis,
            routine_id,
            routines_counters[routine_id],
        )

    routine_tasks: Set[asyncio.Task] = set()

    def back_to_pool(routine_id: int):
        def cb(task):
            routine_tasks.remove(task)
            routines_id_pool.put_nowait(routine_id)

        return cb

    try:
        while True:
            routine_id = await routines_id_pool.get()
            routine_task = asyncio.ensure_future(
                routine_work(
                    redis,
                    routine_id,
                    routines_counters[routine_id],
                )
            )
            routine_tasks.add(routine_task)
            routine_task.add_done_callback(back_to_pool(routine_id))
            await asyncio.sleep(1.001)
    except asyncio.CancelledError:
        num_of_tasks = len(routine_tasks)
        for routine_task in routine_tasks:
            routine_task.cancel()
        if num_of_tasks:
            await asyncio.wait(routine_tasks)

        logger.info("Num of tasks: %d", num_of_tasks)
        raise


async def async_main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("node")
    parser.add_argument("--wait", type=int, default=30)
    parser.add_argument("--routines", type=int, default=10)
    args = parser.parse_args()
    if args.routines < 1:
        raise ValueError("--routines must be positive int")

    try:
        import uvloop
    except ImportError:
        pass
    else:
        uvloop.install()

    loop = asyncio.get_event_loop()
    node_addr: str = args.node

    routines_counters: Dict[int, Counter[str]] = {}

    tick_task = loop.create_task(tick_log(routines_counters))

    redis = await create_redis_cluster(
        [node_addr],
        pool_minsize=1,
        pool_maxsize=1,
        connect_timeout=1.0,
    )
    routine_tasks: Deque[asyncio.Task] = deque()
    try:
        routine_tasks.extend(
            create_routines(
                args.routines,
                redis,
                routines_counters=routines_counters,
            )
        )
        # routine_tasks.append(
        #     loop.create_task(
        #         start_commands_gun(
        #             redis=redis,
        #             routines_num=args.routines,
        #             routines_counters=routines_counters,
        #         )
        #     )
        # )
        # last_routine_id = len(routine_tasks)
        logger.info("Routines %d", len(routine_tasks))

        wait_secs = int(args.wait)
        # cancel_in_sec = wait_secs // len(routine_tasks)
        for sec in range(1, wait_secs + 1):
            await asyncio.sleep(1)
    finally:
        for rt in routine_tasks:
            if not rt.done():
                rt.cancel()
        await asyncio.wait(routine_tasks)

        redis.close()
        await redis.wait_closed()

        tick_task.cancel()


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    loop = asyncio.get_event_loop()
    main_task = loop.create_task(async_main())
    try:
        loop.run_until_complete(main_task)
    except KeyboardInterrupt:
        if not main_task.done() and not main_task.cancelled():
            main_task.cancel()
            loop.run_until_complete(main_task)
    finally:
        loop.close()


if __name__ == "__main__":
    main()
