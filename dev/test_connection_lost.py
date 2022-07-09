import argparse
import asyncio
import logging
import random
import signal
from contextlib import suppress
from typing import Counter, Dict, Mapping

from aioredis_cluster.aioredis import Redis, create_redis_pool
from aioredis_cluster.pool import ConnectionsPool


logger = logging.getLogger(__name__)


async def tick_log(redis: Redis, routines_counters: Mapping[int, Counter]) -> None:
    count = 0
    while True:
        await asyncio.sleep(5)
        count += 1
        logger.info("tick %d", count)
        logger.info("tick %d: Redis %r", count, redis)
        routines = sorted(
            routines_counters.items(),
            key=lambda item: item[1]["redis_count"],
            reverse=True,
        )
        for routine_id, counters in routines:
            logger.info("tick %d: %s: %r", count, routine_id, counters)


async def keys_routine(
    *,
    redis: Redis,
    routine_id: int,
    routine_counters: Counter,
) -> None:
    key = f"key:{routine_id}"
    ch_name = "channel"
    while True:
        routine_counters["key_incr"] += 1

        try:
            await asyncio.sleep(0 if random.random() >= 0.5 else 0.001)
            with (await redis) as conn:
                await conn.incr(key)
                await conn.get(key)
                await asyncio.sleep(random.random())
                await conn.publish(ch_name, key)
            # await asyncio.sleep(30)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("Routine %s exception: %r", routine_id, e)


async def pubsub_routine(
    *,
    redis: Redis,
    routine_id: int,
    routine_counters: Counter,
) -> None:
    ch_name = "channel"
    while True:
        try:
            await asyncio.sleep(0 if random.random() >= 0.5 else 0.001)
            ch = (await redis.subscribe(ch_name))[0]
            # routine_counters["subscribe_count"] -= 1
            # # logger.info('Wait channel %s', ch_name)
            await ch.get()
            routine_counters["pubsub_recv"] += 1
            # routine_counters["pubsub_count"] += int(res)
            await redis.unsubscribe(ch_name)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("Channel exception: %r", e)


async def async_main(args):
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

    routine_id = 0
    routine_tasks = {}

    tick_task = None

    try:
        redis = await create_redis_pool(
            node_addr,
            minsize=args.pool_min,
            maxsize=args.pool_max,
            timeout=1.0,
            pool_cls=ConnectionsPool,
        )

        tick_task = loop.create_task(tick_log(redis, routines_counters))

        try:
            for routine_id in range(args.routines):
                routine_counters = routines_counters[routine_id] = Counter()
                routine_task = loop.create_task(
                    keys_routine(
                        redis=redis,
                        routine_id=routine_id,
                        routine_counters=routine_counters,
                    )
                )
                routine_tasks[routine_id] = routine_task

            routine_id += 1
            routine_counters = routines_counters[routine_id] = Counter()
            routine_task = loop.create_task(
                pubsub_routine(
                    redis=redis,
                    routine_id=routine_id,
                    routine_counters=routine_counters,
                )
            )
            routine_tasks[routine_id] = routine_task

            try:
                await asyncio.sleep(args.wait)
            finally:
                for task in routine_tasks.values():
                    task.cancel()
                await asyncio.wait(routine_tasks.values())
        finally:
            redis.close()
            await redis.wait_closed()
    finally:
        if tick_task:
            tick_task.cancel()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("node")
    parser.add_argument("--wait", type=int, default=30)
    parser.add_argument("--routines", type=int, default=10)
    parser.add_argument("--pool-min", type=int, default=1)
    parser.add_argument("--pool-max", type=int, default=2)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    loop = asyncio.get_event_loop()
    main_task = loop.create_task(async_main(args))

    loop.add_signal_handler(signal.SIGINT, lambda: loop.stop())
    loop.add_signal_handler(signal.SIGTERM, lambda: loop.stop())
    main_task.add_done_callback(lambda f: loop.stop())

    try:
        loop.run_forever()
    finally:
        if not main_task.done() and not main_task.cancelled():
            main_task.cancel()
            with suppress(asyncio.CancelledError):
                loop.run_until_complete(main_task)
        loop.close()


if __name__ == "__main__":
    main()
