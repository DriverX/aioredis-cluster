import asyncio
import logging
import sys
from collections import deque
from typing import Deque, Optional

import async_timeout
from aioredis import Redis, create_pool

from aioredis_cluster.pool import ConnectionsPool

logger = logging.getLogger(__name__)


async def tick_log() -> None:
    count = 0
    while True:
        await asyncio.sleep(5)
        count += 1
        logger.info("tick %d", count)


async def routine(routine_id: int, redis: Redis) -> None:
    logger.info("%d: Routine created", routine_id)
    local_count = 0
    count: Optional[int] = None
    while True:
        try:
            async with async_timeout.timeout(5.0):
                with await redis as redis_conn:
                    count = await redis_conn.incr("counter")
                    local_count += 1
                    if local_count > 0 and local_count % 1000 == 0:
                        logger.info(
                            "%d: local_count=%d, redis_count=%d",
                            routine_id,
                            local_count,
                            count,
                        )
        except asyncio.CancelledError:
            logger.warning("%d: aborted, local_count=%d", routine_id, local_count)
            break
        except asyncio.TimeoutError:
            logger.error(
                "%d: Acquire connnect and execute is timed out, last count - %s", routine_id, count
            )
        except Exception as e:
            logger.exception("%d: Redis error: %r", routine_id, e)
            await asyncio.sleep(0.001)


def create_routines(num: int, redis: Redis, start_id: int = 1) -> Deque[asyncio.Task]:
    loop = asyncio.get_event_loop()
    tasks: Deque[asyncio.Task] = deque()
    for i in range(start_id, start_id + num):
        tasks.append(loop.create_task(routine(i, redis)))
    return tasks


async def conn_acquirer(acquirer_id: int, redis: Redis):
    count = 0
    while True:
        try:
            async with async_timeout.timeout(None):
                with await redis as redis_conn:
                    count += 1
                    logger.info("%d: %d acquired conn", acquirer_id, count)
                    await redis_conn.incr("counter")
                    await asyncio.sleep(1)
        except asyncio.CancelledError:
            break
        except asyncio.TimeoutError:
            logger.error(
                "%d: Acquire connnect and execute is timed out, last count - %s", acquirer_id, count
            )
        except Exception as e:
            logger.error("%d: conn_acquirer error: %r", acquirer_id, e)
            raise


async def async_main() -> None:
    loop = asyncio.get_event_loop()
    node_addr: str = sys.argv[1]

    tick_task = loop.create_task(tick_log())

    redis_pool = await create_pool(
        node_addr,
        minsize=1,
        maxsize=1,
        create_connection_timeout=1.0,
        pool_cls=ConnectionsPool,
    )
    redis = Redis(redis_pool)
    try:
        # acquirer_tasks = [
        #     loop.create_task(conn_acquirer(i, redis))
        #     for i in range(20)
        # ]
        # await asyncio.wait(acquirer_tasks)
        # return
        routine_tasks = create_routines(10, redis)
        # last_routine_id = len(routine_tasks)
        logger.info("Routines %d", len(routine_tasks))
        wait_secs = 60
        # cancel_in_sec = wait_secs // len(routine_tasks)
        for sec in range(1, wait_secs + 1):
            logger.info("Redis: %r", redis)
            await asyncio.sleep(1)
            # if sec % cancel_in_sec == 0:
            #     rt = routine_tasks.popleft()
            #     rt.cancel()
            #     routine_tasks.extend(create_routines(1, redis, start_id=last_routine_id + 1))
            #     last_routine_id += 1

        for rt in routine_tasks:
            if not rt.done():
                rt.cancel()
    finally:
        redis.close()
        await redis.wait_closed()

        tick_task.cancel()


def main() -> None:
    logging.basicConfig(level=logging.DEBUG)

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
