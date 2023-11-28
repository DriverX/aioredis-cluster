import asyncio
from contextlib import suppress

import pytest

from aioredis_cluster.resource_lock import ResourceLock


async def test_acquire__sequentially():
    event_loop = asyncio.get_running_loop()
    lock = ResourceLock()

    results = []

    async def task(task_id):
        nonlocal results

        await lock.acquire("test")
        results.append(task_id)

    task1 = event_loop.create_task(task(1))
    task2 = event_loop.create_task(task(2))
    task3 = event_loop.create_task(task(3))
    tasks = {task1, task2, task3}

    done, pending = await asyncio.wait(tasks, return_when="FIRST_COMPLETED")

    assert task1 in done
    assert len(done) == 1
    assert results == [1]

    tasks.remove(task1)
    lock.release("test")
    done, pending = await asyncio.wait(tasks, return_when="FIRST_COMPLETED")

    assert task2 in done
    assert len(done) == 1
    assert results == [1, 2]

    tasks.remove(task2)
    lock.release("test")
    done, pending = await asyncio.wait(tasks, return_when="FIRST_COMPLETED")

    assert task3 in done
    assert len(done) == 1
    assert results == [1, 2, 3]


async def test_locked():
    event_loop = asyncio.get_running_loop()
    lock = ResourceLock()

    assert lock.locked("foo") is False
    assert lock.locked("bar") is False

    await lock.acquire("foo")

    assert lock.locked("foo") is True
    assert lock.locked("bar") is False

    # create two bar locks
    await lock.acquire("bar")
    bar_lock_task = event_loop.create_task(lock.acquire("bar"))

    assert lock.locked("foo") is True
    assert lock.locked("bar") is True

    lock.release("foo")

    assert lock.locked("foo") is False
    assert lock.locked("bar") is True

    lock.release("bar")
    await asyncio.sleep(0)

    assert lock.locked("foo") is False
    assert lock.locked("bar") is True

    lock.release("bar")
    await bar_lock_task

    assert lock.locked("foo") is False
    assert lock.locked("bar") is False


async def test_context_manager():
    lock = ResourceLock()

    async with lock("foo"):
        async with lock("bar"):
            async with lock("baz"):
                assert lock.locked("foo") is True
                assert lock.locked("bar") is True
                assert lock.locked("baz") is True

    assert lock.locked("foo") is False
    assert lock.locked("bar") is False
    assert lock.locked("baz") is False

    with suppress(RuntimeError):
        async with lock("foo"):
            async with lock("bar"):
                async with lock("baz"):
                    raise RuntimeError

    assert lock.locked("foo") is False
    assert lock.locked("bar") is False
    assert lock.locked("baz") is False


async def test_release():
    lock = ResourceLock()

    await lock.acquire("test")
    lock.release("test")

    with pytest.raises(RuntimeError, match="Lock is not acquired"):
        lock.release("test")

    with pytest.raises(RuntimeError, match="Lock is not acquired"):
        lock.release("test2")
