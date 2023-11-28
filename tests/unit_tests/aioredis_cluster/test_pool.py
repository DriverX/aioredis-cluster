import asyncio
from unittest import mock

import pytest

from aioredis_cluster.compat.asyncio import timeout
from aioredis_cluster.pool import ConnectionsPool


async def moment_10():
    for i in range(10):
        await asyncio.sleep(0)


def create_conn_mock():
    conn = mock.NonCallableMock()
    conn.closed = False
    conn.in_transaction = False
    conn.in_pubsub = False
    conn._waiters = []
    conn.readonly = False
    conn.db = 0
    conn.execute = mock.AsyncMock(return_value=b"OK")
    conn.wait_closed = mock.AsyncMock(return_value=None)

    def close_se():
        conn.closed = True

    conn.close.side_effect = close_se

    conn._last_use_generation = 0

    def set_last_use_generation_se(gen: int):
        conn._last_use_generation = gen

    def get_last_use_generation_se() -> int:
        return conn._last_use_generation

    conn.set_last_use_generation.side_effect = set_last_use_generation_se
    conn.get_last_use_generation.side_effect = get_last_use_generation_se

    return conn


async def test_acquire__correct_return_order(mocker):
    addr = ("127.0.0.1", 6379)

    conn_mocks = [
        create_conn_mock(),
    ]

    pool = ConnectionsPool(addr, minsize=1, maxsize=1)
    mocked_create_new_connection = mocker.patch.object(
        pool,
        "_create_new_connection",
        new=mock.AsyncMock(
            side_effect=conn_mocks,
        ),
    )

    conn1 = conn_mocks[0]
    acquired_conn1 = await pool.acquire()

    assert acquired_conn1 is conn1

    with pytest.raises(asyncio.TimeoutError):
        async with timeout(0.001):
            await pool.acquire()

    acquire_task1 = asyncio.ensure_future(pool.acquire())
    acquire_task2 = asyncio.ensure_future(pool.acquire())

    await asyncio.sleep(0)

    assert pool._conn_waiters_count == 2

    pool.release(conn1)
    acquire_task3 = asyncio.ensure_future(pool.acquire())

    # make several extra loops
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert pool._conn_waiters_count == 2

    assert acquire_task1.done() is True
    assert acquire_task2.done() is False
    assert acquire_task3.done() is False
    assert acquire_task1.result() is conn1

    pool.release(conn1)

    # make several extra loops
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert acquire_task2.done() is True
    assert acquire_task3.done() is False
    assert acquire_task2.result() is conn1

    pool.release(conn1)

    # make several extra loops
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert acquire_task3.done() is True
    assert acquire_task3.result() is conn1

    mocked_create_new_connection.assert_awaited_once()


async def test_acquire__release_multiple_connections_at_time(mocker):
    addr = ("127.0.0.1", 6379)

    conn_mocks = [
        create_conn_mock(),
        create_conn_mock(),
        create_conn_mock(),
    ]

    pool = ConnectionsPool(addr, minsize=1, maxsize=3)
    mocked_create_new_connection = mocker.patch.object(
        pool,
        "_create_new_connection",
        new=mock.AsyncMock(
            side_effect=conn_mocks,
        ),
    )

    acquired_conn1 = await pool.acquire()
    acquired_conn2 = await pool.acquire()
    acquired_conn3 = await pool.acquire()

    conn_waiters = [
        asyncio.ensure_future(pool.acquire()),
        asyncio.ensure_future(pool.acquire()),
        asyncio.ensure_future(pool.acquire()),
        asyncio.ensure_future(pool.acquire()),
        asyncio.ensure_future(pool.acquire()),
        asyncio.ensure_future(pool.acquire()),
    ]

    await asyncio.sleep(0)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    conn_waiters[4].cancel()
    pool.release(acquired_conn1)
    pool.release(acquired_conn3)

    await asyncio.sleep(0)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    # 3 because one is manualy cancelled before
    assert sum(w.done() for w in conn_waiters) == 3
    assert conn_waiters[0].result() is acquired_conn1
    assert conn_waiters[1].result() is acquired_conn3

    pool.release(acquired_conn1)
    pool.release(acquired_conn2)
    pool.release(acquired_conn3)

    await asyncio.sleep(0)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert sum(w.done() for w in conn_waiters) == 6

    assert conn_waiters[2].result() is acquired_conn1
    assert conn_waiters[3].result() is acquired_conn2
    assert conn_waiters[5].result() is acquired_conn3

    mocked_create_new_connection.await_count == 3


async def test_idle_connections_detection__initialize(mocker):
    addr = ("127.0.0.1", 6379)

    conn_mocks = [
        create_conn_mock(),
        create_conn_mock(),
        create_conn_mock(),
    ]

    mocked_create_connection = mocker.patch(
        ConnectionsPool.__module__ + ".create_connection",
        new=mock.AsyncMock(side_effect=conn_mocks),
    )

    pool = ConnectionsPool(addr, minsize=1, maxsize=3)
    await pool._fill_free(override_min=False)

    assert pool._idle_connections_collect_gen == 1
    mocked_create_connection.assert_awaited_once()
    conn_mocks[0].set_last_use_generation.assert_called_once_with(1)
    conn_mocks[1].set_last_use_generation.assert_not_called()
    conn_mocks[2].set_last_use_generation.assert_not_called()


async def test_idle_connections_detection__connection_mark(mocker):
    addr = ("127.0.0.1", 6379)

    conn_mocks = [
        create_conn_mock(),
        create_conn_mock(),
        create_conn_mock(),
    ]

    mocker.patch(
        ConnectionsPool.__module__ + ".create_connection",
        new=mock.AsyncMock(side_effect=conn_mocks),
    )

    pool = ConnectionsPool(addr, minsize=1, maxsize=3)

    collect_event = asyncio.Event()

    async def collect_wait():
        await collect_event.wait()

    mocked_idle_connections_collect_wait = mocker.patch.object(
        pool,
        "_idle_connections_collect_wait",
        side_effect=collect_wait,
    )

    assert pool._idle_connections_collect_gen == 1

    conn1 = await pool.acquire()
    assert pool._idle_connections_collect_task is not None
    await asyncio.sleep(0)

    conn2 = await pool.acquire()

    collect_event.set()
    collect_event.clear()
    await asyncio.sleep(0)
    assert mocked_idle_connections_collect_wait.await_count == 2
    assert pool._idle_connections_collect_gen == 2

    conn3 = await pool.acquire()

    assert conn1._last_use_generation == 1
    assert conn2._last_use_generation == 1
    assert conn3._last_use_generation == 2

    pool.release(conn1)
    pool.release(conn2)
    pool.release(conn3)

    await asyncio.sleep(0)

    assert conn1._last_use_generation == 2
    assert conn2._last_use_generation == 2
    assert conn3._last_use_generation == 2


async def test_idle_connections_detection__collect(mocker):
    addr = ("127.0.0.1", 6379)

    conn_mocks = [
        create_conn_mock(),
        create_conn_mock(),
        create_conn_mock(),
    ]

    mocked_create_connection = mocker.patch(
        ConnectionsPool.__module__ + ".create_connection",
        new=mock.AsyncMock(side_effect=conn_mocks),
    )

    pool = ConnectionsPool(addr, minsize=1, maxsize=3)
    assert pool._idle_connections_collect_gen == 1

    collect_event = asyncio.Event()

    async def collect_wait():
        await collect_event.wait()

    mocker.patch.object(
        pool,
        "_idle_connections_collect_wait",
        side_effect=collect_wait,
    )

    await pool._fill_free(override_min=False)
    conn1 = await pool.acquire()
    conn2 = await pool.acquire()
    pool.release(conn1)
    pool.release(conn2)
    await moment_10()
    assert pool.freesize == 2

    # collect gen 2, nothing to close
    collect_event.set()
    collect_event.clear()
    await moment_10()
    assert pool._idle_connections_collect_gen == 2

    # collect gen 3, close 1 connection
    collect_event.set()
    collect_event.clear()
    await moment_10()
    assert pool._idle_connections_collect_gen == 3

    conn_mocks[0].close.assert_called_once_with()
    conn_mocks[0].wait_closed.assert_awaited_once_with()
    conn_mocks[1].close.assert_not_called()

    # collect gen 4, nothing to close
    collect_event.set()
    collect_event.clear()
    await moment_10()
    assert pool._idle_connections_collect_gen == 4

    conn2 = await pool.acquire()
    assert conn2 is conn_mocks[1]
    conn3 = await pool.acquire()
    assert conn3 is conn_mocks[2]
    pool.release(conn2)
    pool.release(conn3)
    await moment_10()

    assert pool.freesize == 2
    assert mocked_create_connection.await_count == 3
    assert conn_mocks[0]._last_use_generation == 1
    assert conn_mocks[1]._last_use_generation == 4
    assert conn_mocks[2]._last_use_generation == 4
    assert pool._idle_connections_collect_gen == 4
