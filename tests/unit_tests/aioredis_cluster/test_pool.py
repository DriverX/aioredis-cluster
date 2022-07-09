import asyncio

import async_timeout
import mock
import pytest

from aioredis_cluster.pool import ConnectionsPool


def create_conn_mock():
    conn = mock.NonCallableMock()
    conn.closed = False
    conn.in_transaction = False
    conn.in_pubsub = False
    conn._waiters = []
    conn.readonly = False
    conn.db = 0
    conn.execute = mock.AsyncMock(return_value=b"OK")
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
        async with async_timeout.timeout(0.001):
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
