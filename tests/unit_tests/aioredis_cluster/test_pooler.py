import asyncio
from unittest import mock

import pytest

from aioredis_cluster.abc import AbcConnection, AbcPool
from aioredis_cluster.pooler import Pooler
from aioredis_cluster.structs import Address


def create_pool_mock():
    mocked = mock.NonCallableMock()
    mocked.closed = False
    mocked.wait_closed = mock.AsyncMock()
    return mocked


async def test_ensure_pool__identical_address(add_async_finalizer):
    mocked_create_pool = mock.AsyncMock(
        return_value=create_pool_mock(),
    )
    pooler = Pooler(mocked_create_pool)
    add_async_finalizer(lambda: pooler.close())

    result = await pooler.ensure_pool(Address("localhost", 1234))

    assert result is mocked_create_pool.return_value
    mocked_create_pool.assert_called_once_with(("localhost", 1234))

    result2 = await pooler.ensure_pool(Address("localhost", 1234))

    assert result2 is result
    assert mocked_create_pool.call_count == 1


async def test_ensure_pool__multiple(add_async_finalizer):
    pools = [
        mock.AsyncMock(AbcConnection),
        mock.AsyncMock(AbcConnection),
        mock.AsyncMock(AbcConnection),
    ]
    mocked_create_pool = mock.AsyncMock(side_effect=pools)

    pooler = Pooler(mocked_create_pool)
    add_async_finalizer(lambda: pooler.close())

    result1 = await pooler.ensure_pool(Address("localhost", 1234))
    result2 = await pooler.ensure_pool(Address("localhost", 4321))
    result3 = await pooler.ensure_pool(Address("127.0.0.1", 1234))

    assert result1 is pools[0]
    assert result2 is pools[1]
    assert result3 is pools[2]
    assert mocked_create_pool.call_count == 3
    mocked_create_pool.assert_has_calls(
        [
            mock.call(("localhost", 1234)),
            mock.call(("localhost", 4321)),
            mock.call(("127.0.0.1", 1234)),
        ]
    )


async def test_ensure_pool__only_one(add_async_finalizer):
    event_loop = asyncio.get_running_loop()
    pools = {
        ("h1", 1): create_pool_mock(),
        ("h2", 2): create_pool_mock(),
    }
    pool_creation_fut = event_loop.create_future()

    async def create_pool_se(addr):
        nonlocal pool_creation_fut
        await pool_creation_fut
        return pools[addr]

    mocked_create_pool = mock.AsyncMock(side_effect=create_pool_se)

    pooler = Pooler(mocked_create_pool)
    add_async_finalizer(lambda: pooler.close())

    tasks = []
    for i in range(10):
        for addr in pools.keys():
            task = event_loop.create_task(pooler.ensure_pool(Address(addr[0], addr[1])))
            tasks.append(task)

    pool_creation_fut.set_result(None)

    results = await asyncio.gather(*tasks)

    assert len(results) == 20
    assert len([r for r in results if r is pools[("h1", 1)]]) == 10
    assert len([r for r in results if r is pools[("h2", 2)]]) == 10
    assert mocked_create_pool.call_count == 2


async def test_ensure_pool__error(add_async_finalizer):
    pools = [RuntimeError(), mock.AsyncMock(AbcConnection)]
    mocked_create_pool = mock.AsyncMock(side_effect=pools)

    pooler = Pooler(mocked_create_pool)
    add_async_finalizer(lambda: pooler.close())

    addr = Address("localhost", 1234)
    with pytest.raises(RuntimeError):
        await pooler.ensure_pool(addr)

    result = await pooler.ensure_pool(addr)

    assert result is pools[1]
    assert mocked_create_pool.call_count == 2
    mocked_create_pool.assert_has_calls(
        [
            mock.call(("localhost", 1234)),
            mock.call(("localhost", 1234)),
        ]
    )


async def test_close__empty_pooler():
    pooler = Pooler(mock.AsyncMock())
    await pooler.close()

    assert pooler.closed is True


async def test_close__with_pools(mocker, add_async_finalizer):
    addrs_pools = [
        (Address("h1", 1), create_pool_mock()),
        (Address("h2", 2), create_pool_mock()),
    ]
    addrs = [p[0] for p in addrs_pools]
    pools = [p[1] for p in addrs_pools]
    mocked_create_pool = mock.AsyncMock(side_effect=pools)

    pooler = Pooler(mocked_create_pool)
    add_async_finalizer(lambda: pooler.close())

    result1 = await pooler.ensure_pool(addrs[0])
    result2 = await pooler.ensure_pool(addrs[1])

    assert len(pooler._nodes) == 2

    await pooler.close()

    assert len(pooler._nodes) == 0
    assert pooler.closed is True
    result1.close.assert_called_once()
    result2.close.assert_called_once()
    result1.wait_closed.assert_called_once()
    result2.wait_closed.assert_called_once()


async def test_reap_pools(mocker, add_async_finalizer):
    addrs_pools = [
        (Address("h1", 1), create_pool_mock()),
        (Address("h2", 2), create_pool_mock()),
    ]
    addrs = [p[0] for p in addrs_pools]
    pools = [p[1] for p in addrs_pools]
    mocked_create_pool = mock.AsyncMock(side_effect=pools)

    pooler = Pooler(mocked_create_pool, reap_frequency=-1)
    add_async_finalizer(lambda: pooler.close())

    # create pools
    await pooler.ensure_pool(addrs[0])
    await pooler.ensure_pool(addrs[1])
    # try to reap pools
    reaped = await pooler._reap_pools()

    assert len(reaped) == 0

    # touch only one pool
    await pooler.ensure_pool(addrs[1])
    reaped = await pooler._reap_pools()

    assert len(reaped) == 1
    assert reaped[0] is pools[0]
    assert len(pooler._nodes) == 1

    reaped = await pooler._reap_pools()
    assert len(reaped) == 1
    assert reaped[0] is pools[1]
    assert len(pooler._nodes) == 0


async def test_reaper(mocker, add_async_finalizer):
    pooler = Pooler(mock.AsyncMock(), reap_frequency=0)
    add_async_finalizer(lambda: pooler.close())

    assert pooler._reap_calls == 0

    # force two event loop cycle
    # 1 - creation reaper task
    # 2 - reaper one cycle
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert pooler._reap_calls == 1

    await pooler.close()

    assert pooler._reaper_task.cancelled() is True


async def test_add_pubsub_channel__no_addr(add_async_finalizer):
    pooler = Pooler(mock.AsyncMock(), reap_frequency=-1)
    add_async_finalizer(lambda: pooler.close())

    addr = Address("h1", 1234)
    result = pooler.add_pubsub_channel(addr, b"channel", is_pattern=False)

    assert result is False


async def test_add_pubsub_channel(add_async_finalizer):
    pooler = Pooler(mock.AsyncMock(return_value=create_pool_mock()), reap_frequency=-1)
    add_async_finalizer(lambda: pooler.close())

    addr1 = Address("h1", 1234)
    addr2 = Address("h2", 1234)
    pooler._pubsub_addrs[addr1] = set()
    pooler._pubsub_addrs[addr2] = set()

    result1 = pooler.add_pubsub_channel(addr1, b"ch1", is_pattern=False)
    result2 = pooler.add_pubsub_channel(addr1, b"ch1", is_pattern=True)
    result3 = pooler.add_pubsub_channel(addr1, b"ch2", is_pattern=False)
    result4 = pooler.add_pubsub_channel(addr2, b"ch3", is_pattern=False)
    result5 = pooler.add_pubsub_channel(addr1, b"ch3", is_pattern=False)
    result6 = pooler.add_pubsub_channel(addr1, b"ch3", is_sharded=True)
    result7 = pooler.add_pubsub_channel(addr1, b"ch3", is_sharded=True)

    assert result1 is True
    assert result2 is True
    assert result3 is True
    assert result4 is True
    assert result5 is False
    assert result6 is True
    assert result7 is False
    assert len(pooler._pubsub_addrs[addr1]) == 4
    assert len(pooler._pubsub_addrs[addr2]) == 1
    assert len(pooler._pubsub_channels) == 5

    collected_channels = [(ch.name, ch.is_pattern) for ch in pooler._pubsub_channels]
    assert (b"ch1", False) in collected_channels
    assert (b"ch1", True) in collected_channels
    assert (b"ch2", False) in collected_channels
    assert (b"ch3", False) in collected_channels


async def test_remove_pubsub_channel__no_addr(add_async_finalizer):
    pooler = Pooler(mock.AsyncMock(), reap_frequency=-1)
    add_async_finalizer(lambda: pooler.close())

    result = pooler.remove_pubsub_channel(b"channel", is_pattern=False)
    assert result is False


async def test_remove_pubsub_channel(add_async_finalizer):
    pooler = Pooler(mock.AsyncMock(), reap_frequency=-1)
    add_async_finalizer(lambda: pooler.close())

    addr1 = Address("h1", 1234)
    addr2 = Address("h2", 1234)
    pooler._pubsub_addrs[addr1] = set()
    pooler._pubsub_addrs[addr2] = set()

    pooler.add_pubsub_channel(addr1, b"ch1", is_pattern=False)
    pooler.add_pubsub_channel(addr1, b"ch2", is_pattern=False)
    pooler.add_pubsub_channel(addr2, b"ch3", is_pattern=True)
    pooler.add_pubsub_channel(addr1, b"ch3", is_sharded=True)

    result1 = pooler.remove_pubsub_channel(b"ch1", is_pattern=False)
    result2 = pooler.remove_pubsub_channel(b"ch1", is_pattern=True)
    result3 = pooler.remove_pubsub_channel(b"ch2", is_pattern=False)
    result4 = pooler.remove_pubsub_channel(b"ch3", is_pattern=True)
    result5 = pooler.remove_pubsub_channel(b"ch3", is_pattern=True)
    result6 = pooler.remove_pubsub_channel(b"ch3", is_sharded=True)

    assert result1 is True
    assert result2 is False
    assert result3 is True
    assert result4 is True
    assert result5 is False
    assert result6 is True
    assert len(pooler._pubsub_addrs[addr1]) == 0
    assert len(pooler._pubsub_addrs[addr2]) == 0
    assert len(pooler._pubsub_channels) == 0


async def test_get_pubsub_addr(add_async_finalizer):
    pooler = Pooler(mock.AsyncMock(), reap_frequency=-1)
    add_async_finalizer(lambda: pooler.close())

    addr1 = Address("h1", 1234)
    addr2 = Address("h2", 1234)
    pooler._pubsub_addrs[addr1] = set()
    pooler._pubsub_addrs[addr2] = set()

    pooler.add_pubsub_channel(addr1, b"ch1", is_pattern=False)
    pooler.add_pubsub_channel(addr2, b"ch2", is_pattern=True)

    result1 = pooler.get_pubsub_addr(b"ch1", is_pattern=False)
    result2 = pooler.get_pubsub_addr(b"ch1", is_pattern=True)
    result3 = pooler.get_pubsub_addr(b"ch2", is_pattern=False)
    result4 = pooler.get_pubsub_addr(b"ch2", is_pattern=True)

    assert result1 == addr1
    assert result2 is None
    assert result3 is None
    assert result4 == addr2


async def test_ensure_pool__create_pubsub_addr_set(add_async_finalizer):
    addr1 = Address("h1", 1234)
    addr2 = Address("h2", 1234)

    pooler = Pooler(mock.AsyncMock(return_value=create_pool_mock()))
    add_async_finalizer(lambda: pooler.close())

    assert len(pooler._pubsub_addrs) == 0

    await pooler.ensure_pool(addr1)
    await pooler.ensure_pool(addr2)
    await pooler.ensure_pool(addr2)

    assert len(pooler._pubsub_addrs) == 2
    assert addr1 in pooler._pubsub_addrs
    assert addr2 in pooler._pubsub_addrs
    assert len(pooler._pubsub_addrs[addr1]) == 0

    pooler._pubsub_addrs[addr1].add(object())
    await pooler.ensure_pool(addr1)

    assert len(pooler._pubsub_addrs[addr1]) == 1


async def test_reap_pools__cleanup_channels(add_async_finalizer):
    pool_factory = mock.AsyncMock(return_value=mock.Mock(AbcPool))
    pooler = Pooler(pool_factory, reap_frequency=-1)
    add_async_finalizer(lambda: pooler.close())

    addr1 = Address("h1", 1)
    addr2 = Address("h2", 2)

    # create pools
    await pooler.ensure_pool(addr1)
    await pooler.ensure_pool(addr2)

    pooler.add_pubsub_channel(addr1, b"ch1")
    pooler.add_pubsub_channel(addr2, b"ch2")

    # try to reap pools
    reaped = await pooler._reap_pools()

    assert len(reaped) == 0

    reaped = await pooler._reap_pools()

    assert len(reaped) == 2
    assert len(pooler._pubsub_addrs) == 0
    assert len(pooler._pubsub_channels) == 0


async def test_close_only(add_async_finalizer):
    pool1 = create_pool_mock()
    pool2 = create_pool_mock()
    pool3 = create_pool_mock()
    mocked_create_pool = mock.AsyncMock(side_effect=[pool1, pool2, pool3])
    pooler = Pooler(mocked_create_pool)
    add_async_finalizer(lambda: pooler.close())

    addr1 = Address("h1", 1)
    addr2 = Address("h2", 2)

    result_pool1 = await pooler.ensure_pool(addr1)
    await pooler.ensure_pool(addr2)
    result_pool2 = await pooler.ensure_pool(addr2)

    assert result_pool1 is pool1
    assert result_pool2 is pool2
    assert mocked_create_pool.call_count == 2

    await pooler.close_only([addr2])

    pool2.close.assert_called_once_with()
    pool2.wait_closed.assert_called_once_with()

    result_pool3 = await pooler.ensure_pool(addr2)
    assert result_pool3 is pool3

    result_pool1 = await pooler.ensure_pool(addr1)
    assert result_pool1 is pool1
