import asyncio
from unittest import mock

import asynctest
import pytest

from aioredis_cluster.pooler import Pooler
from aioredis_cluster.structs import Address, PrivatePoolDescription


def create_pool_mock():
    mocked = mock.NonCallableMock()
    mocked.closed = False
    mocked.wait_closed = asynctest.CoroutineMock()
    return mocked


async def test_ensure_pool__identical_address(mocker):
    mocked_create_pool = asynctest.CoroutineMock(
        return_value=create_pool_mock(),
    )
    pooler = Pooler(mocked_create_pool)

    result = await pooler.ensure_pool(Address("localhost", 1234))

    assert result is mocked_create_pool.return_value
    mocked_create_pool.assert_called_once_with(("localhost", 1234))

    result2 = await pooler.ensure_pool(Address("localhost", 1234))

    assert result2 is result
    assert mocked_create_pool.call_count == 1


async def test_ensure_pool__multiple(mocker):
    pools = [object(), object(), object()]
    mocked_create_pool = asynctest.CoroutineMock(side_effect=pools)

    pooler = Pooler(mocked_create_pool)

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


async def test_ensure_pool__only_one(mocker, loop):
    pools = {
        ("h1", 1): create_pool_mock(),
        ("h2", 2): create_pool_mock(),
    }
    pool_creation_fut = loop.create_future()

    async def create_pool_se(addr):
        nonlocal pool_creation_fut
        await pool_creation_fut
        return pools[addr]

    mocked_create_pool = asynctest.CoroutineMock(side_effect=create_pool_se)

    pooler = Pooler(mocked_create_pool)

    tasks = []
    for i in range(10):
        for addr in pools.keys():
            task = loop.create_task(pooler.ensure_pool(Address(addr[0], addr[1])))
            tasks.append(task)

    pool_creation_fut.set_result(None)

    results = await asyncio.gather(*tasks)

    assert len(results) == 20
    assert len([r for r in results if r is pools[("h1", 1)]]) == 10
    assert len([r for r in results if r is pools[("h2", 2)]]) == 10
    assert mocked_create_pool.call_count == 2


async def test_ensure_pool__error(mocker, loop):
    pools = [RuntimeError(), object()]
    mocked_create_pool = asynctest.CoroutineMock(side_effect=pools)

    pooler = Pooler(mocked_create_pool)

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


async def test_get_private_pool(mocker):
    mocked_create_pool = asynctest.CoroutineMock(
        return_value=create_pool_mock(),
    )
    pooler = Pooler(mocked_create_pool)

    private = PrivatePoolDescription(pool_minsize=12, pool_maxsize=42)
    result = await pooler.create_private_pool(Address("localhost", 1234), private)

    assert result is mocked_create_pool.return_value
    mocked_create_pool.assert_called_once_with(("localhost", 1234), 12, 42)

    await pooler.create_private_pool(Address("localhost", 1234), PrivatePoolDescription())

    assert mocked_create_pool.call_count == 2


async def test_create_private_pool__limit(mocker):
    mocked_create_pool = asynctest.CoroutineMock(
        return_value=create_pool_mock(),
    )
    pooler = Pooler(mocked_create_pool, private_pools_limit=1)

    result = await pooler.create_private_pool(Address("localhost", 1234), PrivatePoolDescription())

    assert result is mocked_create_pool.return_value
    mocked_create_pool.assert_called_once_with(("localhost", 1234), None, None)

    with pytest.raises(ValueError):
        await pooler.create_private_pool(Address("localhost", 1234), PrivatePoolDescription())


async def test_close__empty_pooler():
    pooler = Pooler(asynctest.CoroutineMock())
    await pooler.close()

    assert pooler.closed is True


async def test_close__with_pools(mocker):
    addrs_pools = [
        (Address("h1", 1), create_pool_mock()),
        (Address("h2", 2), create_pool_mock()),
        (Address("h3", 3), create_pool_mock()),
    ]
    addrs = [p[0] for p in addrs_pools]
    pools = [p[1] for p in addrs_pools]
    mocked_create_pool = asynctest.CoroutineMock(side_effect=pools)

    pooler = Pooler(mocked_create_pool)

    result1 = await pooler.ensure_pool(addrs[0])
    result2 = await pooler.ensure_pool(addrs[1])
    result3 = await pooler.create_private_pool(addrs[2], PrivatePoolDescription())

    assert len(pooler._public_pools) == 2
    assert len(pooler._private_pools) == 1

    await pooler.close()

    assert len(pooler._public_pools) == 0
    assert len(pooler._private_pools) == 0

    assert pooler.closed is True
    result1.close.assert_called_once()
    result2.close.assert_called_once()
    result3.close.assert_called_once()
    result1.wait_closed.assert_called_once()
    result2.wait_closed.assert_called_once()
    result3.wait_closed.assert_called_once()


async def test_reap_pools(mocker):
    addrs_pools = [
        (Address("h1", 1), create_pool_mock()),
        (Address("h2", 2), create_pool_mock()),
    ]
    addrs = [p[0] for p in addrs_pools]
    pools = [p[1] for p in addrs_pools]
    mocked_create_pool = asynctest.CoroutineMock(side_effect=pools)

    pooler = Pooler(mocked_create_pool, reap_frequency=-1)

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
    assert len(pooler._public_pools) == 1

    reaped = await pooler._reap_pools()
    assert len(reaped) == 1
    assert reaped[0] is pools[1]
    assert len(pooler._public_pools) == 0


async def test_reaper(mocker):
    pooler = Pooler(asynctest.CoroutineMock(), reap_frequency=0)

    assert pooler._reap_calls == 0

    # force two event loop cycle
    # 1 - creation reaper task
    # 2 - reaper one cycle
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert pooler._reap_calls == 1

    await pooler.close()

    assert pooler._reaper_task.cancelled() is True


async def test_add_pubsub_channel__no_addr():
    pooler = Pooler(asynctest.CoroutineMock(), reap_frequency=-1)

    addr = Address("h1", 1234)
    result = pooler.add_pubsub_channel(addr, b"channel", False)

    assert result is False


async def test_add_pubsub_channel():
    pooler = Pooler(asynctest.CoroutineMock(return_value=create_pool_mock()), reap_frequency=-1)

    addr1 = Address("h1", 1234)
    addr2 = Address("h2", 1234)
    pooler._pubsub_addrs[addr1] = set()
    pooler._pubsub_addrs[addr2] = set()

    result1 = pooler.add_pubsub_channel(addr1, b"ch1", False)
    result2 = pooler.add_pubsub_channel(addr1, b"ch1", True)
    result3 = pooler.add_pubsub_channel(addr1, b"ch2", False)
    result4 = pooler.add_pubsub_channel(addr2, b"ch3", False)
    result5 = pooler.add_pubsub_channel(addr1, b"ch3", False)

    assert result1 is True
    assert result2 is True
    assert result3 is True
    assert result4 is True
    assert result5 is False
    assert len(pooler._pubsub_addrs[addr1]) == 3
    assert len(pooler._pubsub_addrs[addr2]) == 1
    assert len(pooler._pubsub_channels) == 4

    collected_channels = [(ch.name, ch.is_pattern) for ch in pooler._pubsub_channels]
    assert (b"ch1", False) in collected_channels
    assert (b"ch1", True) in collected_channels
    assert (b"ch2", False) in collected_channels
    assert (b"ch3", False) in collected_channels


async def test_remove_pubsub_channel__no_addr():
    pooler = Pooler(asynctest.CoroutineMock(), reap_frequency=-1)

    result = pooler.remove_pubsub_channel(b"channel", False)
    assert result is False


async def test_remove_pubsub_channel():
    pooler = Pooler(asynctest.CoroutineMock(), reap_frequency=-1)

    addr1 = Address("h1", 1234)
    addr2 = Address("h2", 1234)
    pooler._pubsub_addrs[addr1] = set()
    pooler._pubsub_addrs[addr2] = set()

    pooler.add_pubsub_channel(addr1, b"ch1", False)
    pooler.add_pubsub_channel(addr1, b"ch2", False)
    pooler.add_pubsub_channel(addr2, b"ch3", True)

    result1 = pooler.remove_pubsub_channel(b"ch1", False)
    result2 = pooler.remove_pubsub_channel(b"ch1", True)
    result3 = pooler.remove_pubsub_channel(b"ch2", False)
    result4 = pooler.remove_pubsub_channel(b"ch3", True)
    result5 = pooler.remove_pubsub_channel(b"ch3", True)

    assert result1 is True
    assert result2 is False
    assert result3 is True
    assert result4 is True
    assert result5 is False
    assert len(pooler._pubsub_addrs[addr1]) == 0
    assert len(pooler._pubsub_addrs[addr2]) == 0
    assert len(pooler._pubsub_channels) == 0


async def test_get_pubsub_addr():
    pooler = Pooler(asynctest.CoroutineMock(), reap_frequency=-1)

    addr1 = Address("h1", 1234)
    addr2 = Address("h2", 1234)
    pooler._pubsub_addrs[addr1] = set()
    pooler._pubsub_addrs[addr2] = set()

    pooler.add_pubsub_channel(addr1, b"ch1", False)
    pooler.add_pubsub_channel(addr2, b"ch2", True)

    result1 = pooler.get_pubsub_addr(b"ch1", False)
    result2 = pooler.get_pubsub_addr(b"ch1", True)
    result3 = pooler.get_pubsub_addr(b"ch2", False)
    result4 = pooler.get_pubsub_addr(b"ch2", True)

    assert result1 == addr1
    assert result2 is None
    assert result3 is None
    assert result4 == addr2


async def test_ensure_pool__create_pubsub_addr_set(mocker):
    addr1 = Address("h1", 1234)
    addr2 = Address("h2", 1234)

    pooler = Pooler(asynctest.CoroutineMock(return_value=create_pool_mock()))

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


async def test_reap_pools__cleanup_channels(mocker):
    pooler = Pooler(asynctest.CoroutineMock(), reap_frequency=-1)

    addr1 = Address("h1", 1)
    addr2 = Address("h2", 2)

    # create pools
    await pooler.ensure_pool(addr1)
    await pooler.ensure_pool(addr2)

    pooler.add_pubsub_channel(addr1, b"ch1", False)
    pooler.add_pubsub_channel(addr2, b"ch2", False)

    # try to reap pools
    reaped = await pooler._reap_pools()

    assert len(reaped) == 0

    reaped = await pooler._reap_pools()

    assert len(reaped) == 2
    assert len(pooler._pubsub_addrs) == 0
    assert len(pooler._pubsub_channels) == 0


async def test_close_only():
    pool1 = create_pool_mock()
    pool2 = create_pool_mock()
    pool3 = create_pool_mock()
    mocked_create_pool = asynctest.CoroutineMock(side_effect=[pool1, pool2, pool3])
    pooler = Pooler(mocked_create_pool)
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
