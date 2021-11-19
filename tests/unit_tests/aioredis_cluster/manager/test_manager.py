import asyncio
import time

import mock
import pytest

from aioredis_cluster.manager import ClusterManager
from aioredis_cluster.structs import Address, ClusterNode


@pytest.fixture
def pooler_mock():
    def factory():
        mocked_pool = mock.NonCallableMock()
        mocked_pool.execute = mock.AsyncMock()

        mocked = mock.NonCallableMock()
        mocked.ensure_pool = mock.AsyncMock(return_value=mocked_pool)
        return mocked

    return factory


async def test_require_reload_state(pooler_mock):
    pooler = pooler_mock()
    manager = ClusterManager(["addr1", "addr2"], pooler)

    assert manager._reload_event.is_set() is False

    manager.require_reload_state()

    assert manager._reload_event.is_set() is True

    await manager.close()


async def test_state_reloader(mocker, pooler_mock, event_loop):
    pooler = pooler_mock()
    manager = ClusterManager(["addr1", "addr2"], pooler)

    load_state_fut = event_loop.create_future()
    mocked_load_state = mocker.patch.object(
        manager,
        "_load_state",
        new=mock.AsyncMock(side_effect=lambda reload_id: load_state_fut.set_result(None)),
    )

    manager.require_reload_state()

    await asyncio.wait_for(load_state_fut, 10)

    mocked_load_state.assert_called_once()

    await manager.close()


async def test_get_state__no_state_load_state(mocker, pooler_mock):
    pooler = pooler_mock()
    manager = ClusterManager(["addr1", "addr2"], pooler)

    mocked_load_state = mocker.patch.object(manager, "_load_state", new=mock.AsyncMock())

    state = await manager.get_state()

    assert state is mocked_load_state.return_value

    await asyncio.sleep(0)
    await manager.close()


async def test_get_state__state_exists(mocker, pooler_mock):
    pooler = pooler_mock()
    manager = ClusterManager(["addr1", "addr2"], pooler)

    state = mock.NonCallableMock()
    state._data.created_at = time.time()
    manager._state = state

    mocked_require_reload_state = mocker.patch.object(manager, "require_reload_state")

    result = await manager.get_state()

    assert result is state
    mocked_require_reload_state.assert_not_called()

    await asyncio.sleep(0)
    await manager.close()


async def test_get_state__state_exists_and_require_reload_state(mocker, pooler_mock):
    pooler = pooler_mock()
    manager = ClusterManager(["addr1", "addr2"], pooler)

    state = mock.NonCallableMock()
    state.created_at = 0
    manager._state = state

    mocked_require_reload_state = mocker.patch.object(manager, "require_reload_state")

    result = await manager.get_state()

    assert result is state
    mocked_require_reload_state.assert_not_called()

    await asyncio.sleep(0)
    await manager.close()


async def test_load_state(mocker, pooler_mock):
    pooler = pooler_mock()

    mocker.patch(ClusterManager.__module__ + ".random.sample", return_value=["addr1", "addr2"])

    manager = ClusterManager(["addr1", "addr2"], pooler)

    mocked_load_slots = mocker.patch.object(manager, "_load_slots", new=mock.AsyncMock())
    mocked_create_cluster_state = mocker.patch(manager.__module__ + ".create_cluster_state")
    mocked_state = mocked_create_cluster_state.return_value
    mocked_state._data.masters = [
        ClusterNode(Address("master1", 6666), "master1_id"),
        ClusterNode(Address("master2", 7777), "master2_id"),
    ]
    mocked_state._data.addrs = [object(), object()]
    mocked_state.random_master.return_value = ClusterNode(
        Address("random_master", 5555), "random_master_id"
    )

    mocked_create_registry = mocker.patch(manager.__module__ + ".create_registry")

    result = await manager._load_state(1)

    assert result is mocked_state
    assert result is manager._state
    assert manager._commands is mocked_create_registry.return_value
    mocked_load_slots.assert_called_once_with(["addr1", "addr2"])
    mocked_create_registry.assert_called_once_with(
        pooler.ensure_pool.return_value.execute.return_value
    )
    assert pooler.ensure_pool.call_count == 3
    assert pooler.ensure_pool.call_args_list[0] == mock.call(Address("master1", 6666))
    assert pooler.ensure_pool.call_args_list[1] == mock.call(Address("master2", 7777))
    assert pooler.ensure_pool.call_args_list[2] == mock.call(Address("random_master", 5555))

    await asyncio.sleep(0)
    await manager.close()


async def test_init(mocker, pooler_mock):
    pooler = pooler_mock()
    manager = ClusterManager(["addr1", "addr2"], pooler)

    mocked_load_state = mocker.patch.object(manager, "_load_state", new=mock.AsyncMock())

    await manager._init()

    mocked_load_state.assert_called_once()

    await asyncio.sleep(0)
    await manager.close()


async def test_load_slots__first_response(pooler_mock):
    pooler = pooler_mock()
    pool = pooler.ensure_pool.return_value
    manager = ClusterManager(["addr1"], pooler)

    result = await manager._load_slots(["addr2", "addr3"])

    assert result is pool.execute.return_value
    pooler.ensure_pool.assert_called_once_with("addr2")
    pool.execute.assert_called_once_with(b"CLUSTER", b"SLOTS", encoding="utf-8")


async def test_load_slots__with_error_but_success(pooler_mock):
    pooler = pooler_mock()
    pool = pooler.ensure_pool.return_value

    def ensure_pool_se(addr):
        if addr == "addr1":
            raise RuntimeError("ensure_pool")
        return pool

    pooler.ensure_pool.side_effect = ensure_pool_se

    def pool_execute_se(*args, **kwargs):
        if pool.execute.call_count == 1:
            raise RuntimeError("execute")
        return pool.execute.return_value

    pool.execute.side_effect = pool_execute_se

    manager = ClusterManager(["addr1"], pooler)

    result = await manager._load_slots(["addr1", "addr2", "addr3"])

    assert result is pool.execute.return_value
    assert pooler.ensure_pool.call_count == 3
    assert pool.execute.call_count == 2
    pooler.ensure_pool.assert_has_calls(
        [
            mock.call("addr1"),
            mock.call("addr2"),
            mock.call("addr3"),
        ]
    )
    pool.execute.assert_has_calls(
        [
            mock.call(b"CLUSTER", b"SLOTS", encoding="utf-8"),
            mock.call(b"CLUSTER", b"SLOTS", encoding="utf-8"),
        ]
    )


async def test_load_slots__with_error(pooler_mock):
    pooler = pooler_mock()

    def ensure_pool_se(addr):
        raise RuntimeError(addr)

    pooler.ensure_pool.side_effect = ensure_pool_se

    manager = ClusterManager(["addr1"], pooler)

    with pytest.raises(RuntimeError, match="addr1"):
        await manager._load_slots(["addr1", "addr2", "addr3"])

    assert pooler.ensure_pool.call_count == 3
    pooler.ensure_pool.assert_has_calls(
        [
            mock.call("addr1"),
            mock.call("addr2"),
            mock.call("addr3"),
        ]
    )

    await asyncio.sleep(0)
    await manager.close()


@pytest.mark.parametrize(
    "follow_cluster, reload_id, state_addrs, expect",
    [
        (False, 0, None, ["startup1", "startup2"]),
        (False, 1, None, ["startup1", "startup2"]),
        (False, 2, ["addr1"], ["startup1", "startup2"]),
        (False, 200, ["addr1"], ["startup1", "startup2"]),
        (True, 0, None, ["startup1", "startup2"]),
        (True, 1, None, ["startup1", "startup2"]),
        (True, 2, None, ["startup1", "startup2"]),
        (True, 200, None, ["startup1", "startup2"]),
        (True, 0, ["addr1", "addr2"], ["startup1", "startup2"]),
        (True, 1, ["addr1", "addr2"], ["startup1", "startup2"]),
        (True, 2, ["addr1", "addr2"], ["addr1", "addr2"]),
        (True, 200, ["addr1", "addr2"], ["addr1", "addr2"]),
    ],
)
async def test_get_init_addrs__cluster_following(
    mocker, pooler_mock, follow_cluster, reload_id, state_addrs, expect
):
    mocker.patch(ClusterManager.__module__ + ".random.shuffle", side_effect=lambda addrs: addrs)

    pooler = pooler_mock()
    manager = ClusterManager(["startup1", "startup2"], pooler)
    manager._follow_cluster = follow_cluster
    if state_addrs is not None:
        manager._state = mock.NonCallableMock()
        manager._state._data.addrs = state_addrs

    result = manager._get_init_addrs(reload_id)

    assert result == expect


async def test_reload_state(mocker, pooler_mock):
    pooler = pooler_mock()
    manager = ClusterManager(["addr1"], pooler)

    state = object()
    mocked_load_state = mocker.patch.object(
        manager, "_load_state", new=mock.AsyncMock(return_value=state)
    )

    result = await manager.reload_state()

    assert result is state
    mocked_load_state.assert_called_once_with(1)

    result = await manager.reload_state()
    assert result is state
    assert mocked_load_state.call_count == 2
    assert mocked_load_state.call_args_list[1] == mock.call(2)

    await asyncio.sleep(0)
    await manager.close()
