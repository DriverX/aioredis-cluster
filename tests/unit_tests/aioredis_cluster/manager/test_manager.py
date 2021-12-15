import asyncio
import time

import mock
import pytest

from aioredis_cluster.cluster_state import ClusterState, NodeClusterState
from aioredis_cluster.manager import ClusterManager
from aioredis_cluster.structs import Address, ClusterNode

from ._cluster_slots import SLOTS


@pytest.fixture
def pooler_mock():
    def factory():
        conn = mock.NonCallableMock()
        conn.execute = mock.AsyncMock(
            return_value=object(),
        )
        pool = mock.NonCallableMock()
        pool.execute = mock.AsyncMock(
            return_value=object(),
        )

        acquirer_mock = mock.NonCallableMock()
        acquirer_mock.__aenter__ = mock.AsyncMock(return_value=conn)
        acquirer_mock.__aexit__ = mock.AsyncMock(return_value=None)
        pool.get.return_value = acquirer_mock

        mocked = mock.NonCallableMock()
        mocked.ensure_pool = mock.AsyncMock(return_value=pool)

        mocked._pool = pool
        mocked._conn = conn

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

    mocked_state = mock.NonCallableMock()
    mocked_state._data.masters = [
        ClusterNode(Address("master1", 6666), "master1_id"),
        ClusterNode(Address("master2", 7777), "master2_id"),
    ]
    mocked_state._data.addrs = [object(), object()]
    mocked_state.random_master.return_value = ClusterNode(
        Address("random_master", 5555), "random_master_id"
    )
    mocked_fetch_state = mocker.patch.object(
        manager,
        "_fetch_state",
        new=mock.AsyncMock(
            return_value=mocked_state,
        ),
    )

    mocked_create_registry = mocker.patch(manager.__module__ + ".create_registry")

    result = await manager._load_state(1)

    assert result is mocked_state
    assert result is manager._state
    assert manager._commands is mocked_create_registry.return_value
    mocked_fetch_state.assert_called_once_with(["addr1", "addr2"])
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


async def test_fetch_state__first_response(pooler_mock, mocker):
    pooler = pooler_mock()
    pool = pooler._pool
    conn = pooler._conn
    cluster_slots_resp = object()
    conn.execute.side_effect = [
        "cluster_state:ok",
        cluster_slots_resp,
    ]
    manager = ClusterManager(["addr1"], pooler)
    mocked_create_cluster_state = mocker.patch(ClusterManager.__module__ + ".create_cluster_state")

    result = await manager._fetch_state(["addr2", "addr3"])

    assert result is mocked_create_cluster_state.return_value
    pooler.ensure_pool.assert_called_once_with("addr2")
    pool.get.asssert_called_once()
    assert conn.execute.await_count == 2
    execute_calls = conn.execute.call_args_list
    assert execute_calls[0] == mock.call(b"CLUSTER", b"INFO", encoding="utf-8")
    assert execute_calls[1] == mock.call(b"CLUSTER", b"SLOTS", encoding="utf-8")


async def test_fetch_state__with_fail_state(pooler_mock, mocker):
    pooler = pooler_mock()
    conn = pooler._conn
    conn.execute.side_effect = [
        "cluster_state:fail",
        SLOTS,
    ]
    manager = ClusterManager(["addr1"], pooler)
    result = await manager._fetch_state(["addr2"])

    assert result.state is NodeClusterState.FAIL


async def test_fetch_state__with_error_but_success(pooler_mock):
    pooler = pooler_mock()
    conn = pooler._conn
    pool = pooler._pool

    def ensure_pool_se(addr):
        if addr == "addr1":
            raise RuntimeError("ensure_pool")
        return pool

    pooler.ensure_pool.side_effect = ensure_pool_se

    conn.execute.side_effect = [
        # first try
        "cluster_state:fail",
        object(),
        # second try
        "cluster_state:ok",
        RuntimeError("execute fail"),
        # success third try
        "cluster_state:ok",
        SLOTS,
    ]

    manager = ClusterManager(["addr1"], pooler)

    result = await manager._fetch_state(["addr1", "addr2", "addr3", "addr4"])

    assert isinstance(result, ClusterState)
    assert result.state is NodeClusterState.OK
    assert result.state_from == "addr4"
    assert pooler.ensure_pool.await_count == 4
    assert conn.execute.await_count == 6
    pooler.ensure_pool.assert_has_calls(
        [
            mock.call("addr1"),
            mock.call("addr2"),
            mock.call("addr3"),
            mock.call("addr4"),
        ]
    )
    conn.execute.assert_has_calls(
        [
            mock.call(b"CLUSTER", b"INFO", encoding="utf-8"),
            mock.call(b"CLUSTER", b"SLOTS", encoding="utf-8"),
        ]
    )


async def test_fetch_state__with_error(pooler_mock):
    pooler = pooler_mock()
    pool = pooler._pool
    conn = pooler._conn

    pooler.ensure_pool.side_effect = [
        RuntimeError("first error"),
        pool,
        pool,
        pool,
    ]

    pool.get.return_value.__aenter__.side_effect = [
        RuntimeError("second error"),
        conn,
        conn,
    ]

    execute_resp = mock.NonCallableMagicMock()
    conn.execute.side_effect = [RuntimeError("third error"), execute_resp]

    manager = ClusterManager(["addr1"], pooler)

    with pytest.raises(RuntimeError, match="third error"):
        await manager._fetch_state(["addr1", "addr2", "addr3"])

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
