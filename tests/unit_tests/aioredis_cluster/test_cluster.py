import asyncio

import mock
import pytest

from aioredis_cluster.aioredis import (
    ConnectionClosedError,
    ConnectionForcedCloseError,
    PoolClosedError,
    ProtocolError,
)
from aioredis_cluster.cluster import Cluster
from aioredis_cluster.command_info import default_registry
from aioredis_cluster.commands import RedisCluster
from aioredis_cluster.errors import (
    AskError,
    ClusterDownError,
    ConnectTimeoutError,
    LoadingError,
    MovedError,
    TryAgainError,
    UncoveredSlotError,
)
from aioredis_cluster.pool import ConnectionsPool
from aioredis_cluster.structs import Address


def create_async_mock(**kwargs):
    if "return_value" not in kwargs:
        kwargs["return_value"] = mock.Mock()
    return mock.AsyncMock(**kwargs)


def get_pooler_mock():
    mocked = mock.NonCallableMock()
    mocked.ensure_pool = create_async_mock()
    mocked.close_only = create_async_mock()

    conn = mock.NonCallableMock()
    conn.execute = create_async_mock()

    pool = mocked.ensure_pool.return_value
    pool.execute = create_async_mock()
    pool.auth = create_async_mock()
    pool.acquire = create_async_mock(return_value=conn)
    pool.maxsize = 10
    pool.minsize = 1
    pool.size = 1
    pool.freesize = 0

    conn_acquirer = mock.MagicMock()
    conn_acquirer.__aenter__ = create_async_mock(return_value=conn)
    conn_acquirer.__aexit__ = create_async_mock(return_value=None)

    pool.get.return_value = conn_acquirer

    mocked._pool = pool
    mocked._conn = conn

    return mocked


def get_manager_mock():
    mocked = mock.NonCallableMock()
    mocked.get_state = create_async_mock(return_value=mock.NonCallableMock())
    mocked.commands = default_registry

    return mocked


def test_init__no_startup_nodes():
    with pytest.raises(ValueError, match="startup_nodes must be one at least"):
        Cluster([])


async def test_init__defaults(mocker):
    mocked_pooler = mocker.patch(Cluster.__module__ + ".Pooler")
    mocked_manager = mocker.patch(Cluster.__module__ + ".ClusterManager")

    cl = Cluster(["addr1", "addr2"])

    assert cl._retry_min_delay == cl.RETRY_MIN_DELAY
    assert cl._retry_max_delay == cl.RETRY_MAX_DELAY
    assert cl._max_attempts == cl.MAX_ATTEMPTS
    assert cl._password is None
    assert cl._encoding is None
    assert cl._pool_minsize == cl.POOL_MINSIZE
    assert cl._pool_maxsize == cl.POOL_MAXSIZE
    assert cl._commands_factory is RedisCluster
    assert cl._connect_timeout == cl.CONNECT_TIMEOUT
    assert cl._pool_cls is ConnectionsPool
    assert cl._pooler is mocked_pooler.return_value
    assert cl._manager is mocked_manager.return_value
    assert cl._attempt_timeout == cl.ATTEMPT_TIMEOUT
    mocked_pooler.assert_called_once_with(cl._create_default_pool, reap_frequency=None)
    mocked_manager.assert_called_once_with(
        ["addr1", "addr2"],
        cl._pooler,
        state_reload_interval=None,
        follow_cluster=None,
        execute_timeout=5.0,
    )


async def test_init__customized(mocker):
    mocked_pooler = mocker.patch(Cluster.__module__ + ".Pooler")
    mocked_manager = mocker.patch(Cluster.__module__ + ".ClusterManager")

    kwargs = {
        "retry_min_delay": 1.2,
        "retry_max_delay": 3.4,
        "max_attempts": 5,
        "idle_connection_timeout": 10.0,
        "password": "PASSWORD",
        "encoding": "cp1251",
        "pool_minsize": 3,
        "pool_maxsize": 7,
        "commands_factory": object(),
        "connect_timeout": 9.9,
        "pool_cls": mock.Mock(),
        "state_reload_interval": 2.2,
        "follow_cluster": True,
        "attempt_timeout": 7.8,
    }

    cl = Cluster(["addr1", "addr2"], **kwargs)

    assert cl._retry_min_delay == kwargs["retry_min_delay"]
    assert cl._retry_max_delay == kwargs["retry_max_delay"]
    assert cl._max_attempts == kwargs["max_attempts"]
    assert cl._password == kwargs["password"]
    assert cl._encoding == kwargs["encoding"]
    assert cl._pool_minsize == kwargs["pool_minsize"]
    assert cl._pool_maxsize == kwargs["pool_maxsize"]
    assert cl._connect_timeout == kwargs["connect_timeout"]
    assert cl._commands_factory is kwargs["commands_factory"]
    assert cl._pool_cls is kwargs["pool_cls"]
    assert cl._pooler is mocked_pooler.return_value
    assert cl._manager is mocked_manager.return_value
    assert cl._attempt_timeout == kwargs["attempt_timeout"]
    mocked_pooler.assert_called_once_with(
        cl._create_default_pool, reap_frequency=kwargs["idle_connection_timeout"]
    )
    mocked_manager.assert_called_once_with(
        ["addr1", "addr2"],
        cl._pooler,
        state_reload_interval=kwargs["state_reload_interval"],
        follow_cluster=kwargs["follow_cluster"],
        execute_timeout=kwargs["attempt_timeout"],
    )


async def test_create_pool(mocker):
    mocked_create_pool = mocker.patch(Cluster.__module__ + ".create_pool", new=create_async_mock())

    pool_cls = mock.Mock()
    cl = Cluster(
        ["addr1"],
        password="foobar",
        encoding="utf-8",
        pool_minsize=5,
        pool_maxsize=15,
        connect_timeout=1.2,
        pool_cls=pool_cls,
    )

    addr = "addr2"
    pool = await cl._create_pool(addr)

    assert pool is mocked_create_pool.return_value
    mocked_create_pool.assert_called_once_with(
        "addr2",
        pool_cls=pool_cls,
        password="foobar",
        encoding="utf-8",
        minsize=5,
        maxsize=15,
        create_connection_timeout=1.2,
    )


async def test_create_pool_by_addr(mocker):
    mocked_create_pool = mocker.patch(Cluster.__module__ + ".create_pool", new=create_async_mock())

    pool_cls = mock.Mock()
    cl = Cluster(
        ["addr1"],
        password="foobar",
        encoding="utf-8",
        pool_minsize=5,
        pool_maxsize=15,
        connect_timeout=1.2,
        pool_cls=pool_cls,
    )
    mocked_manager = mocker.patch.object(cl, "_manager", new=get_manager_mock())
    state = mocked_manager.get_state.return_value
    state._data.masters = [
        mock.NonCallableMock(name="master1", addr=("addr1", 8001)),
        mock.NonCallableMock(name="master2", addr=("addr2", 8002)),
        mock.NonCallableMock(name="master3", addr=("addr3", 8003)),
    ]

    addr = Address("addr2", 8002)
    redis = await cl.create_pool_by_addr(addr, maxsize=42)

    assert redis.connection is mocked_create_pool.return_value
    mocked_create_pool.assert_called_once_with(
        ("addr2", 8002),
        pool_cls=pool_cls,
        password="foobar",
        encoding="utf-8",
        minsize=5,
        maxsize=42,
        create_connection_timeout=1.2,
    )


async def test_create_pool_by_addr__bad_addr(mocker):
    pool_cls = mock.Mock()
    cl = Cluster(
        ["addr1"],
        password="foobar",
        encoding="utf-8",
        pool_minsize=5,
        pool_maxsize=15,
        connect_timeout=1.2,
        pool_cls=pool_cls,
    )
    mocked_manager = mocker.patch.object(cl, "_manager", new=get_manager_mock())
    state = mocked_manager.get_state.return_value
    state.has_addr.return_value = False

    addr = Address("addr4", 8004)
    with pytest.raises(ValueError):
        await cl.create_pool_by_addr(addr, maxsize=42)

    state.has_addr.assert_called_once_with(addr)


async def test_auth(mocker):
    cl = Cluster(["addr1"])

    mocked_pooler = mocker.patch.object(cl, "_pooler", new=get_pooler_mock())
    pool_mock = mocked_pooler.ensure_pool.return_value

    async def batch_op_se(fn):
        for p in [pool_mock, pool_mock]:
            await fn(p)

    mocked_pooler.batch_op = create_async_mock(side_effect=batch_op_se)

    await cl.auth("PASSWORD")

    assert cl._password == "PASSWORD"
    mocked_pooler.batch_op.assert_called_once()
    pool_mock.auth.assert_has_calls(
        [
            mock.call("PASSWORD"),
            mock.call("PASSWORD"),
        ]
    )


async def test_execute__success_random_master(mocker):
    cl = Cluster(["addr1"])

    mocked_pooler = mocker.patch.object(cl, "_pooler", new=get_pooler_mock())
    pool = mocked_pooler._pool
    mocked_manager = mocker.patch.object(cl, "_manager", new=get_manager_mock())
    state = mocked_manager.get_state.return_value

    result = await cl.execute("INFO")

    assert mocked_manager.get_state.call_count == 1
    assert mocked_pooler.ensure_pool.call_count == 1
    assert mocked_manager.require_reload_state.call_count == 0
    assert result is pool.execute.return_value
    mocked_pooler.ensure_pool.assert_called_with(state.random_master.return_value.addr)
    state.random_master.assert_called_once_with()


async def test_execute__success_slot_master(mocker):
    cl = Cluster(["addr1"])

    mocked_pooler = mocker.patch.object(cl, "_pooler", new=get_pooler_mock())
    pool = mocked_pooler._pool
    mocked_manager = mocker.patch.object(cl, "_manager", new=get_manager_mock())
    state = mocked_manager.get_state.return_value

    result = await cl.execute("mset", "key", "value")

    assert mocked_manager.get_state.call_count == 1
    assert mocked_pooler.ensure_pool.call_count == 1
    assert mocked_manager.require_reload_state.call_count == 0
    assert result is pool.execute.return_value
    mocked_pooler.ensure_pool.assert_called_with(state.slot_master.return_value.addr)
    # cl.determine_slot(b'key') == 12539
    state.slot_master.assert_called_once_with(12539)


async def test_execute__success_but_slot_oncovered(mocker):
    cl = Cluster(["addr1"])

    mocked_pooler = mocker.patch.object(cl, "_pooler", new=get_pooler_mock())
    pool = mocked_pooler._pool
    mocked_manager = mocker.patch.object(cl, "_manager", new=get_manager_mock())
    state = mocked_manager.get_state.return_value
    state.slot_master.side_effect = UncoveredSlotError(0)

    result = await cl.execute("set", "key", "value")

    assert mocked_manager.get_state.call_count == 1
    assert mocked_pooler.ensure_pool.call_count == 1
    assert mocked_manager.require_reload_state.call_count == 1
    assert result is pool.execute.return_value
    mocked_pooler.ensure_pool.assert_called_with(state.random_master.return_value.addr)
    # cl.determine_slot(b'key') == 12539
    state.slot_master.assert_called_once_with(12539)
    state.random_master.assert_called_once_with()


async def test_execute__success_one_connection_retry(mocker):
    cl = Cluster(["addr1"])

    mocked_pooler = mocker.patch.object(cl, "_pooler", new=get_pooler_mock())
    pool = mocked_pooler._pool

    pool_execute_count = 0

    def pool_execute_se(*args, **kwargs):
        nonlocal pool_execute_count
        pool_execute_count += 1
        if pool_execute_count == 1:
            raise ConnectionRefusedError

        return pool.execute.return_value

    pool.execute.side_effect = pool_execute_se

    mocked_manager = mocker.patch.object(cl, "_manager", new=get_manager_mock())
    state = mocked_manager.get_state.return_value

    mocked_execute_retry_slowdown = mocker.patch.object(
        cl, "_execute_retry_slowdown", new=create_async_mock()
    )

    result = await cl.execute("get", "key")

    assert mocked_manager.get_state.call_count == 2
    assert mocked_pooler.ensure_pool.call_count == 2
    assert mocked_manager.require_reload_state.call_count == 1
    assert result is pool.execute.return_value
    assert mocked_pooler.ensure_pool.call_args_list[0] == mock.call(
        state.slot_master.return_value.addr
    )
    assert mocked_pooler.ensure_pool.call_args_list[1] == mock.call(
        state.random_slot_replica.return_value.addr
    )
    # cl.determine_slot(b'key') == 12539
    state.slot_master.assert_called_once_with(12539)
    mocked_execute_retry_slowdown.assert_called_once_with(1, 10)


async def test_execute__success_several_problem_retry(mocker):
    cl = Cluster(["addr1"])

    mocked_pooler = mocker.patch.object(cl, "_pooler", new=get_pooler_mock())
    conn = mocked_pooler._conn
    pool = mocked_pooler._pool

    pool_execute_count = 0

    errors = [
        ConnectionRefusedError(),
        ConnectionError(),
        ConnectTimeoutError(("addr1", 6379)),
        AskError("ASK 12539 1.2.3.4:9999"),
        ClusterDownError("CLUSTERDOWN cluster is down"),
        TryAgainError("TRYAGAIN try again later"),
        MovedError("MOVED 12539 4.3.2.1:6666"),
        ProtocolError(),
        MovedError("MOVED 12539 4.3.2.1:6666"),
    ]

    def conn_execute_se(*args, **kwargs):
        nonlocal pool_execute_count

        if args[0] == b"ASKING":
            return conn.execute.return_value

        pool_execute_count += 1
        raise errors[pool_execute_count - 1]

    def pool_execute_se(*args, **kwargs):
        nonlocal pool_execute_count

        pool_execute_count += 1
        if pool_execute_count <= len(errors):
            err = errors[pool_execute_count - 1]
            raise err

        return ("result", pool_execute_count)

    pool.execute.side_effect = pool_execute_se
    conn.execute.side_effect = conn_execute_se

    mocked_manager = mocker.patch.object(cl, "_manager", new=get_manager_mock())
    state = mocked_manager.get_state.return_value

    mocked_execute_retry_slowdown = mocker.patch.object(
        cl, "_execute_retry_slowdown", new=create_async_mock()
    )

    result = await cl.execute("get", "key")

    assert mocked_manager.get_state.call_count == 10
    assert mocked_pooler.ensure_pool.call_count == 10
    assert mocked_manager.require_reload_state.call_count == 8
    assert result == ("result", 10)

    ensure_pool_calls = mocked_pooler.ensure_pool.call_args_list

    # #1 attempt
    assert ensure_pool_calls[0] == mock.call(state.slot_master.return_value.addr)
    # #2 attempt to random replica node
    assert ensure_pool_calls[1] == mock.call(state.random_slot_replica.return_value.addr)
    # #3 attempt try to execute on random node in cluster
    assert ensure_pool_calls[2] == mock.call(state.random_node.return_value.addr)
    # #4 attempt try random node in cluster
    assert ensure_pool_calls[3] == mock.call(state.random_node.return_value.addr)
    # #5 attempt as ASKING to node from MovedError
    assert ensure_pool_calls[4] == mock.call(Address("1.2.3.4", 9999))
    # #6 attempt again random node because CLUSTERDOWN
    assert ensure_pool_calls[5] == mock.call(state.random_node.return_value.addr)
    # #7 attempt again random node because TRYAGAIN
    assert ensure_pool_calls[6] == mock.call(state.random_node.return_value.addr)
    # #8 attempt to node from MovedError
    assert ensure_pool_calls[7] == mock.call(Address("4.3.2.1", 6666))
    # #9 attempt again random node because ProtocolError
    assert ensure_pool_calls[8] == mock.call(state.random_node.return_value.addr)
    # #10 attempt to node from MovedError, finally
    assert ensure_pool_calls[9] == mock.call(Address("4.3.2.1", 6666))
    # cl.determine_slot(b'key') == 12539
    state.slot_master.assert_called_once_with(12539)
    assert state.random_slot_replica.call_count == 1
    assert state.random_slot_replica.call_args == mock.call(12539)
    assert state.random_node.call_count == 5
    assert mocked_execute_retry_slowdown.call_count == 9

    asking_calls = [c for c in conn.execute.call_args_list if c == mock.call(b"ASKING")]
    assert len(asking_calls) == 1


async def test_execute__cancelled_error(mocker):
    cl = Cluster(["addr1"])

    mocked_pooler = mocker.patch.object(cl, "_pooler", new=get_pooler_mock())
    mocked_pooler.ensure_pool.side_effect = asyncio.CancelledError()

    mocked_manager = mocker.patch.object(cl, "_manager", new=get_manager_mock())

    mocked_execute_retry_slowdown = mocker.patch.object(
        cl, "_execute_retry_slowdown", new=create_async_mock()
    )

    with pytest.raises(asyncio.CancelledError):
        await cl.execute("get", "key")

    assert mocked_manager.get_state.call_count == 1
    assert mocked_pooler.ensure_pool.call_count == 1
    assert mocked_manager.require_reload_state.call_count == 0
    mocked_execute_retry_slowdown.assert_not_called()


async def test_execute__error_after_max_attempts(mocker):
    cl = Cluster(["addr1"])

    mocked_pooler = mocker.patch.object(cl, "_pooler", new=get_pooler_mock())
    pool = mocked_pooler._pool

    pool_execute_count = 0

    errors = [
        MovedError("MOVED 12539 1.2.3.4:1000"),
        MovedError("MOVED 12539 1.2.3.4:1001"),
        MovedError("MOVED 12539 1.2.3.4:1002"),
        MovedError("MOVED 12539 1.2.3.4:1003"),
        MovedError("MOVED 12539 1.2.3.4:1004"),
        MovedError("MOVED 12539 1.2.3.4:1005"),
        MovedError("MOVED 12539 1.2.3.4:1006"),
        MovedError("MOVED 12539 1.2.3.4:1007"),
        MovedError("MOVED 12539 1.2.3.4:1008"),
        MovedError("MOVED 12539 1.2.3.4:1009"),
        MovedError("MOVED 12539 1.2.3.4:1010"),
        MovedError("MOVED 12539 1.2.3.4:1011"),
    ]

    def pool_execute_se(*args, **kwargs):
        nonlocal pool_execute_count

        pool_execute_count += 1
        raise errors[pool_execute_count - 1]

    pool.execute.side_effect = pool_execute_se

    mocked_manager = mocker.patch.object(cl, "_manager", new=get_manager_mock())
    state = mocked_manager.get_state.return_value

    mocked_execute_retry_slowdown = mocker.patch.object(
        cl, "_execute_retry_slowdown", new=create_async_mock()
    )

    with pytest.raises(MovedError) as ei:
        await cl.execute("time")

    assert (ei.value.info.host, ei.value.info.port) == ("1.2.3.4", 1009)
    assert mocked_manager.get_state.call_count == 10
    assert mocked_pooler.ensure_pool.call_count == 10
    assert mocked_manager.require_reload_state.call_count == 10

    ensure_pool_calls = mocked_pooler.ensure_pool.call_args_list

    assert ensure_pool_calls[0] == mock.call(state.random_master.return_value.addr)
    assert ensure_pool_calls[1] == mock.call(Address("1.2.3.4", 1000))
    assert ensure_pool_calls[2] == mock.call(Address("1.2.3.4", 1001))
    assert ensure_pool_calls[3] == mock.call(Address("1.2.3.4", 1002))
    assert ensure_pool_calls[4] == mock.call(Address("1.2.3.4", 1003))
    assert ensure_pool_calls[5] == mock.call(Address("1.2.3.4", 1004))
    assert ensure_pool_calls[6] == mock.call(Address("1.2.3.4", 1005))
    assert ensure_pool_calls[7] == mock.call(Address("1.2.3.4", 1006))
    assert ensure_pool_calls[8] == mock.call(Address("1.2.3.4", 1007))
    assert ensure_pool_calls[9] == mock.call(Address("1.2.3.4", 1008))
    assert state.random_node.call_count == 0
    assert mocked_execute_retry_slowdown.call_count == 9


async def test_execute__unexpected_error(mocker):
    cl = Cluster(["addr1"])

    mocked_pooler = mocker.patch.object(cl, "_pooler", new=get_pooler_mock())
    mocked_pooler.ensure_pool.side_effect = RuntimeError("Boom!")

    mocked_manager = mocker.patch.object(cl, "_manager", new=get_manager_mock())

    mocked_execute_retry_slowdown = mocker.patch.object(
        cl, "_execute_retry_slowdown", new=create_async_mock()
    )

    with pytest.raises(RuntimeError, match="Boom!"):
        await cl.execute("echo", "hello")

    assert mocked_manager.get_state.call_count == 1
    assert mocked_pooler.ensure_pool.call_count == 1
    assert mocked_manager.require_reload_state.call_count == 0
    mocked_execute_retry_slowdown.assert_not_called()


async def test_execute__retriable_error_in_unexpected_call(mocker):
    cl = Cluster(["addr1"])

    mocked_pooler = mocker.patch.object(cl, "_pooler", new=get_pooler_mock())

    mocked_manager = mocker.patch.object(cl, "_manager", new=get_manager_mock())
    mocked_manager.get_state.side_effect = ConnectionRefusedError()

    mocked_execute_retry_slowdown = mocker.patch.object(
        cl, "_execute_retry_slowdown", new=create_async_mock()
    )

    with pytest.raises(ConnectionRefusedError):
        await cl.execute("ping")

    assert mocked_manager.get_state.call_count == 1
    assert mocked_pooler.ensure_pool.call_count == 0
    assert mocked_manager.require_reload_state.call_count == 0
    assert mocked_execute_retry_slowdown.call_count == 0


@pytest.mark.parametrize(
    "error, retry_node_addr, require_reload_state",
    [
        (ConnectionError(), Address("random_replica", 6379), 1),
        (ConnectionRefusedError(), Address("random_replica", 6379), 1),
        (OSError(), Address("random_replica", 6379), 1),
        (ConnectTimeoutError(object()), Address("random_replica", 6379), 1),
        (ClusterDownError("CLUSTERDOWN clusterdown"), Address("random", 6379), 1),
        (TryAgainError("TRYAGAIN tryagain"), Address("random", 6379), 1),
        (MovedError("MOVED 12539 1.2.3.4:1000"), Address("1.2.3.4", 1000), 1),
        (AskError("ASK 12539 1.2.3.4:1000"), Address("1.2.3.4", 1000), 0),
        (LoadingError("LOADING loading"), Address("random", 6379), 1),
        (ProtocolError(), Address("random", 6379), 1),
        (ConnectionClosedError(), Address("random_replica", 6379), 1),
        (ConnectionForcedCloseError(), Address("random_replica", 6379), 1),
        (PoolClosedError(), Address("random_replica", 6379), 1),
    ],
)
async def test_execute__retryable_errors(mocker, error, retry_node_addr, require_reload_state):
    cl = Cluster(["addr1"])

    mocked_pooler = mocker.patch.object(cl, "_pooler", new=get_pooler_mock())

    pool = mocked_pooler._pool
    conn = mocked_pooler._conn

    pool_execute_count = 0

    def pool_execute_se(*args, **kwargs):
        nonlocal pool_execute_count
        if args[0] == b"ASKING":
            return b"OK"

        pool_execute_count += 1
        if pool_execute_count == 1:
            raise error

        return pool.execute.return_value

    pool.execute.side_effect = pool_execute_se
    conn.execute.side_effect = pool_execute_se

    mocked_manager = mocker.patch.object(cl, "_manager", new=get_manager_mock())
    state = mocked_manager.get_state.return_value
    state.slot_master.return_value.addr = Address("master", 6379)
    state.random_node.return_value.addr = Address("random", 6379)
    state.random_slot_replica.return_value.addr = Address("random_replica", 6379)

    mocked_execute_retry_slowdown = mocker.patch.object(
        cl, "_execute_retry_slowdown", new=create_async_mock()
    )

    result = await cl.execute("get", "key")

    assert mocked_manager.get_state.call_count == 2
    assert mocked_pooler.ensure_pool.call_count == 2
    assert mocked_manager.require_reload_state.call_count == require_reload_state
    assert result is pool.execute.return_value
    assert mocked_pooler.ensure_pool.call_args_list[0] == mock.call(Address("master", 6379))
    assert mocked_pooler.ensure_pool.call_args_list[1] == mock.call(retry_node_addr)
    # cl.determine_slot(b'key') == 12539
    state.slot_master.assert_called_once_with(12539)
    mocked_execute_retry_slowdown.assert_called_once_with(1, 10)


async def test_keys_master__success(mocker):
    commands_factory = mock.Mock()
    cl = Cluster(["addr1"], commands_factory=commands_factory)

    mocked_pooler = mocker.patch.object(cl, "_pooler", new=get_pooler_mock())
    pool = mocked_pooler.ensure_pool.return_value
    mocked_manager = mocker.patch.object(cl, "_manager", new=get_manager_mock())
    state = mocked_manager.get_state.return_value

    result = await cl.keys_master("key")

    assert result is commands_factory.return_value
    state.slot_master.assert_called_once_with(12539)
    mocked_pooler.ensure_pool.assert_called_once_with(state.slot_master.return_value.addr)
    commands_factory.assert_called_once_with(pool)


async def test_keys_master__with_retries_but_success(mocker):
    commands_factory = mock.Mock()
    cl = Cluster(["addr1"], commands_factory=commands_factory)

    mocked_pooler = mocker.patch.object(cl, "_pooler", new=get_pooler_mock())
    pool = mocked_pooler.ensure_pool.return_value

    errors = [
        ConnectionError(),
        LoadingError("LOADING loading"),
        ProtocolError(),
        ClusterDownError("CLUSTERDOWN cluster is down"),
        OSError("socket problem"),
    ]
    errors_iter = iter(errors)

    def ensure_pool_se(addr):
        try:
            raise next(errors_iter)
        except StopIteration:
            return pool

    mocked_pooler.ensure_pool.side_effect = ensure_pool_se

    mocked_manager = mocker.patch.object(cl, "_manager", new=get_manager_mock())
    state = mocked_manager.get_state.return_value

    mocker.patch.object(cl, "_execute_retry_slowdown", new=create_async_mock())

    result = await cl.keys_master("key")

    assert result is commands_factory.return_value
    state.slot_master.assert_called_with(12539)
    assert state.slot_master.call_count == 6
    mocked_pooler.ensure_pool.assert_has_calls([mock.call(state.slot_master.return_value.addr)] * 6)
    commands_factory.assert_called_once_with(pool)


async def test_keys_master__with_error_after_retries(mocker):
    cl = Cluster(["addr1"], max_attempts=3)

    mocked_pooler = mocker.patch.object(cl, "_pooler", new=get_pooler_mock())

    errors = [
        ConnectionError(),
        ConnectionError(),
        ClusterDownError("CLUSTERDOWN cluster is down"),
    ]

    mocked_pooler.ensure_pool.side_effect = errors

    mocked_manager = mocker.patch.object(cl, "_manager", new=get_manager_mock())
    state = mocked_manager.get_state.return_value

    mocker.patch.object(cl, "_execute_retry_slowdown", new=create_async_mock())

    with pytest.raises(ClusterDownError):
        await cl.keys_master("key")

    assert state.slot_master.call_count == 3


async def test_all_masters__with_error_after_retries(mocker):
    cl = Cluster(["addr1"], max_attempts=3)

    mocked_pooler = mocker.patch.object(cl, "_pooler", new=get_pooler_mock())
    pool = mocked_pooler.ensure_pool.return_value

    pools_returns = [
        OSError(),
        pool,
        ConnectionError(),
        pool,
        pool,
        ClusterDownError("CLUSTERDOWN cluster is down"),
    ]

    pools_returns_iter = iter(pools_returns)

    def ensure_pool_se(addr):
        res = next(pools_returns_iter)
        if isinstance(res, Exception):
            raise res
        return res

    mocked_pooler.ensure_pool.side_effect = ensure_pool_se

    mocked_manager = mocker.patch.object(cl, "_manager", new=get_manager_mock())
    state = mocked_manager.get_state.return_value
    state._data.masters = [
        mock.NonCallableMock(name="master1"),
        mock.NonCallableMock(name="master2"),
        mock.NonCallableMock(name="master3"),
    ]

    mocker.patch.object(cl, "_execute_retry_slowdown", new=create_async_mock())

    with pytest.raises(ClusterDownError):
        await cl.all_masters()


async def test_execute__with_attempt_timeout__non_idempotent(mocker, event_loop):
    cl = Cluster(["addr1"], attempt_timeout=0.001)

    mocked_pooler = mocker.patch.object(cl, "_pooler", new=get_pooler_mock())
    pool = mocked_pooler._pool
    pool.execute = mock.Mock(return_value=event_loop.create_future())

    mocked_manager = mocker.patch.object(cl, "_manager", new=get_manager_mock())
    state = mocked_manager.get_state.return_value
    state.slot_master.return_value.addr = Address("1.2.3.4", 9999)

    mocked_execute_retry_slowdown = mocker.patch.object(
        cl, "_execute_retry_slowdown", new=create_async_mock()
    )

    with pytest.raises(asyncio.TimeoutError):
        await cl.execute("set", "key", "value")

    assert mocked_manager.get_state.call_count == 1
    assert mocked_pooler.ensure_pool.call_count == 1
    assert mocked_manager.require_reload_state.call_count == 1
    mocked_execute_retry_slowdown.assert_not_called()


async def test_execute__with_attempt_timeout__idempotent(mocker):
    cl = Cluster(["addr1"], max_attempts=3)

    mocked_pooler = mocker.patch.object(cl, "_pooler", new=get_pooler_mock())
    pool = mocked_pooler._pool
    pool.execute.side_effect = asyncio.TimeoutError()

    mocked_manager = mocker.patch.object(cl, "_manager", new=get_manager_mock())
    state = mocked_manager.get_state.return_value
    state.slot_master.return_value.addr = Address("1.2.3.4", 9999)
    state.random_node.return_value.addr = Address("6.6.6.6", 9999)

    mocked_execute_retry_slowdown = mocker.patch.object(
        cl, "_execute_retry_slowdown", new=create_async_mock()
    )

    with pytest.raises(asyncio.TimeoutError):
        await cl.execute("get", "key")

    assert mocked_manager.get_state.call_count == 3
    assert mocked_pooler.ensure_pool.call_count == 3
    assert mocked_manager.require_reload_state.call_count == 3
    assert mocked_execute_retry_slowdown.call_count == 2


async def test_execute__with_attempt_timeout__idempotent__success(mocker):
    cl = Cluster(["addr1"], max_attempts=4)

    mocked_pooler = mocker.patch.object(cl, "_pooler", new=get_pooler_mock())
    pool = mocked_pooler._pool
    pool.execute.side_effect = [
        asyncio.TimeoutError(),
        asyncio.TimeoutError(),
        asyncio.TimeoutError(),
        b"VALUE",
    ]

    mocked_manager = mocker.patch.object(cl, "_manager", new=get_manager_mock())
    state = mocked_manager.get_state.return_value
    state.slot_master.return_value.addr = Address("1.2.3.4", 9999)
    state.random_node.return_value.addr = Address("6.6.6.6", 9999)

    mocked_execute_retry_slowdown = mocker.patch.object(
        cl, "_execute_retry_slowdown", new=create_async_mock()
    )

    result = await cl.execute("get", "key")

    assert result == b"VALUE"
    assert mocked_manager.get_state.call_count == 4
    assert mocked_pooler.ensure_pool.call_count == 4
    assert mocked_manager.require_reload_state.call_count == 3
    assert mocked_execute_retry_slowdown.call_count == 3


async def test_keys_master__with_attempt_timeout(mocker):
    cl = Cluster(["addr1"], max_attempts=2)

    mocked_pooler = mocker.patch.object(cl, "_pooler", new=get_pooler_mock())
    pool = mocked_pooler._pool
    pool.execute.side_effect = asyncio.TimeoutError()

    mocked_manager = mocker.patch.object(cl, "_manager", new=get_manager_mock())
    state = mocked_manager.get_state.return_value
    state.slot_master.return_value.addr = Address("1.2.3.4", 9999)

    mocked_execute_retry_slowdown = mocker.patch.object(
        cl, "_execute_retry_slowdown", new=create_async_mock()
    )

    with pytest.raises(asyncio.TimeoutError):
        await cl.keys_master("foo{key}", "bar{key}")

    assert mocked_manager.get_state.call_count == 2
    assert mocked_pooler.ensure_pool.call_count == 2
    assert mocked_manager.require_reload_state.call_count == 2
    assert mocked_execute_retry_slowdown.call_count == 1


async def test_all_masters__with_attempt_timeout(mocker, event_loop):
    cl = Cluster(["addr1"], attempt_timeout=0.001)

    mocked_pooler = mocker.patch.object(cl, "_pooler", new=get_pooler_mock())
    pool = mocked_pooler._pool
    execute1_fut = event_loop.create_future()
    execute1_fut.set_result(None)
    execute2_fut = event_loop.create_future()

    execute_futs_iter = iter([execute1_fut, execute2_fut])

    async def execute_side_effect(*args, **kwargs):
        return await next(execute_futs_iter)

    pool.execute.side_effect = execute_side_effect

    mocked_manager = mocker.patch.object(cl, "_manager", new=get_manager_mock())
    state = mocked_manager.get_state.return_value
    state._data.masters = [
        mock.NonCallableMock(addr=Address("1.2.3.4", 6666)),
        mock.NonCallableMock(addr=Address("1.2.3.4", 9999)),
    ]

    mocked_execute_retry_slowdown = mocker.patch.object(
        cl, "_execute_retry_slowdown", new=create_async_mock()
    )

    with pytest.raises(asyncio.TimeoutError):
        await cl.all_masters()

    assert mocked_manager.get_state.call_count == 1
    assert mocked_pooler.ensure_pool.call_count == 2
    assert mocked_manager.require_reload_state.call_count == 1
    mocked_execute_retry_slowdown.assert_not_called()
