import pytest

from aioredis_cluster.commands.commands import blocked_for_cluster


async def test_blocked_for_cluster(mocker, event_loop) -> None:
    cluster_conn = object()

    def conn_is_cluster_se(conn):
        nonlocal cluster_conn
        return cluster_conn is conn

    mocker.patch(
        blocked_for_cluster.__module__ + ".conn_is_cluster", side_effect=conn_is_cluster_se
    )

    class TestClass:
        def __init__(self, conn) -> None:
            self.connection = conn

        @blocked_for_cluster
        async def async_method(self, param: str) -> None:
            pass

        @blocked_for_cluster
        def fut_method(self, param: str):
            fut = event_loop.create_future()
            fut.set_result(None)
            return fut

    instance = TestClass(cluster_conn)

    with pytest.raises(NotImplementedError, match="Command is blocked"):
        await instance.async_method("foo")

    with pytest.raises(NotImplementedError, match="Command is blocked"):
        await instance.fut_method("foo")

    instance = TestClass(object())
    await instance.async_method("foo")
    await instance.fut_method("foo")
