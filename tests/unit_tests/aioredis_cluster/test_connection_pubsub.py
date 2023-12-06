import asyncio
from asyncio.queues import Queue
from unittest import mock

import pytest

from aioredis_cluster.aioredis.stream import StreamReader
from aioredis_cluster.command_info.commands import PUBSUB_SUBSCRIBE_COMMANDS
from aioredis_cluster.connection import RedisConnection
from aioredis_cluster.errors import MovedError, RedisError

pytestmark = [pytest.mark.timeout(1)]


async def moment(times: int = 1) -> None:
    for _ in range(times):
        await asyncio.sleep(0)


class MockedReader(StreamReader):
    def __init__(self) -> None:
        self.queue: Queue = Queue()
        self.eof = False

    def set_parser(self, *args):
        pass

    def feed_data(self, data):
        pass

    async def readobj(self):
        result = await self.queue.get()
        self.queue.task_done()
        return result

    def at_eof(self) -> bool:
        return self.eof and self.queue.empty()


def get_mocked_reader():
    return MockedReader()


def get_mocked_writer():
    writer = mock.AsyncMock()
    writer.write = mock.Mock()
    writer.transport = mock.NonCallableMock()
    return writer


async def close_connection(conn: RedisConnection) -> None:
    conn.close()
    await conn.wait_closed()


async def execute(redis: RedisConnection, *args, **kwargs):
    return await redis.execute(*args, **kwargs)


async def execute_pubsub(redis: RedisConnection, *args, **kwargs):
    return await redis.execute_pubsub(*args, **kwargs)


async def test_execute__simple_subscribe(add_async_finalizer):
    reader = get_mocked_reader()
    writer = get_mocked_writer()
    redis = RedisConnection(reader=reader, writer=writer, address="localhost:6379")
    add_async_finalizer(lambda: close_connection(redis))

    reader.queue.put_nowait([b"subscribe", b"chan", 1])
    reader.queue.put_nowait([b"ssubscribe", b"chan", 1])
    reader.queue.put_nowait([b"psubscribe", b"chan", 2])

    result_channel = await redis.execute_pubsub("SUBSCRIBE", "chan")
    result_sharded = await redis.execute_pubsub("SSUBSCRIBE", "chan")
    result_pattern = await redis.execute_pubsub("PSUBSCRIBE", "chan")

    assert result_channel == [[b"subscribe", b"chan", 1]]
    assert result_pattern == [[b"psubscribe", b"chan", 2]]
    assert result_sharded == [[b"ssubscribe", b"chan", 1]]
    assert redis.in_pubsub == 3
    assert redis._client_in_pubsub is True
    assert redis._server_in_pubsub is True
    assert len(redis._waiters) == 0

    assert "chan" in redis.pubsub_channels
    assert "chan" in redis.pubsub_patterns
    assert "chan" in redis.sharded_pubsub_channels

    assert redis.pubsub_channels["chan"] is not redis.pubsub_patterns["chan"]
    assert redis.pubsub_channels["chan"] is not redis.sharded_pubsub_channels["chan"]
    assert redis.pubsub_patterns["chan"] is not redis.sharded_pubsub_channels["chan"]


async def test_execute__simple_unsubscribe(add_async_finalizer):
    reader = get_mocked_reader()
    writer = get_mocked_writer()
    redis = RedisConnection(reader=reader, writer=writer, address="localhost:6379")
    add_async_finalizer(lambda: close_connection(redis))

    reader.queue.put_nowait([b"ssubscribe", b"chan", 1])
    reader.queue.put_nowait([b"psubscribe", b"chan", 1])
    reader.queue.put_nowait([b"subscribe", b"chan", 2])

    await redis.execute_pubsub("SSUBSCRIBE", "chan")
    await redis.execute_pubsub("PSUBSCRIBE", "chan")
    await redis.execute_pubsub("SUBSCRIBE", "chan")

    assert redis.in_pubsub == 3

    reader.queue.put_nowait([b"unsubscribe", b"chan", 1])
    reader.queue.put_nowait([b"punsubscribe", b"chan", 0])
    reader.queue.put_nowait([b"sunsubscribe", b"chan", 0])
    result_channel = await redis.execute_pubsub("UNSUBSCRIBE", "chan")
    result_pattern = await redis.execute_pubsub("PUNSUBSCRIBE", "chan")
    result_sharded = await redis.execute_pubsub("SUNSUBSCRIBE", "chan")

    await moment()

    assert redis.in_pubsub == 0
    assert result_channel == [[b"unsubscribe", b"chan", 1]]
    assert result_pattern == [[b"punsubscribe", b"chan", 0]]
    assert result_sharded == [[b"sunsubscribe", b"chan", 0]]
    assert redis._client_in_pubsub is False
    assert redis._server_in_pubsub is False


@pytest.mark.parametrize(
    "command",
    [
        "SUBSCRIBE",
        "PSUBSCRIBE",
        "SSUBSCRIBE",
        "UNSUBSCRIBE",
        "PUNSUBSCRIBE",
        "SUNSUBSCRIBE",
    ],
)
async def test_execute__first_command(add_async_finalizer, command: str):
    reader = get_mocked_reader()
    writer = get_mocked_writer()
    redis = RedisConnection(reader=reader, writer=writer, address="localhost:6379")
    add_async_finalizer(lambda: close_connection(redis))

    is_subscribe_command = command in PUBSUB_SUBSCRIBE_COMMANDS
    kind = command.encode().lower()

    subs_num = 1 if is_subscribe_command else 0
    reader.queue.put_nowait((kind, b"chan", subs_num))

    await redis.execute_pubsub(command, "chan")

    if is_subscribe_command:
        assert redis.in_pubsub == 1
        assert redis._client_in_pubsub is True
        assert redis._server_in_pubsub is True
    else:
        assert redis.in_pubsub == 0
        assert redis._client_in_pubsub is False
        assert redis._server_in_pubsub is False


async def test_execute__half_open_pubsub_mode(add_async_finalizer):
    reader = get_mocked_reader()
    writer = get_mocked_writer()
    redis = RedisConnection(reader=reader, writer=writer, address="localhost:6379")
    add_async_finalizer(lambda: close_connection(redis))

    get_task = asyncio.ensure_future(execute(redis, "GET", "foo", encoding="utf-8"))
    ping1_task = asyncio.ensure_future(execute(redis, "PING", "ping_reply1"))
    subs_task = asyncio.ensure_future(execute_pubsub(redis, "SSUBSCRIBE", "chan"))
    # SET not send and execute() must raise RedisError exception
    set_task = asyncio.ensure_future(execute(redis, "SET", "foo", "val2"))
    ping2_task = asyncio.ensure_future(execute(redis, "PING", "ping_reply2", encoding="utf-8"))

    # need extra loop for asyncio.ensure_future starts a tasks
    await moment()

    assert redis._client_in_pubsub is True
    assert redis._server_in_pubsub is False

    reader.queue.put_nowait(b"val1")
    reader.queue.put_nowait(b"ping_reply1")
    reader.queue.put_nowait([b"ssubscribe", b"chan", 1])

    # This is incorrect. Redis must return error with restrict this command in PubSub mode
    # and client must prevent send SET command in half-open PubSub mode
    # reader.queue.put_nowait(b"OK")

    reader.queue.put_nowait([b"pong", b"ping_reply2"])

    # make 2 extra loops
    await moment(2)

    assert get_task.done() is True
    assert ping1_task.done() is True
    assert subs_task.done() is True
    assert set_task.done() is True
    assert ping2_task.done() is True

    assert get_task.result() == "val1"
    assert ping1_task.result() == b"ping_reply1"
    assert subs_task.result() == [[b"ssubscribe", b"chan", 1]]
    with pytest.raises(RedisError, match="Connection in PubSub mode"):
        assert set_task.result()
    assert ping2_task.result() == "ping_reply2"

    assert redis.in_pubsub == 1


async def test_execute__unsubscribe_without_subscribe(add_async_finalizer):
    reader = get_mocked_reader()
    writer = get_mocked_writer()
    redis = RedisConnection(reader=reader, writer=writer, address="localhost:6379")
    add_async_finalizer(lambda: close_connection(redis))

    reader.queue.put_nowait([b"sunsubscribe", b"chan", 0])
    await redis.execute_pubsub("SUNSUBSCRIBE", "chan")
    reader.queue.put_nowait((b"punsubscribe", b"chan", 0))
    await redis.execute_pubsub("PUNSUBSCRIBE", "chan")
    reader.queue.put_nowait((b"unsubscribe", b"chan", 0))
    await redis.execute_pubsub("UNSUBSCRIBE", "chan")

    assert redis.in_pubsub == 0
    assert redis._client_in_pubsub is False
    assert redis._server_in_pubsub is False


async def test__redis_push_unsubscribe(add_async_finalizer):
    reader = get_mocked_reader()
    writer = get_mocked_writer()
    redis = RedisConnection(reader=reader, writer=writer, address="localhost:6379")
    add_async_finalizer(lambda: close_connection(redis))

    reader.queue.put_nowait([b"subscribe", b"chan:1", 1])
    reader.queue.put_nowait([b"subscribe", b"chan:2", 2])
    reader.queue.put_nowait([b"psubscribe", b"chan:3", 3])
    reader.queue.put_nowait([b"psubscribe", b"chan:4", 4])
    reader.queue.put_nowait([b"ssubscribe", b"chan:5:{shard}", 1])
    reader.queue.put_nowait([b"ssubscribe", b"chan:6:{shard}", 2])
    await redis.execute_pubsub("SUBSCRIBE", "chan:1", "chan:2")
    await redis.execute_pubsub("PSUBSCRIBE", "chan:3", "chan:4")
    await redis.execute_pubsub("SSUBSCRIBE", "chan:5:{shard}", "chan:6:{shard}")

    assert redis.in_pubsub == 6

    reader.queue.put_nowait([b"unsubscribe", b"chan:1", 3])
    reader.queue.put_nowait([b"unsubscribe", b"chan:2", 2])
    reader.queue.put_nowait([b"punsubscribe", b"chan:3", 1])
    reader.queue.put_nowait([b"punsubscribe", b"chan:4", 0])
    reader.queue.put_nowait([b"sunsubscribe", b"chan:5:{shard}", 1])
    reader.queue.put_nowait([b"sunsubscribe", b"chan:6:{shard}", 0])
    # some extra channel
    reader.queue.put_nowait([b"unsubscribe", b"chan:7", 3])

    await moment()

    assert redis.in_pubsub == 0

    assert redis._reader_task.done() is False


async def test_moved_with_pubsub(add_async_finalizer):
    reader = get_mocked_reader()
    writer = get_mocked_writer()
    redis = RedisConnection(reader=reader, writer=writer, address="localhost:6379")
    add_async_finalizer(lambda: close_connection(redis))

    reader.queue.put_nowait((b"ssubscribe", b"chan1", 1))

    # key slot for chan1 - 2323
    await redis.execute_pubsub("SSUBSCRIBE", "chan1")

    assert len(redis.sharded_pubsub_channels) == 1
    assert "chan1" in redis.sharded_pubsub_channels

    # key slot chan2:{shard1} - 10271
    reader.queue.put_nowait([b"ssubscribe", b"chan2:{shard1}", 11])
    reader.queue.put_nowait([b"ssubscribe", b"chan3:{shard1}", 11])
    await redis.execute_pubsub("SSUBSCRIBE", "chan2:{shard1}")
    await redis.execute_pubsub("SSUBSCRIBE", "chan3:{shard1}")

    assert len(redis.sharded_pubsub_channels) == 3
    assert "chan2:{shard1}" in redis.sharded_pubsub_channels
    assert "chan3:{shard1}" in redis.sharded_pubsub_channels

    reader.queue.put_nowait(MovedError("MOVED 2323 127.0.0.1:6379"))
    await moment()

    assert len(redis.sharded_pubsub_channels) == 2
    assert "chan1" not in redis.sharded_pubsub_channels

    reader.queue.put_nowait(MovedError("MOVED 10271 127.0.0.1:6379"))
    await moment()

    assert len(redis.sharded_pubsub_channels) == 0

    assert redis._reader_task.done() is False, redis._reader_task.exception()


async def test_execute__unexpectable_unsubscribe_and_moved(add_async_finalizer):
    reader = get_mocked_reader()
    writer = get_mocked_writer()
    redis = RedisConnection(reader=reader, writer=writer, address="localhost:6379")
    add_async_finalizer(lambda: close_connection(redis))

    reader.queue.put_nowait([b"ssubscribe", b"chan1:{shard1}", 1])
    reader.queue.put_nowait([b"ssubscribe", b"chan2:{shard1}", 2])

    await redis.execute_pubsub("SSUBSCRIBE", "chan1:{shard1}")
    await redis.execute_pubsub("SSUBSCRIBE", "chan2:{shard1}")

    reader.queue.put_nowait([b"sunsubscribe", b"chan2:{shard1}", 1])
    reader.queue.put_nowait(MovedError("MOVED 10271 127.0.0.1:6379"))
    await moment()

    assert redis.in_pubsub == 0
    assert redis._reader_task.done() is False


async def test_execute__ssubscribe_with_first_moved(add_async_finalizer):
    reader = get_mocked_reader()
    writer = get_mocked_writer()
    redis = RedisConnection(reader=reader, writer=writer, address="localhost:6379")
    add_async_finalizer(lambda: close_connection(redis))

    reader.queue.put_nowait(MovedError("MOVED 10271 127.0.0.1:6379"))

    with pytest.raises(MovedError, match="MOVED 10271 127.0.0.1:6379"):
        await redis.execute_pubsub("SSUBSCRIBE", "chan1:{shard1}")

    assert redis.in_pubsub == 0
    assert redis._client_in_pubsub is False
    assert redis._server_in_pubsub is False
    assert redis._reader_task.done() is False


async def test_execute__client_unsubscribe_with_server_unsubscribe(add_async_finalizer):
    reader = get_mocked_reader()
    writer = get_mocked_writer()
    redis = RedisConnection(reader=reader, writer=writer, address="localhost:6379")
    add_async_finalizer(lambda: close_connection(redis))

    reader.queue.put_nowait([b"ssubscribe", b"chan:1", 1])
    sub_result1 = await redis.execute_pubsub("SSUBSCRIBE", "chan:1")

    reader.queue.put_nowait([b"ssubscribe", b"chan:2", 2])
    sub_result2 = await redis.execute_pubsub("SSUBSCRIBE", "chan:2")

    reader.queue.put_nowait([b"ssubscribe", b"chan:3", 3])
    sub_result3 = await redis.execute_pubsub("SSUBSCRIBE", "chan:3")

    assert sub_result1 == [[b"ssubscribe", b"chan:1", 1]]
    assert sub_result2 == [[b"ssubscribe", b"chan:2", 2]]
    assert sub_result3 == [[b"ssubscribe", b"chan:3", 3]]
    assert redis.in_pubsub == 3

    reader.queue.put_nowait([b"sunsubscribe", b"chan:1", 2])
    reader.queue.put_nowait([b"sunsubscribe", b"chan:3", 1])
    reader.queue.put_nowait([b"sunsubscribe", b"chan:2", 0])
    reader.queue.put_nowait(MovedError("MOVED 1 127.0.0.1:6379"))
    await moment()

    reader.queue.put_nowait([b"sunsubscribe", b"chan:3", 0])
    unsub_result3 = await redis.execute_pubsub("SUNSUBSCRIBE", "chan:3")

    assert unsub_result3 == [[b"sunsubscribe", b"chan:3", 0]]

    await moment()

    assert redis.in_pubsub == 0

    assert redis._reader_task is not None
    assert redis._reader_task.done() is False


async def test_execute__ping(add_async_finalizer):
    reader = get_mocked_reader()
    writer = get_mocked_writer()
    redis = RedisConnection(reader=reader, writer=writer, address="localhost:6379")
    redis._in_pubsub = 1
    add_async_finalizer(lambda: close_connection(redis))

    ping1_task = asyncio.ensure_future(redis.execute("PING"))
    subs_task = asyncio.ensure_future(redis.execute_pubsub("SUBSCRIBE", "chan"))
    await moment()
    ping2_task = asyncio.ensure_future(redis.execute("PING"))
    ping3_task = asyncio.ensure_future(redis.execute("PING", "my_message"))
    reader.queue.put_nowait(b"PONG")
    reader.queue.put_nowait((b"subscribe", b"chan", 1))
    reader.queue.put_nowait(b"PONG")
    reader.queue.put_nowait((b"pong", "my_message"))
    await moment(2)

    assert redis.in_pubsub == 1

    assert ping1_task.done() is True
    assert subs_task.done() is True
    assert ping2_task.done() is True
    assert ping3_task.done() is True

    assert redis._reader_task is not None
    assert redis._reader_task.done() is False


async def test_subscribe_and_receive_messages(add_async_finalizer):
    reader = get_mocked_reader()
    writer = get_mocked_writer()
    redis = RedisConnection(reader=reader, writer=writer, address="localhost:6379")
    add_async_finalizer(lambda: close_connection(redis))

    reader.queue.put_nowait([b"subscribe", b"chan", 1])
    reader.queue.put_nowait([b"ssubscribe", b"chan:{shard}", 1])
    reader.queue.put_nowait([b"psubscribe", b"chan:*", 2])

    await redis.execute_pubsub("SUBSCRIBE", "chan")
    await redis.execute_pubsub("SSUBSCRIBE", "chan:{shard}")
    await redis.execute_pubsub("PSUBSCRIBE", "chan:*")

    channel = redis.pubsub_channels["chan"]
    pattern = redis.pubsub_patterns["chan:*"]
    sharded = redis.sharded_pubsub_channels["chan:{shard}"]

    reader.queue.put_nowait([b"smessage", b"chan:{shard}", b"sharded_msg"])
    reader.queue.put_nowait([b"pmessage", b"chan:*", b"chan:foo", b"pattern_msg"])
    reader.queue.put_nowait([b"message", b"chan", b"channel_msg"])

    await moment()

    channel_msg = await channel.get()
    pattern_msg = await pattern.get()
    sharded_msg = await sharded.get()

    assert channel_msg == b"channel_msg"
    assert pattern_msg == (b"chan:foo", b"pattern_msg")
    assert sharded_msg == b"sharded_msg"
