import asyncio
from asyncio.queues import Queue
from unittest import mock

import pytest

from aioredis_cluster.connection import RedisConnection
from aioredis_cluster.errors import MovedError


class Reader:
    def __init__(self) -> None:
        self.queue = Queue()
        self.eof = False

    def set_parser(self, *args):
        pass

    async def readobj(self):
        result = await self.queue.get()
        self.queue.task_done()
        return result

    def at_eof(self) -> bool:
        return self.eof and self.queue.empty()


async def test_moved_with_pubsub():
    reader = Reader()
    writer = mock.AsyncMock()
    writer.write = mock.Mock()
    writer.transport = mock.NonCallableMock()
    redis = RedisConnection(reader=reader, writer=writer, address="localhost:6379")

    s = redis.execute_pubsub("SSUBSCRIBE", "a")
    reader.queue.put_nowait((b"ssubscribe", b"a", 10))
    await s

    s = redis.execute_pubsub("SSUBSCRIBE", "b")
    await reader.queue.put(MovedError("1 1 127.0.0.1:6379"))
    with pytest.raises(MovedError):
        await asyncio.wait_for(s, timeout=1)
    assert not redis._reader_task.done(), redis._reader_task.exception()

    reader.queue.put_nowait((b"smessage", b"a", b"123"))
    assert not redis._reader_task.done()
    redis.close()
    await redis.wait_closed()
