import logging

from aioredis_cluster._aioredis.util import wait_ok
from aioredis_cluster.abc import AbcConnection
from aioredis_cluster.aioredis import RedisConnection as BaseConnection


logger = logging.getLogger(__name__)


class RedisConnection(BaseConnection, AbcConnection):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self._readonly = False

    @property
    def readonly(self) -> bool:
        return self._readonly

    async def set_readonly(self, value: bool) -> None:
        """Turn node readonly or readwrite mode."""

        if value:
            fut = self.execute(b"READONLY")
        else:
            fut = self.execute(b"READWRITE")
        await wait_ok(fut)
        self._readonly = value

    async def auth_with_username(self, username: str, password: str) -> bool:
        """Authenticate to server with username and password."""
        fut = self.execute("AUTH", username, password)
        return await wait_ok(fut)
