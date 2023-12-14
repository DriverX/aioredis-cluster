from typing import Awaitable, Callable, Mapping

from aioredis_cluster._aioredis.commands.pubsub import wait_return_channels
from aioredis_cluster._aioredis.util import wait_make_dict
from aioredis_cluster.abc import AbcChannel, AbcConnection


class ShardedPubSubCommandsMixin:
    """Sharded Pub/Sub commands mixin.

    For commands details see: https://redis.io/docs/manual/pubsub/#sharded-pubsub
    """

    execute: Callable[..., Awaitable]
    _pool_or_conn: AbcConnection

    def spublish(self, channel, message):
        """Post a message to channel."""
        return self.execute(b"SPUBLISH", channel, message)

    def ssubscribe(self, channel, *channels):
        """Switch connection to Pub/Sub mode and
        subscribe to specified channels.

        Arguments can be instances of :class:`~aioredis.Channel`.

        Returns :func:`asyncio.gather()` coroutine which when done will return
        a list of :class:`~aioredis.Channel` objects.
        """
        conn = self._pool_or_conn
        return wait_return_channels(
            conn.execute_pubsub(b"SSUBSCRIBE", channel, *channels),
            conn,
            "sharded_pubsub_channels",
        )

    def sunsubscribe(self, channel, *channels):
        """Unsubscribe from specific channels.

        Arguments can be instances of :class:`~aioredis.Channel`.
        """
        conn = self._pool_or_conn
        return conn.execute_pubsub(b"SUNSUBSCRIBE", channel, *channels)

    def pubsub_shardchannels(self, pattern=None):
        """Lists the currently active channels."""
        args = [b"PUBSUB", b"SHARDCHANNELS"]
        if pattern is not None:
            args.append(pattern)
        return self.execute(*args)

    def pubsub_shardnumsub(self, *channels):
        """Returns the number of subscribers for the specified channels."""
        return wait_make_dict(self.execute(b"PUBSUB", b"SHARDNUMSUB", *channels))

    @property
    def sharded_pubsub_channels(self) -> Mapping[str, AbcChannel]:
        """Returns read-only channels dict.

        See :attr:`~aioredis.RedisConnection.pubsub_channels`
        """
        return self._pool_or_conn.sharded_pubsub_channels
