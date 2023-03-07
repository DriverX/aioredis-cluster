from aioredis_cluster.aioredis.errors import (
    ConnectionClosedError,
    PoolClosedError,
    RedisError,
    ReplyError,
)
from aioredis_cluster.util import parse_moved_response_error

__all__ = [
    "ConnectTimeoutError",
    "RedisClusterError",
    "ClusterClosedError",
    "ClusterStateError",
    "UncoveredSlotError",
    "MovedError",
    "AskError",
    "TryAgainError",
    "ClusterDownError",
    "LoadingError",
]


class ConnectTimeoutError(RedisError):
    """Raises than connect to redis is timed out"""

    def __init__(self, address) -> None:
        super().__init__(address)

        self.address = address


class RedisClusterError(RedisError):
    """Cluster exception class for aioredis exceptions."""


class ClusterClosedError(RedisClusterError):
    """Raises while cluster already closed and not abla to execute command"""


class ClusterStateError(RedisClusterError):
    """Raises while cluster state is corrupted or not initialized"""


class UncoveredSlotError(ClusterStateError):
    """Raise than slot not found in cluster slots state"""

    def __init__(self, slot: int) -> None:
        super().__init__(slot)

        self.slot = slot


class MovedError(ReplyError):
    """Raised when key moved to another node in cluster or
    ASK if key in resharding state."""

    MATCH_REPLY = ("MOVED ",)

    def __init__(self, msg, *args) -> None:
        super().__init__(msg, *args)

        self.info = parse_moved_response_error(msg)

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.info!r}>"


class AskError(ReplyError):
    """Raised when key in resharding state."""

    MATCH_REPLY = ("ASK ",)

    def __init__(self, msg, *args) -> None:
        super().__init__(msg, *args)

        self.info = parse_moved_response_error(msg)

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.info!r}>"


class TryAgainError(ReplyError):
    """Raised when keys during the resharding -
    split between the source and destination nodes"""

    MATCH_REPLY = ("TRYAGAIN ",)


class ClusterDownError(ReplyError):
    """Raised when cluster is probably down"""

    MATCH_REPLY = ("CLUSTERDOWN ",)


class LoadingError(ReplyError):
    """Raised when cluster replica node is loading"""

    MATCH_REPLY = ("LOADING ",)


network_errors = (ConnectionError, ConnectTimeoutError)
closed_errors = (ConnectionClosedError, PoolClosedError)
