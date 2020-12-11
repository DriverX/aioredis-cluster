from typing import Tuple, Type, TypeVar, Union

from aioredis_cluster.util import RedirInfo


class RedisError(Exception):
    pass


class ConnectionClosedError(RedisError):
    pass


class PoolClosedError(RedisError):
    pass


_TReplyError = TypeVar('_TReplyError', bound='ReplyError')


class ReplyError(RedisError):
    MATCH_REPLY: Union[str, Tuple[str, ...]]

    def __new__(cls: Type[_TReplyError], msg: str, *args) -> _TReplyError:
        pass


class ConnectTimeoutError(RedisError):
    def __init__(self, address) -> None:
        pass


class RedisClusterError(RedisError):
    pass


class ClusterClosedError(RedisClusterError):
    pass


class ClusterStateError(RedisClusterError):
    pass


class UncoveredSlotError(ClusterStateError):
    slot: int

    def __init__(self, slot: int) -> None:
        pass


class MovedError(ReplyError):
    info: RedirInfo


class AskError(ReplyError):
    info: RedirInfo


class TryAgainError(ReplyError):
    pass


class ClusterDownError(ReplyError):
    pass


class LoadingError(ReplyError):
    pass


network_errors: Tuple[Type[Exception], ...]
closed_errors: Tuple[Type[Exception], ...]
