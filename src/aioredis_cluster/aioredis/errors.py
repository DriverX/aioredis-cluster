from ._errors import ConnectTimeoutError


try:
    from aioredis.errors import *  # noqa
except ImportError:
    from ._errors import *  # noqa


__all__ = (  # noqa
    "RedisError",
    "ConnectTimeoutError",
    "ProtocolError",
    "ReplyError",
    "MaxClientsError",
    "AuthError",
    "PipelineError",
    "MultiExecError",
    "WatchVariableError",
    "ChannelClosedError",
    "ConnectionClosedError",
    "ConnectionForcedCloseError",
    "PoolClosedError",
    "MasterNotFoundError",
    "SlaveNotFoundError",
    "ReadOnlyError",
)
