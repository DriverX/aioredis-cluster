from ._errors import ConnectTimeoutError


try:
    from aioredis.errors import (
        MasterReplyError,
        RedisError,
        ProtocolError,
        ReplyError,
        MaxClientsError,
        AuthError,
        PipelineError,
        MultiExecError,
        WatchVariableError,
        ChannelClosedError,
        ConnectionClosedError,
        ConnectionForcedCloseError,
        PoolClosedError,
        MasterNotFoundError,
        SlaveNotFoundError,
        ReadOnlyError,
    )
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
    "MasterReplyError",
    "SlaveNotFoundError",
    "ReadOnlyError",
)
