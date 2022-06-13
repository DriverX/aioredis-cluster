from ._errors import ConnectTimeoutError


try:
    from aioredis.errors import (
        AuthError,
        ChannelClosedError,
        ConnectionClosedError,
        ConnectionForcedCloseError,
        MasterNotFoundError,
        MasterReplyError,
        MaxClientsError,
        MultiExecError,
        PipelineError,
        PoolClosedError,
        ProtocolError,
        ReadOnlyError,
        RedisError,
        ReplyError,
        SlaveNotFoundError,
        SlaveReplyError,
        WatchVariableError,
    )
except ImportError:
    from ._errors import (
        AuthError,
        ChannelClosedError,
        ConnectionClosedError,
        ConnectionForcedCloseError,
        MasterNotFoundError,
        MasterReplyError,
        MaxClientsError,
        MultiExecError,
        PipelineError,
        PoolClosedError,
        ProtocolError,
        ReadOnlyError,
        RedisError,
        ReplyError,
        SlaveNotFoundError,
        SlaveReplyError,
        WatchVariableError,
    )


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
    "SlaveReplyError",
    "ReadOnlyError",
)
