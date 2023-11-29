try:
    from aioredis.stream import StreamReader, open_connection, open_unix_connection
except ImportError:
    from .._aioredis.stream import StreamReader, open_connection, open_unix_connection

__all__ = (
    "open_connection",
    "open_unix_connection",
    "StreamReader",
)
