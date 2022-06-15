try:
    from aioredis.util import _NOTSET, wait_convert, wait_ok
except ImportError:
    from .._aioredis.util import _NOTSET, wait_convert, wait_ok


(_NOTSET,)


__all__ = (
    "wait_convert",
    "wait_ok",
)
