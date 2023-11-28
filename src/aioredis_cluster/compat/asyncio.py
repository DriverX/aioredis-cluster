import asyncio

try:
    from async_timeout import timeout as _async_timeout
except ImportError:
    _async_timeout = None  # type: ignore


__all__ = ("timeout",)


if hasattr(asyncio, "timeout"):
    timeout = asyncio.timeout
elif _async_timeout is not None:
    timeout = _async_timeout
else:
    raise RuntimeError("async timeout compat version not found")
