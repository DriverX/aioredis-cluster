try:
    from aioredis.abc import AbcChannel, AbcConnection, AbcPool
except ImportError:
    from .._aioredis.abc import AbcChannel, AbcConnection, AbcPool


__all__ = (
    "AbcConnection",
    "AbcPool",
    "AbcChannel",
)
