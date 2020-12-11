from aioredis.abc import AbcChannel, AbcConnection, AbcPool


__all__ = [
    "AbcConnection",
    "AbcPool",
    "AbcChannel",
    "AbcCluster",
]


class AbcCluster(AbcConnection):
    pass
