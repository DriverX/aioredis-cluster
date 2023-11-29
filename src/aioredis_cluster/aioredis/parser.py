try:
    from aioredis.parser import PyReader, Reader
except ImportError:
    from .._aioredis.parser import PyReader, Reader

__all__ = (
    "Reader",
    "PyReader",
)
