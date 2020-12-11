import asyncio
from typing import AsyncContextManager, Dict, Generic, TypeVar

import attr


__all__ = [
    "ResourceLock",
]

_T = TypeVar("_T")


@attr.s(slots=True)
class LockAndCount:
    lock: asyncio.Lock = attr.ib()
    acquires: int = attr.ib()


class ResourceLock(Generic[_T]):
    def __init__(self) -> None:
        self._locks: Dict[_T, LockAndCount] = {}

    def locked(self, lock_key: _T) -> bool:
        return lock_key in self._locks

    async def acquire(self, lock_key: _T) -> None:
        if lock_key not in self._locks:
            lac = LockAndCount(asyncio.Lock(), 1)
            self._locks[lock_key] = lac
        else:
            lac = self._locks[lock_key]
            lac.acquires += 1

        await lac.lock.acquire()

    def release(self, lock_key: _T) -> None:
        if self.locked(lock_key):
            lac = self._locks[lock_key]
            lac.acquires -= 1

            if lac.acquires == 0:
                del self._locks[lock_key]

            lac.lock.release()
        else:
            raise RuntimeError("Lock is not acquired.")

    def __call__(self, lock_key: _T) -> "_AsyncContextManager[_T]":
        return _AsyncContextManager(self, lock_key)


class _AsyncContextManager(Generic[_T], AsyncContextManager):
    __slots__ = ("_lock", "_lock_key")

    def __init__(self, lock: ResourceLock[_T], lock_key: _T) -> None:
        self._lock = lock
        self._lock_key = lock_key

    async def __aenter__(self):
        await self._lock.acquire(self._lock_key)
        # similar behavior as asyncio.Lock
        return None

    async def __aexit__(self, exc_type, exc, tb):
        self._lock.release(self._lock_key)
