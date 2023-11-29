from binascii import crc_hqx

try:
    from aioredis_cluster.speedup.crc import crc16 as cy_crc16
    from aioredis_cluster.speedup.crc import key_slot as cy_key_slot
except ImportError:
    cy_crc16 = None
    cy_key_slot = None

__all__ = (
    "crc16",
    "key_slot",
    "determine_slot",
    "CrossSlotKeysError",
)

REDIS_CLUSTER_HASH_SLOTS = 16384


def py_crc16(data: bytes) -> int:
    return crc_hqx(data, 0)


def py_key_slot(k: bytes, bucket: int = REDIS_CLUSTER_HASH_SLOTS) -> int:
    """Calculate key slot for a given key.

    :param key - bytes
    :param bucket - int
    """

    start = k.find(b"{")
    if start > -1:
        end = k.find(b"}", start + 1)
        if end > -1 and end != start + 1:
            k = k[start + 1 : end]
    return py_crc16(k) % bucket


if cy_crc16:
    crc16 = cy_crc16
else:
    crc16 = py_crc16

if cy_key_slot:
    key_slot = cy_key_slot
else:
    key_slot = py_key_slot


class CrossSlotKeysError(Exception):
    pass


def determine_slot(first_key: bytes, *keys: bytes) -> int:
    slot: int = key_slot(first_key)
    for k in keys:
        if slot != key_slot(k):
            raise CrossSlotKeysError("all keys must map to the same key slot")

    return slot
