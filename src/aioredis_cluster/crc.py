from binascii import crc_hqx


__all__ = (
    "crc16",
    "key_slot",
)

REDIS_CLUSTER_HASH_SLOTS = 16384


def crc16(data: bytes) -> int:
    return crc_hqx(data, 0)


def key_slot(k: bytes, bucket: int = REDIS_CLUSTER_HASH_SLOTS) -> int:
    """Calculate key slot for a given key.

    :param key - bytes
    :param bucket - int
    """

    start = k.find(b"{")
    if start > -1:
        end = k.find(b"}", start + 1)
        if end > -1 and end != start + 1:
            k = k[start + 1 : end]
    return crc16(k) % bucket
