from binascii import crc_hqx


cdef int REDIS_CLUSTER_HASH_SLOTS = 16384


cpdef int crc16(bytes data):
    return crc_hqx(data, 0)


cpdef int key_slot(bytes k, int bucket = REDIS_CLUSTER_HASH_SLOTS):
    """Calculate key slot for a given key.

    :param key - bytes
    :param bucket - int
    """

    cdef int start, end

    start = k.find(b"{")
    if start > -1:
        end = k.find(b"}", start + 1)
        if end > -1 and end != start + 1:
            k = k[start + 1 : end]
    return crc16(k) % bucket


cpdef int find_slot(list slots, int slot):
    cdef object cl_slot
    cdef int lo = 0
    cdef int mid
    cdef int hi = len(slots)
    # binary search
    while lo < hi:
        mid = (lo + hi) // 2
        cl_slot = slots[mid]

        if cl_slot.end < slot:
            lo = mid + 1
        else:
            hi = mid

    if lo >= len(slots):
        return -1

    cl_slot = slots[lo]
    if not (cl_slot.begin <= slot <= cl_slot.end):
    # if not cl_slot.in_range(slot):
        return -1

    return lo
