_inf = float("inf")


cpdef bytes ensure_bytes(object obj):
    obj_type = type(obj)
    if obj_type is bytes:
        return obj
    if obj_type is bytearray:
        return bytes(obj)
    if obj_type is str:
        return obj.encode("utf-8")
    if obj_type is int:
        return b"%d" % obj
    if obj_type is float:
        if obj == _inf:
            return b"inf"
        if obj == -_inf:
            return b"-inf"
        return repr(obj).encode("ascii")
    raise TypeError(
        f"{obj!r} expected to be of bytearray, bytes, "
        " float, int, or str type"
    )


def iter_ensure_bytes(seq):
    for obj in seq:
        yield ensure_bytes(obj)


def ensure_str(obj):
    if isinstance(obj, str):
        return obj
    return obj.decode("utf-8")


cdef bytearray _encode_command(object args, bytearray buf = None):
    cdef bytes barg

    if buf is None:
        buf = bytearray()
    buf.extend(b"*%d\r\n" % len(args))
    for arg in args:
        barg = ensure_bytes(arg)
        buf.extend(b"$%d\r\n%s\r\n" % (len(barg), barg))
    return buf


def encode_command(*args, buf = None):
    return _encode_command(args, buf)
