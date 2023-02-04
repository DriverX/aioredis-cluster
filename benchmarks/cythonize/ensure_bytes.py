from aioredis_cluster.speedup.ensure_bytes import encode_command as cy_encode_command
from aioredis_cluster.speedup.ensure_bytes import (
    iter_ensure_bytes as cy_iter_ensure_bytes,
)
from aioredis_cluster.util import py_encode_command, py_iter_ensure_bytes

from . import run_bench

ds = [
    (
        b"XADD",
        "queue:queue_shard1",
        b"MAXLEN",
        b"~",
        10000,
        b"*",
        "json",
        b'{"request_id":"75347c19-5cf4-4919-8247-268e63487908","chunk_num":42,"user_id":"blahblah_:user:uid_d5e1d7efd4de4f7ca36c407e61f2aab8","session_id":"f4a9ae3f-6f1d-4fc3-bcf6-fbb191418c0d","source":"frontent","deadline":1675539675,"last":false,"routing":{"exchange":null,"redis_stream_name":"extra_backend_queue","redis_stream_shards_num":1},"apply_foo":true,"apply_bar":true,"foo_config_name":null,"foo_routes":{"exchange":null,"beer_enabled":true,"beer_threshold":0.24,"vodka_threshold":0.24,"max_num_shots":0,"redis_stream_name":"foo_queue","redis_stream_shards_num":1}}',
    ),
    (
        b"ZREMRANGEBYSCORE",
        "push_inbox:{75347c19-5cf4-4919-8247-268e63487908}",
        float("-inf"),
        1674934871.5091486,
    ),
    (b"LRANGE", "audios_durations:{75347c19-5cf4-4919-8247-268e63487908}", -1, -1),
]


def run_py_iter_ensure_bytes():
    for args in ds:
        list(py_iter_ensure_bytes(args))


def run_cy_iter_ensure_bytes():
    for args in ds:
        list(cy_iter_ensure_bytes(args))


def run_py_encode_command():
    for args in ds:
        py_encode_command(*args)


def run_cy_encode_command():
    for args in ds:
        cy_encode_command(*args)


def main():
    print(run_bench(run_py_iter_ensure_bytes))
    print(run_bench(run_cy_iter_ensure_bytes))
    print(run_bench(run_py_encode_command))
    print(run_bench(run_cy_encode_command))


if __name__ == "__main__":
    main()
