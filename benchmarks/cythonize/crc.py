from aioredis_cluster.cluster_state import py_find_slot
from aioredis_cluster.crc import py_key_slot
from aioredis_cluster.speedup.crc import find_slot as cy_find_slot
from aioredis_cluster.speedup.crc import key_slot as cy_key_slot
from aioredis_cluster.structs import Address, ClusterNode, ClusterSlot

from . import run_bench

ds = [
    b"queue:queue_shard1",
    b"push_inbox:{75347c19-5cf4-4919-8247-268e63487908}",
    b"audios_durations:{75347c19-5cf4-4919-8247-268e63487908}",
    b"prefs:device:{b1a04957-26fd-5915-9563-96b39f177cf9}:blah/foobar/some_user_agent/pinocchio/f8b549e22de64f4da47c518822205f62/coll/general_options",
]

_master_node = ClusterNode(Address("127.0.0.1", 6379), "node_id")
slots = [
    ClusterSlot(begin=begin_slot, end=begin_slot + 128, master=_master_node)
    for begin_slot in range(0, 16384, 128)
]
slots_to_find = [11563, 415, 15489, 7489, 14956, 9272, 8442, 9858, 8256, 6154]


def run_py_key_slot():
    for key in ds:
        py_key_slot(key)


def run_cy_key_slot():
    for key in ds:
        cy_key_slot(key)


def run_py_find_slot():
    for slot in slots_to_find:
        assert py_find_slot(slots, slot) != -1


def run_cy_find_slot():
    for slot in slots_to_find:
        assert cy_find_slot(slots, slot) != -1


def main():
    print(run_bench(run_py_key_slot))
    print(run_bench(run_cy_key_slot))
    print(run_bench(run_py_find_slot))
    print(run_bench(run_cy_find_slot))


if __name__ == "__main__":
    main()
