from functools import lru_cache

import pytest

from aioredis_cluster.errors import ClusterStateError, UncoveredSlotError
from aioredis_cluster.manager import ClusterState, create_cluster_state
from aioredis_cluster.structs import Address

from ._cluster_slots import SLOTS


@lru_cache(None)
def get_state():
    return create_cluster_state(SLOTS)


def get_nodes_addr(nodes):
    addrs = []
    for node in nodes:
        addrs.append(node.addr)

    return sorted(addrs)


def get_slots_ranges(slots):
    ranges = []
    for slot in slots:
        ranges.append((slot.begin, slot.end))

    return sorted(ranges)


def test_create_cluster_state():
    state = create_cluster_state(SLOTS)

    addrs = sorted([Address("172.17.0.2", port) for port in range(7000, 7005)])
    masters_addrs = sorted(
        [
            Address("172.17.0.2", 7000),
            Address("172.17.0.2", 7001),
            Address("172.17.0.2", 7002),
        ]
    )
    replicas_addrs = sorted(
        [
            Address("172.17.0.2", 7003),
            Address("172.17.0.2", 7004),
        ]
    )
    slot_ranges = sorted(
        [
            (0, 5460),
            (9995, 9995),
            (12182, 12182),
            (5461, 9994),
            (9996, 10922),
            (10923, 12181),
            (12183, 16383),
        ]
    )

    assert len(state.nodes) == 5
    assert len(state.addrs) == 5
    assert len(set(state.addrs)) == 5
    assert len(state.masters) == 3
    assert len(state.replicas) == 2
    assert len(state.slots) == 7
    assert sorted(state.addrs) == addrs
    assert sorted(state.nodes.keys()) == addrs
    assert get_nodes_addr(state.masters) == masters_addrs
    assert get_nodes_addr(state.replicas) == replicas_addrs
    assert get_slots_ranges(state.slots) == slot_ranges


def test_find_slot__empty_state():
    state = ClusterState()

    with pytest.raises(UncoveredSlotError):
        state.find_slot(0)

    with pytest.raises(UncoveredSlotError):
        state.find_slot(16384)


@pytest.mark.parametrize(
    "slot, expect",
    [
        (0, (0, 5460)),
        (5400, (0, 5460)),
        (8000, (5461, 9994)),
        (12182, (12182, 12182)),
        (12183, (12183, 16383)),
        (16383, (12183, 16383)),
        (16384, UncoveredSlotError(16384)),
    ],
)
def test_find_slot(slot, expect):
    state = get_state()

    if isinstance(expect, Exception):
        with pytest.raises(type(expect)):
            state.find_slot(slot)
    else:
        result = state.find_slot(slot)
        assert (result.begin, result.end) == expect


def test_slot_master__empty_state():
    state = ClusterState()

    with pytest.raises(UncoveredSlotError):
        state.slot_master(0)


@pytest.mark.parametrize(
    "slot, expect",
    [
        (0, "172.17.0.2:7000"),
        (5400, "172.17.0.2:7000"),
        (8000, "172.17.0.2:7001"),
        (12182, "172.17.0.2:7000"),
        (12183, "172.17.0.2:7002"),
        (16383, "172.17.0.2:7002"),
        (16384, UncoveredSlotError(16384)),
    ],
)
def test_slot_master(slot, expect):
    state = get_state()

    if isinstance(expect, Exception):
        with pytest.raises(type(expect)):
            state.slot_master(slot)
    else:
        result = state.slot_master(slot)
        assert str(result.addr) == expect


def test_random_master__empty_state():
    state = ClusterState()

    with pytest.raises(ClusterStateError):
        state.random_master()


def test_random_master(mocker):
    state = get_state()

    mocker.patch(state.__module__ + ".random.choice", side_effect=lambda s: s[1])
    result = state.random_master()

    assert result is state.masters[1]


def test_random_node__empty_state():
    state = ClusterState()

    with pytest.raises(ClusterStateError):
        state.random_node()


def test_random_node(mocker):
    state = get_state()

    mocker.patch(state.__module__ + ".random.choice", side_effect=lambda s: s[1])
    result = state.random_node()

    assert result is state.nodes[state.addrs[1]]
