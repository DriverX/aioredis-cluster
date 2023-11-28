import asyncio
import random

import pytest

from aioredis_cluster import ClusterClosedError, RedisClusterError


async def test_cluster_close(cluster):
    event_loop = asyncio.get_running_loop()
    cl = await cluster()

    await cl.execute("set", "key", "value")

    pool = await cl.keys_master("key")
    await pool.client_pause(100)

    get_task = event_loop.create_task(cl.execute("get", "key"))

    await asyncio.sleep(0.05)

    cl.close()
    await cl.wait_closed()

    assert get_task.done()
    with pytest.raises(ClusterClosedError):
        get_task.result()

    with pytest.raises(ClusterClosedError):
        await cl.execute("PING")


async def test_cluster_keys_master(cluster):
    cl = await cluster()

    pool = await cl.keys_master("foo")
    await pool.set("foo", "TEST")
    val = await cl.execute("GET", "foo")

    assert val == b"TEST"

    with pytest.raises(RedisClusterError, match="all keys must map"):
        await cl.keys_master("foo", "bar", "baz")


async def test_cluster_all_masters(cluster):
    cl = await cluster()

    pools = await cl.all_masters()

    results = []
    for p in pools:
        info = await p.info()
        results.append((info["server"]["run_id"], info["replication"]["role"]))

    assert len(set(r[0] for r in results)) == 3
    assert [r[1] for r in results] == ["master"] * 3


async def test_cluster__info_commands(cluster):
    cl = await cluster()

    pools = await cl.all_masters()

    pool = random.choice(pools)
    cluster_info = await pool.cluster_info()

    assert cluster_info["cluster_size"] == "3"
    assert cluster_info["cluster_state"] == "ok"
    assert cluster_info["cluster_slots_ok"] == "16384"

    pool = random.choice(pools)
    cluster_nodes = await pool.cluster_nodes()

    assert len(cluster_nodes) == 6
    assert len([n for n in cluster_nodes if "master" in n["flags"]]) == 3
    assert len([n for n in cluster_nodes if "slave" in n["flags"]]) == 3

    master_ids = [n["id"] for n in cluster_nodes if "master" in n["flags"]]

    cluster_slaves = []
    pool = random.choice(pools)
    cluster_slaves.append(await pool.cluster_slaves(master_ids[0]))
    pool = random.choice(pools)
    cluster_slaves.append(await pool.cluster_slaves(master_ids[1]))
    pool = random.choice(pools)
    cluster_slaves.append(await pool.cluster_slaves(master_ids[2]))

    assert sum(len(nodes) for nodes in cluster_slaves) == 3
