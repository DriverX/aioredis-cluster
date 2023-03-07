aioredis_cluster
================

[![PyPI version](https://img.shields.io/pypi/v/aioredis-cluster)](https://pypi.org/project/aioredis-cluster/) ![aioredis-cluster CI/CD](https://github.com/DriverX/aioredis-cluster/workflows/aioredis-cluster%20CI/CD/badge.svg)

Redis Cluster support for [aioredis](https://github.com/aio-libs/aioredis) (support only v1.x.x).

Many implementation features were inspired by [go-redis](https://github.com/go-redis/redis) project.

Requirements
------------

* [Python](https://www.python.org) 3.8+
* [async_timeout](https://pypi.org/project/async_timeout/)

Features
--------

* commands execute failover (retry command on other node in cluster)
* support resharding replies ASK/MOVED
* restore cluster state from alive nodes
* one node is enough to know the topology and initialize client
* cluster state auto reload

Limitations
-----------

### Commands with limitations

* Keys in `mget`/`mset` must provide one key slot.
  ```python

  # works
  await redis.mget("key1:{foo}", "key2:{foo}")

  # throw RedisClusterError
  await redis.mget("key1", "key2")

  ```

### Commands are not supported

`Redis` methods below do not works and not supported in cluster mode implementation.
```
cluster_add_slots
cluster_count_failure_reports
cluster_count_key_in_slots
cluster_del_slots
cluster_failover
cluster_forget
cluster_get_keys_in_slots
cluster_meet
cluster_replicate
cluster_reset
cluster_save_config
cluster_set_config_epoch
cluster_setslot
cluster_readonly
cluster_readwrite
client_setname
shutdown
slaveof
script_kill
move
select
flushall
flushdb
script_load
script_flush
script_exists
scan
iscan
quit
swapdb
migrate
migrate_keys
wait
bgrewriteaof
bgsave
config_rewrite
config_set
config_resetstat
save
sync
pipeline
multi_exec
```

But you can always execute command you need on concrete node on cluster with usual `aioredis.RedisConnection`, `aioredis.ConnectionsPool` or high-level `aioredis.Redis` interfaces.


Installation
------------

```bash

pip install aioredis-cluster

```

Usage
-----

```python

import aioredis_cluster

redis = await aioredis_cluster.create_redis_cluster([
    "redis://redis-cluster-node1",
])

# or
redis = await aioredis_cluster.create_redis_cluster([
    "redis://redis-cluster-node1",
    "redis://redis-cluster-node2",
    "redis://redis-cluster-node3",
])

# or
redis = await aioredis_cluster.create_redis_cluster([
    ("redis-cluster-node1", 6379),
])

await redis.set("key", "value", expire=180)

redis.close()
await redis.wait_closed()

```

License
-------

The aioredis_cluster is offered under MIT license.
