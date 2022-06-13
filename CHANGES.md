Changes
=======

2.0.0 (2022-xx-xx)
------------------

* include `aioredis==1.3.1` source code into `aioredis_cluster.aioredis`
* fix reacquire connection in `aioredic.ConnectionsPool` after Redis node failure
* this release have not backward incompatible changes

1.8.0 (2022-05-20)
------------------

* Add `xadd_620` commands method for support `XADD` options for Redis 6.2+

1.7.1 (2021-12-15)
------------------

* add `ClusterState.slots_assigned`
* require reload cluster state for some cases with `UncoveredSlotError`

1.7.0 (2021-12-15)
------------------

* add `execute_timeout` for `Manager`
* improve cluster state reload logging
* reduce number of addresses to fetch cluster state
* acquire dedicate connection from pool to fetch cluster state
* extend `ClusterState` by new attributes: `state`, `state_from`, `current_epoch`

1.6.1 (2021-11-23)
------------------

* fix keys extraction for `XREAD` and `XREADGROUP` commands

1.6.0 (2021-11-20)
------------------

* make public `Address`, `ClusterNode` and `ClusterState` structs. Available by import `from aioredis_cluster import`
* `Cluster` provides some new helpful methods:
    * `get_master_node_by_keys(*keys)` - return master `ClusterNode` which contains keys `keys`
    * `create_pool_by_addr(addr, **kwargs)` - create connection pool by `addr` and return pool wrapped by `commands_factory` from `Cluster` constructor. By default is `aioredis_cluster.RedisCluster` instance.
    * `get_cluster_state()` - return `ClusterState` instance with recent known cluster state received from Redis cluster
    * `extract_keys(command_sequence)` - returns keys of command sequence
* drop `pytest-aiohttp` plugin for tests
* add `pytest-asyncio` dependency for tests
* switch `asynctest` -> `mock` library for aio tests
* drop `attrs` dependency. For Python 3.6 you need install `dataclasses`
* fix extract keys for `BLPOP`/`BRPOP` commands
* add support keys extraction for `ZUNION`, `ZINTER`, `ZDIFF`, `ZUNIONSTORE`, `ZINTERSTORE`, `ZDIFFSTORE` commands
* acquire dedicate connection from pool for potential blocking commands like `BLPOP`, `BRPOP`, `BRPOPLPUSH`, `BLMOVE`, `BLMPOP`, `BZPOPMIN`, `BZPOPMAX`, `XREAD`, `XREADGROUP`

1.5.2 (2020-12-14)
------------------

* README update

1.5.1 (2020-12-11)
------------------

* speedup crc16. Use implementation from python stdlib

1.5.0 (2020-12-10)
------------------

* remove `state_reload_frequency` from `ClusterManager`. `state_reload_interval` now is one relevant option for state auto reload
* default `state_reload_interval` increased and now is 300 seconds (5 minutes)
* commands registry loads only once, on cluster state initialize
* improve failover. First connection problem cause retry to random slot replica
* improve python3.9 support
* default `idle_connection_timeout` now is 10 minutes

1.4.0 (2020-09-08)
------------------

* fix `aioredis.locks.Lock` issue (https://github.com/aio-libs/aioredis/pull/802, [bpo32734](https://bugs.python.org/issue32734))
* now `aioredis_cluster.Cluster` do not acquire dedicate connection for every execute
* `aioredis_cluster` now requires `python>=3.6.5`

1.3.0 (2019-10-23)
------------------

* improve compatible with Python 3.8
* improve failover logic while command timed out
* read-only commands now retries if attempt_timeout is reached
* add required dependeny `async_timeout`
* `aioredis` dependency bound now is `aioredis >=1.1.0, <2.0.0`

1.2.0 (2019-09-10)
------------------

* add timeout for command execution (per execution try)
* add Cluster option `attempt_timeout` for configure command execution timeout, default timeout is 5 seconds
* Cluster.execute_pubsub() fixes

1.1.1 (2019-06-07)
------------------

* CHANGES fix

1.1.0 (2019-06-06)
------------------

* Cluster state auto reload
* new `state_reload_frequency` option to configure state reload frequency
* new `state_reload_interval` option to configure state auto reload interval
* `follow_cluster` option enable load cluster state from previous cluster state nodes
* establish connection only for master nodes after cluster state load
* change default commands_factory to aioredis_cluster.RedisCluster instead aioredis.Redis
* all cluster info commands always returns structs with str, not bytes
* `keys_master` and `all_masters` methods now try to ensure cluster state instead simply raise exception if connection lost to cluster node, for example
* `max_attempts` always defaults fix

1.0.0 (2019-05-29)
------------------

* Library full rewrite
* Cluster state auto reload
* Command failover if cluster node is down or key slot resharded

0.2.0 (2018-12-27)
------------------

* Pipeline and MULTI/EXEC cluster implementation with keys distribution limitation (because cluster)

0.1.1 (2018-12-26)
------------------

* Python 3.6+ only

0.1.0 (2018-12-24)
------------------

* Initial release based on aioredis PR (https://github.com/aio-libs/aioredis/pull/119)
