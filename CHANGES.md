Changes
=======

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
