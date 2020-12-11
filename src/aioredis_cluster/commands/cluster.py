from aioredis.util import _NOTSET, wait_convert, wait_ok

from aioredis_cluster.util import (
    parse_cluster_nodes,
    parse_cluster_slaves,
    parse_info,
)


class ClusterCommandsMixin:
    """Cluster commands mixin.

    For commands details see: http://redis.io/commands#cluster
    """

    def cluster_add_slots(self, slot, *slots):
        """Assign new hash slots to receiving node."""

        slots = set((slot,) + slots)
        if not all(isinstance(s, int) for s in slots):
            raise TypeError("All parameters must be of type int")

        fut = self.execute(b"CLUSTER", b"ADDSLOTS", *slots)
        return wait_ok(fut)

    def cluster_count_failure_reports(self, node_id):
        """Return the number of failure reports active for a given node."""

        return self.execute(b"CLUSTER", b"COUNT-FAILURE-REPORTS", node_id)

    def cluster_count_key_in_slots(self, slot):
        """Return the number of local keys in the specified hash slot."""

        return self.execute(b"CLUSTER", b"COUNTKEYSINSLOT", slot)

    def cluster_del_slots(self, slot, *slots):
        """Set hash slots as unbound in receiving node."""
        slots = set((slot,) + slots)
        if not all(isinstance(s, int) for s in slots):
            raise TypeError("All parameters must be of type int")

        fut = self.execute(b"CLUSTER", b"DELSLOTS", *slots)
        return wait_ok(fut)

    def cluster_failover(self, force=False):
        """
        Forces the slave to start a manual failover of its master instance.
        """
        command = force and b"FORCE" or b"TAKEOVER"

        fut = self.execute(b"CLUSTER", b"FAILOVER", command)
        return wait_ok(fut)

    def cluster_forget(self, node_id):
        """Remove a node from the nodes table."""
        fut = self.execute(b"CLUSTER", b"FORGET", node_id)
        return wait_ok(fut)

    def cluster_get_keys_in_slots(self, slot, count, *, encoding=_NOTSET):
        """Return local key names in the specified hash slot."""
        return self.execute(b"CLUSTER", b"GETKEYSINSLOT", slot, count, encoding=encoding)

    def cluster_info(self):
        """Provides info about Redis Cluster node state."""
        fut = self.execute(b"CLUSTER", b"INFO", encoding="utf-8")
        return wait_convert(fut, parse_info)

    def cluster_keyslot(self, key):
        """Returns the hash slot of the specified key."""
        return self.execute(b"CLUSTER", b"KEYSLOT", key)

    def cluster_meet(self, ip, port):
        """Force a node cluster to handshake with another node."""
        fut = self.execute(b"CLUSTER", b"MEET", ip, port)
        return wait_ok(fut)

    def cluster_nodes(self):
        """Get Cluster config for the node."""
        fut = self.execute(b"CLUSTER", b"NODES", encoding="utf-8")
        return wait_convert(fut, parse_cluster_nodes)

    def cluster_replicate(self, node_id):
        """Reconfigure a node as a slave of the specified master node."""
        fut = self.execute(b"CLUSTER", b"REPLICATE", node_id)
        return wait_ok(fut)

    def cluster_reset(self, *, hard=False):
        """Reset a Redis Cluster node."""
        reset = hard and b"HARD" or b"SOFT"
        fut = self.execute(b"CLUSTER", b"RESET", reset)
        return wait_ok(fut)

    def cluster_save_config(self):
        """Force the node to save cluster state on disk."""
        fut = self.execute(b"CLUSTER", b"SAVECONFIG")
        return wait_ok(fut)

    def cluster_set_config_epoch(self, config_epoch):
        """Set the configuration epoch in a new node."""

        try:
            config_epoch = int(config_epoch)
        except ValueError:
            raise TypeError("Expected slot to be of type int, got {}".format(type(config_epoch)))

        fut = self.execute(b"CLUSTER", b"SET-CONFIG-EPOCH", config_epoch)
        return wait_ok(fut)

    def cluster_setslot(self, slot, command, node_id=None):
        """Bind a hash slot to specified node."""

        raise NotImplementedError()

    def cluster_slaves(self, node_id):
        """List slave nodes of the specified master node."""
        fut = self.execute(b"CLUSTER", b"SLAVES", node_id, encoding="utf-8")
        return wait_convert(fut, parse_cluster_slaves)

    def cluster_slots(self):
        """Get array of Cluster slot to node mappings."""
        return self.execute(b"CLUSTER", b"SLOTS")

    def cluster_readonly(self):
        """
        Enables read queries for a connection to a Redis Cluster slave node.
        """
        fut = self.execute(b"READONLY")
        return wait_ok(fut)

    def cluster_readwrite(self):
        """
        Disables read queries for a connection to a Redis Cluster slave node.
        """
        fut = self.execute(b"READWRITE")
        return wait_ok(fut)
