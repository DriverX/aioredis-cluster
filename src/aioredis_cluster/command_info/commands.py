# redis command output (v5.0.8)
import enum
from typing import Dict, FrozenSet, Iterable, Mapping, MutableSet, Union, cast

__all__ = (
    "COMMANDS",
    "BLOCKING_COMMANDS",
    "EVAL_COMMANDS",
    "ZUNION_COMMANDS",
    "ZUNIONSTORE_COMMANDS",
    "XREAD_COMMAND",
    "XREADGROUP_COMMAND",
    "PUBSUB_COMMANDS",
    "PATTERN_PUBSUB_COMMANDS",
    "SHARDED_PUBSUB_COMMANDS",
    "PUBSUB_FAMILY_COMMANDS",
    "PING_COMMANDS",
    "PUBSUB_SUBSCRIBE_COMMANDS",
    "PUBSUB_COMMAND_TO_TYPE",
    "PUBSUB_RESP_KIND_TO_TYPE",
    "PubSubType",
)


def _gen_commands_set(commands: Iterable[str]) -> FrozenSet[Union[bytes, str]]:
    cmd_set: MutableSet[Union[bytes, str]] = set()
    for cmd in commands:
        cmd = cmd.upper()
        cmd_set.add(cmd)
        cmd_set.add(cmd.encode("latin1"))
    return frozenset(cmd_set)


COMMANDS = [
    ["xclaim", -6, ["write", "random", "fast"], 1, 1, 1],
    ["dump", 2, ["readonly", "random"], 1, 1, 1],
    ["pttl", 2, ["readonly", "random", "fast"], 1, 1, 1],
    ["decrby", 3, ["write", "denyoom", "fast"], 1, 1, 1],
    ["zrem", -3, ["write", "fast"], 1, 1, 1],
    ["renamenx", 3, ["write", "fast"], 1, 2, 1],
    ["getrange", 4, ["readonly"], 1, 1, 1],
    ["rename", 3, ["write"], 1, 2, 1],
    ["zrangebylex", -4, ["readonly"], 1, 1, 1],
    ["ltrim", 4, ["write"], 1, 1, 1],
    ["del", -2, ["write"], 1, -1, 1],
    ["zscore", 3, ["readonly", "fast"], 1, 1, 1],
    ["slaveof", 3, ["admin", "noscript", "stale"], 0, 0, 0],
    ["zrevrangebylex", -4, ["readonly"], 1, 1, 1],
    ["save", 1, ["admin", "noscript"], 0, 0, 0],
    ["memory", -2, ["readonly", "random"], 0, 0, 0],
    ["restore-asking", -4, ["write", "denyoom", "asking"], 1, 1, 1],
    ["bgrewriteaof", 1, ["admin", "noscript"], 0, 0, 0],
    ["bitpos", -3, ["readonly"], 1, 1, 1],
    ["bitop", -4, ["write", "denyoom"], 2, -1, 1],
    ["evalsha", -3, ["noscript", "movablekeys"], 0, 0, 0],
    ["hmset", -4, ["write", "denyoom", "fast"], 1, 1, 1],
    ["asking", 1, ["fast"], 0, 0, 0],
    ["rpushx", -3, ["write", "denyoom", "fast"], 1, 1, 1],
    ["sunionstore", -3, ["write", "denyoom"], 1, -1, 1],
    ["hkeys", 2, ["readonly", "sort_for_script"], 1, 1, 1],
    ["swapdb", 3, ["write", "fast"], 0, 0, 0],
    ["setbit", 4, ["write", "denyoom"], 1, 1, 1],
    ["migrate", -6, ["write", "random", "movablekeys"], 0, 0, 0],
    ["flushdb", -1, ["write"], 0, 0, 0],
    ["post", -1, ["loading", "stale"], 0, 0, 0],
    ["incr", 2, ["write", "denyoom", "fast"], 1, 1, 1],
    ["lindex", 3, ["readonly"], 1, 1, 1],
    ["scan", -2, ["readonly", "random"], 0, 0, 0],
    ["xrange", -4, ["readonly"], 1, 1, 1],
    ["zrangebyscore", -4, ["readonly"], 1, 1, 1],
    ["getbit", 3, ["readonly", "fast"], 1, 1, 1],
    ["setnx", 3, ["write", "denyoom", "fast"], 1, 1, 1],
    ["msetnx", -3, ["write", "denyoom"], 1, -1, 2],
    ["georadiusbymember_ro", -5, ["readonly", "movablekeys"], 1, 1, 1],
    ["unwatch", 1, ["noscript", "fast"], 0, 0, 0],
    ["psubscribe", -2, ["pubsub", "noscript", "loading", "stale"], 0, 0, 0],
    ["exists", -2, ["readonly", "fast"], 1, -1, 1],
    ["srandmember", -2, ["readonly", "random"], 1, 1, 1],
    ["hgetall", 2, ["readonly", "random"], 1, 1, 1],
    ["randomkey", 1, ["readonly", "random"], 0, 0, 0],
    ["hincrby", 4, ["write", "denyoom", "fast"], 1, 1, 1],
    ["subscribe", -2, ["pubsub", "noscript", "loading", "stale"], 0, 0, 0],
    ["host:", -1, ["loading", "stale"], 0, 0, 0],
    ["auth", 2, ["noscript", "loading", "stale", "fast"], 0, 0, 0],
    ["zrevrange", -4, ["readonly"], 1, 1, 1],
    ["zadd", -4, ["write", "denyoom", "fast"], 1, 1, 1],
    ["rpoplpush", 3, ["write", "denyoom"], 1, 2, 1],
    ["sort", -2, ["write", "denyoom", "movablekeys"], 1, 1, 1],
    ["echo", 2, ["fast"], 0, 0, 0],
    ["debug", -2, ["admin", "noscript"], 0, 0, 0],
    ["sismember", 3, ["readonly", "fast"], 1, 1, 1],
    ["decr", 2, ["write", "denyoom", "fast"], 1, 1, 1],
    ["rpush", -3, ["write", "denyoom", "fast"], 1, 1, 1],
    ["config", -2, ["admin", "noscript", "loading", "stale"], 0, 0, 0],
    ["zlexcount", 4, ["readonly", "fast"], 1, 1, 1],
    ["wait", 3, ["noscript"], 0, 0, 0],
    ["incrbyfloat", 3, ["write", "denyoom", "fast"], 1, 1, 1],
    ["bitcount", -2, ["readonly"], 1, 1, 1],
    ["setrange", 4, ["write", "denyoom"], 1, 1, 1],
    ["cluster", -2, ["admin"], 0, 0, 0],
    ["replicaof", 3, ["admin", "noscript", "stale"], 0, 0, 0],
    ["hdel", -3, ["write", "fast"], 1, 1, 1],
    ["hsetnx", 4, ["write", "denyoom", "fast"], 1, 1, 1],
    ["zincrby", 4, ["write", "denyoom", "fast"], 1, 1, 1],
    ["monitor", 1, ["admin", "noscript"], 0, 0, 0],
    ["psetex", 4, ["write", "denyoom"], 1, 1, 1],
    ["substr", 4, ["readonly"], 1, 1, 1],
    ["georadius_ro", -6, ["readonly", "movablekeys"], 1, 1, 1],
    ["hvals", 2, ["readonly", "sort_for_script"], 1, 1, 1],
    ["geohash", -2, ["readonly"], 1, 1, 1],
    ["zcard", 2, ["readonly", "fast"], 1, 1, 1],
    ["zrevrangebyscore", -4, ["readonly"], 1, 1, 1],
    ["pfcount", -2, ["readonly"], 1, -1, 1],
    ["object", -2, ["readonly", "random"], 2, 2, 1],
    ["xsetid", 3, ["write", "denyoom", "fast"], 1, 1, 1],
    ["unsubscribe", -1, ["pubsub", "noscript", "loading", "stale"], 0, 0, 0],
    ["geodist", -4, ["readonly"], 1, 1, 1],
    ["sdiff", -2, ["readonly", "sort_for_script"], 1, -1, 1],
    ["replconf", -1, ["admin", "noscript", "loading", "stale"], 0, 0, 0],
    ["readonly", 1, ["fast"], 0, 0, 0],
    ["zrank", 3, ["readonly", "fast"], 1, 1, 1],
    ["zrevrank", 3, ["readonly", "fast"], 1, 1, 1],
    ["hstrlen", 3, ["readonly", "fast"], 1, 1, 1],
    ["sunion", -2, ["readonly", "sort_for_script"], 1, -1, 1],
    ["readwrite", 1, ["fast"], 0, 0, 0],
    ["hincrbyfloat", 4, ["write", "denyoom", "fast"], 1, 1, 1],
    ["sinter", -2, ["readonly", "sort_for_script"], 1, -1, 1],
    ["getset", 3, ["write", "denyoom"], 1, 1, 1],
    ["exec", 1, ["noscript", "skip_monitor"], 0, 0, 0],
    ["time", 1, ["random", "fast"], 0, 0, 0],
    ["hset", -4, ["write", "denyoom", "fast"], 1, 1, 1],
    ["lpush", -3, ["write", "denyoom", "fast"], 1, 1, 1],
    ["discard", 1, ["noscript", "fast"], 0, 0, 0],
    ["watch", -2, ["noscript", "fast"], 1, -1, 1],
    ["append", 3, ["write", "denyoom"], 1, 1, 1],
    ["keys", 2, ["readonly", "sort_for_script"], 0, 0, 0],
    ["zcount", 4, ["readonly", "fast"], 1, 1, 1],
    ["publish", 3, ["pubsub", "loading", "stale", "fast"], 0, 0, 0],
    ["role", 1, ["noscript", "loading", "stale"], 0, 0, 0],
    ["move", 3, ["write", "fast"], 1, 1, 1],
    ["pfselftest", 1, ["admin"], 0, 0, 0],
    ["georadius", -6, ["write", "movablekeys"], 1, 1, 1],
    ["pexpire", 3, ["write", "fast"], 1, 1, 1],
    ["strlen", 2, ["readonly", "fast"], 1, 1, 1],
    ["type", 2, ["readonly", "fast"], 1, 1, 1],
    ["pfmerge", -2, ["write", "denyoom"], 1, -1, 1],
    ["sinterstore", -3, ["write", "denyoom"], 1, -1, 1],
    ["dbsize", 1, ["readonly", "fast"], 0, 0, 0],
    ["set", -3, ["write", "denyoom"], 1, 1, 1],
    ["llen", 2, ["readonly", "fast"], 1, 1, 1],
    ["punsubscribe", -1, ["pubsub", "noscript", "loading", "stale"], 0, 0, 0],
    ["xadd", -5, ["write", "denyoom", "random", "fast"], 1, 1, 1],
    ["slowlog", -2, ["admin", "random"], 0, 0, 0],
    ["sscan", -3, ["readonly", "random"], 1, 1, 1],
    ["mget", -2, ["readonly", "fast"], 1, -1, 1],
    ["bzpopmax", -3, ["write", "noscript", "fast"], 1, -2, 1],
    ["persist", 2, ["write", "fast"], 1, 1, 1],
    ["pfdebug", -3, ["write"], 0, 0, 0],
    ["lpushx", -3, ["write", "denyoom", "fast"], 1, 1, 1],
    ["srem", -3, ["write", "fast"], 1, 1, 1],
    ["xpending", -3, ["readonly", "random"], 1, 1, 1],
    ["blpop", -3, ["write", "noscript"], 1, -2, 1],
    ["xrevrange", -4, ["readonly"], 1, 1, 1],
    ["xreadgroup", -7, ["write", "noscript", "movablekeys"], 1, 1, 1],
    ["georadiusbymember", -5, ["write", "movablekeys"], 1, 1, 1],
    ["smembers", 2, ["readonly", "sort_for_script"], 1, 1, 1],
    ["setex", 4, ["write", "denyoom"], 1, 1, 1],
    ["module", -2, ["admin", "noscript"], 0, 0, 0],
    ["zrange", -4, ["readonly"], 1, 1, 1],
    ["restore", -4, ["write", "denyoom"], 1, 1, 1],
    ["unlink", -2, ["write", "fast"], 1, -1, 1],
    ["lset", 4, ["write", "denyoom"], 1, 1, 1],
    ["hexists", 3, ["readonly", "fast"], 1, 1, 1],
    ["spop", -2, ["write", "random", "fast"], 1, 1, 1],
    ["xgroup", -2, ["write", "denyoom"], 2, 2, 1],
    ["bitfield", -2, ["write", "denyoom"], 1, 1, 1],
    ["multi", 1, ["noscript", "fast"], 0, 0, 0],
    ["xinfo", -2, ["readonly", "random"], 2, 2, 1],
    ["xack", -4, ["write", "fast"], 1, 1, 1],
    ["lrem", 4, ["write"], 1, 1, 1],
    ["zpopmax", -2, ["write", "fast"], 1, 1, 1],
    ["psync", 3, ["readonly", "admin", "noscript"], 0, 0, 0],
    ["xdel", -3, ["write", "fast"], 1, 1, 1],
    ["hget", 3, ["readonly", "fast"], 1, 1, 1],
    ["brpop", -3, ["write", "noscript"], 1, -2, 1],
    ["ping", -1, ["stale", "fast"], 0, 0, 0],
    ["client", -2, ["admin", "noscript"], 0, 0, 0],
    ["select", 2, ["loading", "fast"], 0, 0, 0],
    ["expireat", 3, ["write", "fast"], 1, 1, 1],
    ["sadd", -3, ["write", "denyoom", "fast"], 1, 1, 1],
    ["info", -1, ["random", "loading", "stale"], 0, 0, 0],
    ["lrange", 4, ["readonly"], 1, 1, 1],
    ["bzpopmin", -3, ["write", "noscript", "fast"], 1, -2, 1],
    ["flushall", -1, ["write"], 0, 0, 0],
    ["zremrangebyrank", 4, ["write"], 1, 1, 1],
    ["sdiffstore", -3, ["write", "denyoom"], 1, -1, 1],
    ["zinterstore", -4, ["write", "denyoom", "movablekeys"], 0, 0, 0],
    ["ttl", 2, ["readonly", "random", "fast"], 1, 1, 1],
    ["lolwut", -1, ["readonly"], 0, 0, 0],
    ["pubsub", -2, ["pubsub", "random", "loading", "stale"], 0, 0, 0],
    ["lastsave", 1, ["random", "fast"], 0, 0, 0],
    ["smove", 4, ["write", "fast"], 1, 2, 1],
    ["hlen", 2, ["readonly", "fast"], 1, 1, 1],
    ["hscan", -3, ["readonly", "random"], 1, 1, 1],
    ["geopos", -2, ["readonly"], 1, 1, 1],
    ["latency", -2, ["admin", "noscript", "loading", "stale"], 0, 0, 0],
    ["geoadd", -5, ["write", "denyoom"], 1, 1, 1],
    ["hmget", -3, ["readonly", "fast"], 1, 1, 1],
    ["get", 2, ["readonly", "fast"], 1, 1, 1],
    ["expire", 3, ["write", "fast"], 1, 1, 1],
    ["shutdown", -1, ["admin", "noscript", "loading", "stale"], 0, 0, 0],
    ["zscan", -3, ["readonly", "random"], 1, 1, 1],
    ["linsert", 5, ["write", "denyoom"], 1, 1, 1],
    ["xlen", 2, ["readonly", "fast"], 1, 1, 1],
    ["command", 0, ["random", "loading", "stale"], 0, 0, 0],
    ["zpopmin", -2, ["write", "fast"], 1, 1, 1],
    ["rpop", 2, ["write", "fast"], 1, 1, 1],
    ["mset", -3, ["write", "denyoom"], 1, -1, 2],
    ["xread", -4, ["readonly", "noscript", "movablekeys"], 1, 1, 1],
    ["eval", -3, ["noscript", "movablekeys"], 0, 0, 0],
    ["brpoplpush", 4, ["write", "denyoom", "noscript"], 1, 2, 1],
    ["touch", -2, ["readonly", "fast"], 1, 1, 1],
    ["zremrangebylex", 4, ["write"], 1, 1, 1],
    ["incrby", 3, ["write", "denyoom", "fast"], 1, 1, 1],
    ["script", -2, ["noscript"], 0, 0, 0],
    ["pfadd", -2, ["write", "denyoom", "fast"], 1, 1, 1],
    ["xtrim", -2, ["write", "random", "fast"], 1, 1, 1],
    ["lpop", 2, ["write", "fast"], 1, 1, 1],
    ["scard", 2, ["readonly", "fast"], 1, 1, 1],
    ["bgsave", -1, ["admin", "noscript"], 0, 0, 0],
    ["sync", 1, ["readonly", "admin", "noscript"], 0, 0, 0],
    ["pexpireat", 3, ["write", "fast"], 1, 1, 1],
    ["zunionstore", -4, ["write", "denyoom", "movablekeys"], 0, 0, 0],
    ["zremrangebyscore", 4, ["write"], 1, 1, 1],
]


BLOCKING_COMMANDS = _gen_commands_set(
    {
        "BLPOP",
        "BRPOP",
        "BRPOPLPUSH",
        "BLMOVE",
        "BLMPOP",
        "BZPOPMIN",
        "BZPOPMAX",
        "XREAD",
        "XREADGROUP",
    }
)

EVAL_COMMANDS = _gen_commands_set(
    {
        "EVAL",
        "EVALSHA",
    }
)

ZUNION_COMMANDS = _gen_commands_set(
    {
        "ZUNION",
        "ZINTER",
        "ZDIFF",
    }
)

ZUNIONSTORE_COMMANDS = _gen_commands_set(
    {
        "ZUNIONSTORE",
        "ZINTERSTORE",
        "ZDIFFSTORE",
    }
)

XREAD_COMMAND = "XREAD"
XREADGROUP_COMMAND = "XREADGROUP"


class PubSubType(enum.Enum):
    CHANNEL = enum.auto()
    PATTERN = enum.auto()
    SHARDED = enum.auto()


PUBSUB_COMMANDS = _gen_commands_set({"SUBSCRIBE", "UNSUBSCRIBE"})

PATTERN_PUBSUB_COMMANDS = _gen_commands_set(
    {
        "PSUBSCRIBE",
        "PUNSUBSCRIBE",
    }
)

SHARDED_PUBSUB_COMMANDS = _gen_commands_set(
    {
        "SSUBSCRIBE",
        "SUNSUBSCRIBE",
    }
)

PUBSUB_SUBSCRIBE_COMMANDS = _gen_commands_set(
    {
        "SUBSCRIBE",
        "PSUBSCRIBE",
        "SSUBSCRIBE",
    }
)

PUBSUB_FAMILY_COMMANDS = PUBSUB_COMMANDS | PATTERN_PUBSUB_COMMANDS | SHARDED_PUBSUB_COMMANDS

PING_COMMANDS = _gen_commands_set({"PING"})

PUBSUB_RESP_KIND_TO_TYPE: Mapping[bytes, PubSubType] = {
    b"message": PubSubType.CHANNEL,
    b"subscribe": PubSubType.CHANNEL,
    b"unsubscribe": PubSubType.CHANNEL,
    b"pmessage": PubSubType.PATTERN,
    b"psubscribe": PubSubType.PATTERN,
    b"punsubscribe": PubSubType.PATTERN,
    b"smessage": PubSubType.SHARDED,
    b"ssubscribe": PubSubType.SHARDED,
    b"sunsubscribe": PubSubType.SHARDED,
}

_pubsub_command_to_type: Dict[Union[str, bytes], PubSubType] = {}
for cmd in PUBSUB_COMMANDS:
    _pubsub_command_to_type[cmd] = PubSubType.CHANNEL
for cmd in PATTERN_PUBSUB_COMMANDS:
    _pubsub_command_to_type[cmd] = PubSubType.PATTERN
for cmd in SHARDED_PUBSUB_COMMANDS:
    _pubsub_command_to_type[cmd] = PubSubType.SHARDED
PUBSUB_COMMAND_TO_TYPE = cast(Mapping[Union[str, bytes], PubSubType], _pubsub_command_to_type)
