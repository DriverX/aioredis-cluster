import dataclasses
import random
import socket
from asyncio.futures import _chain_future  # type: ignore
from typing import (
    Dict,
    Generator,
    Iterable,
    List,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

from aioredis.util import _converters, decode


__all__ = [
    "ensure_bytes",
    "iter_ensure_bytes",
    "ensure_str",
    "norm_command",
    "parse_info",
    "parse_node_slots",
    "parse_cluster_nodes",
    "chain_future",
    "RedirInfo",
    "parse_moved_response_error",
    "retry_backoff",
    "unused_port",
]

_T = TypeVar("_T")


def _decode(s, encoding=None):
    if encoding:
        return decode(s, encoding)
    return s


def ensure_bytes(obj) -> Union[bytes, bytearray]:
    obj_type = type(obj)
    if obj_type in _converters:
        return _converters[obj_type](obj)

    return bytes(obj)


def iter_ensure_bytes(seq: Iterable) -> Generator[Union[bytes, bytearray], None, None]:
    for obj in seq:
        yield ensure_bytes(obj)


def ensure_str(obj) -> str:
    if isinstance(obj, str):
        return obj
    return obj.decode("utf-8")


def norm_command(command: Union[bytes, str]) -> bytes:
    if isinstance(command, str):
        command = command.encode("utf-8")
    return command.upper()


def parse_info(info: str) -> Dict[str, str]:
    ret: Dict[str, str] = {}
    for line in info.strip().splitlines():
        key, value = line.split(":", 1)
        ret[key] = value
    return ret


def parse_node_slots(raw_slots: str) -> Tuple[Tuple, Tuple]:
    """
    @see: https://redis.io/commands/cluster-nodes#serialization-format
    @see: https://redis.io/commands/cluster-nodes#special-slot-entries
    """

    slots, migrations = [], []
    migration_delimiter = "->-"
    import_delimiter = "-<-"
    range_delimiter = "-"
    migrating_state = "migrating"
    importing_state = "importing"

    for r in raw_slots.strip().split():
        if migration_delimiter in r:
            slot_id, dst_node_id = r[1:-1].split(migration_delimiter, 1)
            migrations.append(
                {"slot": int(slot_id), "node_id": dst_node_id, "state": migrating_state}
            )
        elif import_delimiter in r:
            slot_id, src_node_id = r[1:-1].split(import_delimiter, 1)
            migrations.append(
                {"slot": int(slot_id), "node_id": src_node_id, "state": importing_state}
            )
        elif range_delimiter in r:
            start, end = r.split(range_delimiter)
            slots.append((int(start), int(end)))
        else:
            slots.append((int(r), int(r)))

    return tuple(slots), tuple(migrations)


def parse_cluster_node_line(line: str) -> Dict:
    parts = line.split(None, 8)
    self_id, addr, flags, master_id, ping_sent, pong_recv, config_epoch, link_state = parts[:8]

    host, port = addr.rsplit(":", 1)
    nat_port = None

    if "@" in port:
        # Since version 4.0.0 address_node_info has the format
        # '192.1.2.3:7001@17001
        at_index = port.index("@")
        nat_port = int(port[at_index + 1 :])
        port = port[:at_index]

    node = {
        "id": self_id,
        "host": host,
        "port": int(port),
        "nat-port": nat_port,
        "flags": tuple(flags.split(",")),
        "master": master_id if master_id != "-" else None,
        "ping-sent": int(ping_sent),
        "pong-recv": int(pong_recv),
        "config_epoch": int(config_epoch),
        "status": link_state,
        "slots": tuple(),
        "migrations": tuple(),
    }

    if len(parts) >= 9:
        slots, migrations = parse_node_slots(parts[8])
        node["slots"], node["migrations"] = slots, migrations

    return node


def parse_cluster_slaves(lines: Sequence[str]) -> List[Dict]:
    return [parse_cluster_node_line(line) for line in lines]


def parse_cluster_nodes(resp: str) -> List[Dict]:
    """
    @see: https://redis.io/commands/cluster-nodes # list of string
    """

    return [parse_cluster_node_line(line) for line in resp.strip().splitlines()]


@dataclasses.dataclass
class RedirInfo:
    slot_id: int
    host: str
    port: int
    ask: bool


def parse_moved_response_error(msg: str) -> RedirInfo:
    redir_type, *data = msg.strip().split(" ", 2)
    addr = data[1].rsplit(":", 1)
    return RedirInfo(int(data[0]), addr[0], int(addr[1]), redir_type == "ASK")


def chain_future(src, dst):
    _chain_future(src, dst)


def retry_backoff(retry: int, min_delay: float, max_delay: float) -> float:
    """
    Retry backoff with jitter sleep to prevent overloaded conditions during intervals
    https://www.awsarchitectureblog.com/2015/03/backoff.html
    """

    retry = int(retry)
    if retry < 0:
        retry = 0

    delay = min_delay * (1 << retry)
    if delay > max_delay:
        delay = max_delay

    return random.uniform(0, delay)


def unused_port() -> int:
    """Return a port that is unused on the current host."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])
