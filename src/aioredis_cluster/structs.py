from typing import Dict, List, NamedTuple, Optional, Sequence, Tuple

import attr


class Address(NamedTuple):
    host: str
    port: int

    def __str__(self) -> str:
        return f"{self.host}:{self.port}"


class ClusterNode(NamedTuple):
    addr: Address
    node_id: str


@attr.s(slots=True)
class ClusterSlot:
    begin: int = attr.ib()
    end: int = attr.ib()
    master: ClusterNode = attr.ib()
    replicas: List[ClusterNode] = attr.ib(factory=list)

    def in_range(self, slot: int) -> bool:
        return self.begin <= slot <= self.end


class ExecuteContext:
    __slots__ = (
        "cmd",
        "kwargs",
        "cmd_name",
        "slot",
        "max_attempts",
        "attempt",
    )

    def __init__(
        self,
        *,
        cmd: Sequence[bytes],
        kwargs: Dict,
        max_attempts: int,
    ) -> None:
        if len(cmd) < 1:
            raise ValueError("Execute command is empty")

        self.cmd = tuple(cmd)
        self.kwargs = kwargs
        self.cmd_name = cmd[0].decode("utf-8").upper()
        self.slot: Optional[int] = None
        self.max_attempts = max_attempts
        self.attempt = 0

    def cmd_for_repr(self) -> bytes:
        cmd_bytes = b" ".join(self.cmd)
        if len(cmd_bytes) > 32:
            cmd_bytes = cmd_bytes[:16] + b"..." + cmd_bytes[-16:]
        return cmd_bytes

    def __repr__(self) -> str:
        info: List[Tuple[str, str]] = []
        info.append(("cmd", self.cmd_name))
        info.append(("slot", str(self.slot)))
        info.append(("max_attempts", str(self.max_attempts)))
        info.append(("attempt", str(self.attempt)))
        return "<ExecuteContext {}>".format(" ".join(f"{k}={v}" for k, v in info))


@attr.s(slots=True, kw_only=True)
class ExecuteProps:
    node_addr: Address = attr.ib(init=False)
    reload_state_required: bool = attr.ib(default=False)
    asking: bool = attr.ib(default=False)


@attr.s(slots=True, kw_only=True)
class ExecuteFailProps:
    node_addr: Address = attr.ib()
    error: Exception = attr.ib()
