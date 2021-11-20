import dataclasses
from typing import Dict, List, Optional, Sequence, Tuple

from .command_info import CommandInfo
from .structs import Address


class ExecuteContext:
    __slots__ = (
        "cmd",
        "kwargs",
        "cmd_info",
        "cmd_name",
        "slot",
        "max_attempts",
        "attempt",
    )

    def __init__(
        self,
        *,
        cmd: Sequence[bytes],
        cmd_info: CommandInfo,
        kwargs: Dict,
        max_attempts: int,
    ) -> None:
        if len(cmd) < 1:
            raise ValueError("Execute command is empty")

        self.cmd = tuple(cmd)
        self.kwargs = kwargs
        self.cmd_name = cmd_info.name
        self.cmd_info = cmd_info
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


@dataclasses.dataclass
class ExecuteProps:
    node_addr: Address = dataclasses.field(init=False)
    reload_state_required: bool = False
    asking: bool = False


@dataclasses.dataclass
class ExecuteFailProps:
    node_addr: Address
    error: Exception
