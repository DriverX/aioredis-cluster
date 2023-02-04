import dataclasses
from typing import Any, Dict, List, Optional, Tuple

from .command_info import CommandInfo
from .command_info.commands import SHARDED_PUBSUB_COMMANDS
from .structs import Address


class ExecuteContext:
    __slots__ = (
        "cmd",
        "kwargs",
        "cmd_info",
        "cmd_name",
        "is_sharded_pubsub",
        "slot",
        "max_attempts",
        "attempt",
        "start_time",
        "_cached_cmd_for_repr",
    )

    def __init__(
        self,
        *,
        cmd: Tuple[bytes, ...],
        cmd_info: CommandInfo,
        kwargs: Dict[str, Any],
        max_attempts: int,
        start_time: float,
    ) -> None:
        if len(cmd) < 1:
            raise ValueError("Execute command is empty")

        self.cmd = cmd
        self.kwargs = kwargs
        self.cmd_name = cmd_info.name
        self.cmd_info = cmd_info
        self.is_sharded_pubsub = self.cmd_name in SHARDED_PUBSUB_COMMANDS
        self.slot = -1
        self.max_attempts = max_attempts
        self.attempt = 0
        self.start_time = start_time
        self._cached_cmd_for_repr: Optional[bytes] = None

    def cmd_for_repr(self) -> bytes:
        if self._cached_cmd_for_repr is None:
            cmd_bytes = b" ".join(self.cmd)
            if len(cmd_bytes) > 32:
                cmd_bytes = cmd_bytes[:16] + b"..." + cmd_bytes[-16:]
            self._cached_cmd_for_repr = cmd_bytes
        return self._cached_cmd_for_repr

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
