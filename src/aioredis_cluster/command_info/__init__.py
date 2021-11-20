import dataclasses
from typing import AnyStr, FrozenSet, List, NoReturn, Sequence

from aioredis_cluster.util import ensure_str

from .commands import (
    BLOCKING_COMMANDS,
    COMMANDS,
    EVAL_COMMANDS,
    ZUNION_COMMANDS,
    ZUNIONSTORE_COMMANDS,
)


__all__ = [
    "CommandsRegistry",
    "CommandInfo",
    "CommandInfoError",
    "InvalidCommandError",
    "extract_keys",
    "create_registry",
    "unknown_command",
]


class CommandInfoError(Exception):
    pass


class InvalidCommandError(CommandInfoError):
    pass


def _raise_wrong_num_of_arguments(cmd: "CommandInfo") -> NoReturn:
    raise InvalidCommandError(f"Wrong number of arguments for {cmd.name!r} command")


@dataclasses.dataclass
class CommandInfo:
    name: str
    arity: int
    flags: FrozenSet[str]
    first_key_arg: int
    last_key_arg: int
    key_args_step: int

    _is_unknown: bool = False

    def is_readonly(self) -> bool:
        return "readonly" in self.flags

    def is_blocking(self) -> bool:
        return self.name in BLOCKING_COMMANDS

    def is_unknown(self) -> bool:
        return self._is_unknown


class CommandsRegistry:
    def __init__(self, commands: Sequence[CommandInfo]) -> None:
        self._commands = {cmd.name: cmd for cmd in commands}

    def get_info(self, cmd: AnyStr) -> CommandInfo:
        cmd_name = ensure_str(cmd).upper()

        try:
            info = self._commands[cmd_name]
        except KeyError:
            return unknown_command(cmd_name)

        return info

    def size(self) -> int:
        return len(self._commands)


def _extract_keys_general(info: CommandInfo, exec_command: Sequence[bytes]) -> List[bytes]:
    keys: List[bytes] = []

    if info.first_key_arg <= 0:
        return []

    if info.last_key_arg < 0:
        last_key_arg = len(exec_command) + info.last_key_arg
    else:
        last_key_arg = info.last_key_arg

    num_of_args = last_key_arg - info.first_key_arg + 1
    if info.key_args_step > 1 and num_of_args % info.key_args_step != 0:
        _raise_wrong_num_of_arguments(info)

    for key_idx in range(info.first_key_arg, last_key_arg + 1, info.key_args_step):
        keys.append(exec_command[key_idx])

    return keys


def _extract_keys_eval(info: CommandInfo, exec_command: Sequence[bytes]) -> List[bytes]:
    abs_arity = abs(info.arity)
    num_of_keys = int(exec_command[abs_arity - 1])
    keys = exec_command[abs_arity : abs_arity + num_of_keys]
    if len(keys) != num_of_keys:
        _raise_wrong_num_of_arguments(info)
    return list(keys)


def _extract_keys_zunion(
    info: CommandInfo,
    exec_command: Sequence[bytes],
    store: bool,
) -> List[bytes]:
    keys: List[bytes] = []
    if store:
        keys.append(exec_command[1])
        # dest key + numkeys arguments
        num_of_keys = int(exec_command[2]) + 1
        first_key_arg = 3
        last_key_arg = first_key_arg + num_of_keys - 2
    else:
        num_of_keys = int(exec_command[1])
        first_key_arg = 2
        last_key_arg = first_key_arg + num_of_keys - 1

    if num_of_keys == 0:
        _raise_wrong_num_of_arguments(info)

    keys.extend(exec_command[first_key_arg : last_key_arg + 1])
    if len(keys) != num_of_keys:
        _raise_wrong_num_of_arguments(info)
    return keys


def extract_keys(info: CommandInfo, exec_command: Sequence[bytes]) -> List[bytes]:
    if len(exec_command) < 1:
        raise ValueError("Execute command is empty")

    cmd_name = ensure_str(exec_command[0]).upper()
    if info.name != cmd_name:
        raise ValueError(f"Incorrect info command: {info.name} != {cmd_name}")

    if info.arity > 0 and len(exec_command) > info.arity or len(exec_command) < abs(info.arity):
        _raise_wrong_num_of_arguments(info)

    # special parsing for command
    if info.name in EVAL_COMMANDS:
        keys = _extract_keys_eval(info, exec_command)
    elif info.name in ZUNION_COMMANDS:
        keys = _extract_keys_zunion(info, exec_command, False)
    elif info.name in ZUNIONSTORE_COMMANDS:
        keys = _extract_keys_zunion(info, exec_command, True)
    else:
        keys = _extract_keys_general(info, exec_command)

    return keys


def create_registry(raw_commands: Sequence[List]) -> CommandsRegistry:
    cmds = []
    for raw_cmd in raw_commands:
        first_key_arg = raw_cmd[3]
        last_key_arg = raw_cmd[4]
        key_args_step = raw_cmd[5]
        if first_key_arg >= 1 and (key_args_step == 0 or last_key_arg == 0):
            raise ValueError("Incorrect command")

        cmd = CommandInfo(
            name=raw_cmd[0].upper(),
            arity=raw_cmd[1],
            flags=frozenset(raw_cmd[2]),
            first_key_arg=first_key_arg,
            last_key_arg=last_key_arg,
            key_args_step=key_args_step,
        )
        cmds.append(cmd)

    return CommandsRegistry(cmds)


def unknown_command(name: str) -> CommandInfo:
    return CommandInfo(
        name=name,
        arity=0,
        flags=frozenset(),
        first_key_arg=0,
        last_key_arg=0,
        key_args_step=0,
        _is_unknown=True,
    )


default_registry = create_registry(COMMANDS)
