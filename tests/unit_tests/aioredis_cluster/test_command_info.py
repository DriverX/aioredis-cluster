import pytest

from aioredis_cluster.command_info import (
    InvalidCommandError,
    UnknownCommandError,
    default_registry,
    extract_keys,
)


registry = default_registry


@pytest.mark.parametrize(
    "cmd, exec_command, expect",
    [
        ("KEYS", b"KEYS *".split(), []),
        ("KEYS", b"KEYS foo{bar}".split(), []),
        ("GET", b"GET foo".split(), [b"foo"]),
        ("MGET", b"MGET key1".split(), [b"key1"]),
        ("MSET", b"MSET key1 val1".split(), [b"key1"]),
        ("MGET", b"MGET key1 key{id} KEY3".split(), [b"key1", b"key{id}", b"KEY3"]),
        ("MSET", b"MSET key1 val1 key{id} val2 KEY3 val3".split(), [b"key1", b"key{id}", b"KEY3"]),
        ("GETSET", b"GETSET key value".split(), [b"key"]),
        ("BITOP", b"BITOP AND dest src1".split(), [b"dest", b"src1"]),
        (
            "BITOP",
            b"BITOP AND dest src1 src2 src3 src4".split(),
            [b"dest", b"src1", b"src2", b"src3", b"src4"],
        ),
        # specials
        ("EVAL", b"eval script 1 key1".split(), [b"key1"]),
        ("EVALSHA", b"evalsha sha1 1 key1".split(), [b"key1"]),
        ("EVAL", b"eval script 3 key1 key2 key3 val1".split(), [b"key1", b"key2", b"key3"]),
        ("EVALSHA", b"EVALSHA sha1 3 key1 key2 key3 val1".split(), [b"key1", b"key2", b"key3"]),
        ("PFMERGE", b"PFMERGE destkey sourcekey".split(), [b"destkey", b"sourcekey"]),
    ],
)
def test_extract_keys__successful(cmd, exec_command, expect):
    info = registry.get_info(cmd)
    result = extract_keys(info, exec_command)
    assert result == expect


@pytest.mark.parametrize(
    "cmd, exec_command, expect",
    [
        ("GET", [], ValueError),
        ("GET", [b"UNKNOWN"], ValueError),
        ("GET", [b"GETT"], ValueError),
        ("CLUSTER", [b"CLUSTER"], InvalidCommandError),
        ("GET", b"GET key1 key2".split(), InvalidCommandError),
        ("MSET", b"MSET key1 val1 key2".split(), InvalidCommandError),
        # specials
        ("EVAL", b"eval script 2 key1".split(), InvalidCommandError),
        ("EVALSHA", b"evalsha sha1 2 key1".split(), InvalidCommandError),
    ],
)
def test_extract_keys__error(cmd, exec_command, expect):
    info = registry.get_info(cmd)
    with pytest.raises(expect):
        extract_keys(info, exec_command)


@pytest.mark.parametrize(
    "cmd_name, expect",
    [
        ("GET", "GET"),
        ("set", "SET"),
        (b"LRANGE", "LRANGE"),
        (b"mset", "MSET"),
    ],
)
def test_registry_get_info__success(cmd_name, expect):
    info = registry.get_info(cmd_name)
    assert info.name == expect


@pytest.mark.parametrize(
    "cmd_name, expect",
    [
        ("GET", True),
        ("set", False),
        (b"LRANGE", True),
        (b"mset", False),
    ],
)
def test_registry_get_info__is_readonly(cmd_name, expect):
    info = registry.get_info(cmd_name)

    assert info.is_readonly() is expect


@pytest.mark.parametrize(
    "cmd_name, expect",
    [
        ("GEET", UnknownCommandError),
        ("", UnknownCommandError),
        (b"LRANGE ", UnknownCommandError),
    ],
)
def test_registry_get_info__error(cmd_name, expect):
    with pytest.raises(expect):
        registry.get_info(cmd_name)
