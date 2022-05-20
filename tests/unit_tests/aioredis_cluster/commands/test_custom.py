from unittest import mock

import pytest

from aioredis_cluster.commands.commands import RedisCluster


@pytest.mark.parametrize(
    "max_len, min_id, exact, limit, expect_error_msg",
    [
        (None, None, False, 100, "limit cannot be used without min_id or max_len"),
        (100, 100, False, None, "max_len and min_id options at the same time are not compatible"),
        (None, 100, True, 100, "limit cannot be used with exact option"),
    ],
)
def test__xadd_620_errors(max_len, min_id, exact, limit, expect_error_msg):
    redis = RedisCluster(mock.NonCallableMock())
    with pytest.raises(ValueError, match=expect_error_msg):
        redis.xadd_620(
            "test_stream",
            fields={"field": "value"},
            max_len=max_len,
            min_id=min_id,
            exact=exact,
            limit=limit,
        )


@pytest.mark.parametrize(
    "max_len, min_id, exact, limit, no_mk_stream, expect_command",
    [
        (
            100,
            None,
            False,
            None,
            False,
            [b"XADD", "test_stream", b"MAXLEN", b"~", 100, b"*", "field", "value"],
        ),
        (
            None,
            100,
            False,
            None,
            False,
            [b"XADD", "test_stream", b"MINID", b"~", 100, b"*", "field", "value"],
        ),
        (
            100,
            None,
            True,
            None,
            False,
            [b"XADD", "test_stream", b"MAXLEN", 100, b"*", "field", "value"],
        ),
        (
            None,
            100,
            True,
            None,
            False,
            [b"XADD", "test_stream", b"MINID", 100, b"*", "field", "value"],
        ),
        (
            100,
            None,
            False,
            1000,
            False,
            [b"XADD", "test_stream", b"MAXLEN", b"~", 100, b"LIMIT", 1000, b"*", "field", "value"],
        ),
        (
            None,
            100,
            False,
            1000,
            False,
            [b"XADD", "test_stream", b"MINID", b"~", 100, b"LIMIT", 1000, b"*", "field", "value"],
        ),
        (
            None,
            None,
            False,
            None,
            True,
            [b"XADD", "test_stream", b"NOMKSTREAM", b"*", "field", "value"],
        ),
        (
            100,
            None,
            False,
            1000,
            True,
            [
                b"XADD",
                "test_stream",
                b"NOMKSTREAM",
                b"MAXLEN",
                b"~",
                100,
                b"LIMIT",
                1000,
                b"*",
                "field",
                "value",
            ],
        ),
    ],
)
def test__xadd_620_generate_command(
    mocker, max_len, min_id, exact, limit, no_mk_stream, expect_command
):
    redis = RedisCluster(mock.NonCallableMock())
    mocked_redis = mocker.patch.object(redis, "execute")
    redis.xadd_620(
        "test_stream",
        fields={"field": "value"},
        max_len=max_len,
        min_id=min_id,
        exact=exact,
        limit=limit,
        no_mk_stream=no_mk_stream,
    )
    mocked_redis.assert_called_once_with(*expect_command)
