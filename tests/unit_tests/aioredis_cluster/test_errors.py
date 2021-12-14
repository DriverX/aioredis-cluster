import pytest

from aioredis_cluster.aioredis import ReplyError
from aioredis_cluster.errors import (
    AskError,
    ClusterDownError,
    LoadingError,
    MovedError,
    TryAgainError,
)


@pytest.mark.parametrize(
    "error_msg, expect_type",
    [
        ("MOVED 12539 1.2.3.4:1000", MovedError),
        ("ASK 12539 1.2.3.4:1000", AskError),
        ("TRYAGAIN try again", TryAgainError),
        ("CLUSTERDOWN cluster is down", ClusterDownError),
        ("LOADING loading", LoadingError),
    ],
)
def test_reply_error__classify(error_msg, expect_type):
    with pytest.raises(expect_type):
        raise ReplyError(error_msg)


def test_moved_error():
    error = MovedError("MOVED 12539 1.2.3.4:1000")
    assert error.info.host == "1.2.3.4"
    assert error.info.port == 1000
    assert error.info.slot_id == 12539


def test_ask_error():
    error = AskError("MOVED 12539 1.2.3.4:1000")
    assert error.info.host == "1.2.3.4"
    assert error.info.port == 1000
    assert error.info.slot_id == 12539
