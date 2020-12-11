import pytest

from aioredis_cluster.util import retry_backoff


@pytest.mark.parametrize(
    "retry, min_delay, max_delay, expect",
    [
        (-1, 1.5, 10.0, 1.6),
        (0, 1.5, 10.0, 1.6),
        (1, 1.5, 10.0, 3.1),
        (2, 1.5, 10.0, 6.1),
        (3, 1.5, 10.0, 10.1),
        (4, 1.5, 10.0, 10.1),
    ],
)
def test_retry_backoff(mocker, retry, min_delay, max_delay, expect):
    mocker.patch(retry_backoff.__module__ + ".random.uniform", side_effect=lambda a, b: b + 0.1)

    result = retry_backoff(retry, min_delay, max_delay)
    assert result == expect
