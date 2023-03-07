import timeit
from typing import Callable, Tuple


def run_bench(run_fn: Callable) -> Tuple[str, float]:
    timings = []
    for i in range(10):
        t = timeit.timeit(stmt=run_fn, number=100000)
        timings.append(t)

    result_t = min(timings)
    return run_fn.__name__, result_t
