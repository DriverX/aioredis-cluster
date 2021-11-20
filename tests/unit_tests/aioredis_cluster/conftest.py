import asyncio

import pytest


def pytest_collection_modifyitems(items):
    for item in items:
        if not item.get_closest_marker("asyncio") and asyncio.iscoroutinefunction(item.function):
            item.add_marker(pytest.mark.asyncio)
