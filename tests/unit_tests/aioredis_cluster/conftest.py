import pytest
import pytest_asyncio


@pytest.fixture
def add_finalizer():
    finalizers = []

    def adder(finalizer):
        finalizers.append(finalizer)

    try:
        yield adder
    finally:
        for finalizer in finalizers:
            finalizer()


@pytest_asyncio.fixture
async def add_async_finalizer():
    finalizers = []

    def adder(finalizer):
        finalizers.append(finalizer)

    try:
        yield adder
    finally:
        for finalizer in finalizers:
            await finalizer()
