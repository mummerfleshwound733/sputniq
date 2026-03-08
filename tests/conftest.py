from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest

from sputniq import Worker


@pytest.fixture
async def worker() -> AsyncGenerator[Worker, None]:
    w = Worker()
    await w.start()
    yield w
    await w.stop(timeout=5.0)
