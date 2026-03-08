"""Type-level assertions checked by mypy/ty in CI.

The reveal_type() calls are inside TYPE_CHECKING blocks so they run only
under static type checkers, not at test time.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sputniq import BoundTask, RetryConfig, Worker

worker = Worker()


@worker.task
async def takes_str(s: str) -> int:
    return len(s)


@worker.task(retry=RetryConfig(max_attempts=5))
async def takes_int(n: int) -> bool:
    return n > 0


if TYPE_CHECKING:
    from typing_extensions import reveal_type

    reveal_type(takes_str)
    reveal_type(takes_int)


def test_bare_decorator_is_bound_task() -> None:
    assert isinstance(takes_str, BoundTask)
    assert takes_str.__name__ == "takes_str"


def test_parameterized_decorator_is_bound_task() -> None:
    assert isinstance(takes_int, BoundTask)
    assert takes_int.__name__ == "takes_int"
