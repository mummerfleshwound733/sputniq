"""Tests for the generic TaskMiddleware hook — no DI framework required."""

from __future__ import annotations

import asyncio
import contextvars
from typing import TYPE_CHECKING

import pytest

from sputniq import TaskMiddleware, Worker

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def recording_middleware(log: list[str]) -> TaskMiddleware:
    """Middleware that records before/after execution."""

    async def middleware(call_next: Callable[[], Awaitable[None]]) -> None:
        log.append("before")
        await call_next()
        log.append("after")

    return middleware


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_middleware_called_around_task(worker: Worker) -> None:
    log: list[str] = []
    w = Worker(task_middleware=recording_middleware(log))
    await w.start()

    @w.task
    async def noop() -> None:
        log.append("task")

    noop.enqueue()
    await w.stop(timeout=5.0)

    assert log == ["before", "task", "after"]


async def test_middleware_called_per_task() -> None:
    log: list[str] = []
    w = Worker(task_middleware=recording_middleware(log))
    await w.start()

    @w.task
    async def noop() -> None:
        log.append("task")

    noop.enqueue()
    noop.enqueue()
    await w.stop(timeout=5.0)

    assert log.count("before") == 2
    assert log.count("task") == 2
    assert log.count("after") == 2


async def test_middleware_runs_after_even_on_task_error() -> None:
    log: list[str] = []
    w = Worker(
        task_middleware=recording_middleware(log),
        default_retry=pytest.importorskip("sputniq").RetryConfig(max_attempts=1),
    )
    await w.start()

    @w.task
    async def failing() -> None:
        log.append("task")
        msg = "boom"
        raise RuntimeError(msg)

    failing.enqueue()
    await w.stop(timeout=5.0)

    assert log == ["before", "task", "after"]


async def test_middleware_can_inject_value_via_closure() -> None:
    """Any DI framework can pass values by setting them before call_next."""
    injected: list[str] = []

    async def di_middleware(call_next: Callable[[], Awaitable[None]]) -> None:
        # Framework sets up context here (e.g. opens a DB session)
        injected.append("injected_value")
        await call_next()
        # Framework tears down context here

    w = Worker(task_middleware=di_middleware)
    await w.start()

    results: list[str] = []

    @w.task
    async def use_injection() -> None:
        results.append(injected[-1])

    use_injection.enqueue()
    await w.stop(timeout=5.0)

    assert results == ["injected_value"]


async def test_no_middleware_works_normally() -> None:
    w = Worker()
    await w.start()
    results: list[int] = []

    @w.task
    async def add(x: int) -> None:
        results.append(x)

    add.enqueue(1)
    add.enqueue(2)
    await w.stop(timeout=5.0)
    assert sorted(results) == [1, 2]


async def test_middleware_scope_isolated_between_concurrent_tasks() -> None:
    var: contextvars.ContextVar[int] = contextvars.ContextVar("test_var")
    seen: list[int] = []
    barrier = asyncio.Event()

    async def ctx_middleware(call_next: Callable[[], Awaitable[None]]) -> None:
        token = var.set(id(asyncio.current_task()))
        await call_next()
        var.reset(token)

    w = Worker(task_middleware=ctx_middleware, max_concurrency=5)
    await w.start()

    @w.task
    async def read_var() -> None:
        await barrier.wait()
        seen.append(var.get())

    for _ in range(5):
        read_var.enqueue()

    await asyncio.sleep(0.05)
    barrier.set()
    await w.stop(timeout=5.0)

    assert len(seen) == 5
    assert len(set(seen)) == 5


async def test_middleware_receives_correct_call_next() -> None:
    executed = asyncio.Event()

    async def passthrough(call_next: Callable[[], Awaitable[None]]) -> None:
        await call_next()

    w = Worker(task_middleware=passthrough)
    await w.start()

    @w.task
    async def signal() -> None:
        executed.set()

    signal.enqueue()
    await w.stop(timeout=5.0)
    assert executed.is_set()


async def test_add_middleware_multiple_called_in_order() -> None:
    log: list[str] = []

    async def mw1(call_next: Callable[[], Awaitable[None]]) -> None:
        log.append("mw1-before")
        await call_next()
        log.append("mw1-after")

    async def mw2(call_next: Callable[[], Awaitable[None]]) -> None:
        log.append("mw2-before")
        await call_next()
        log.append("mw2-after")

    w = Worker()
    w.add_middleware(mw1)
    w.add_middleware(mw2)
    await w.start()

    @w.task
    async def noop() -> None:
        log.append("task")

    noop.enqueue()
    await w.stop(timeout=5.0)

    assert log == ["mw1-before", "mw2-before", "task", "mw2-after", "mw1-after"]


async def test_add_middleware_combined_with_constructor_middleware() -> None:
    log: list[str] = []

    async def ctor_mw(call_next: Callable[[], Awaitable[None]]) -> None:
        log.append("ctor-before")
        await call_next()
        log.append("ctor-after")

    async def added_mw(call_next: Callable[[], Awaitable[None]]) -> None:
        log.append("added-before")
        await call_next()
        log.append("added-after")

    w = Worker(task_middleware=ctor_mw)
    w.add_middleware(added_mw)
    await w.start()

    @w.task
    async def noop() -> None:
        log.append("task")

    noop.enqueue()
    await w.stop(timeout=5.0)

    assert log == ["ctor-before", "added-before", "task", "added-after", "ctor-after"]
