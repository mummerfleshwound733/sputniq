from __future__ import annotations

import asyncio

from sputniq import BoundTask, RetryConfig, Worker


async def test_bare_decorator(worker: Worker) -> None:
    @worker.task
    async def my_func(x: int) -> int:
        return x * 2

    assert isinstance(my_func, BoundTask)
    assert my_func.__name__ == "my_func"


async def test_parameterized_decorator(worker: Worker) -> None:
    @worker.task(retry=RetryConfig(max_attempts=5))
    async def my_func() -> None:
        pass

    assert isinstance(my_func, BoundTask)


async def test_custom_name(worker: Worker) -> None:
    @worker.task(name="custom")
    async def my_func() -> None:
        pass

    assert my_func.__name__ == "custom"


async def test_direct_await_without_start() -> None:
    """BoundTask.__call__ should work without the worker running."""
    w = Worker()

    @w.task
    async def double(x: int) -> int:
        return x * 2

    result = await double(21)
    assert result == 42


async def test_enqueue_runs_task(worker: Worker) -> None:
    done = asyncio.Event()

    @worker.task
    async def signal() -> None:
        done.set()

    signal.enqueue()
    await asyncio.wait_for(done.wait(), timeout=5.0)


async def test_bound_task_name_attribute(worker: Worker) -> None:
    @worker.task
    async def some_function() -> None:
        pass

    assert some_function.__name__ == "some_function"


async def test_multiple_tasks_independent(worker: Worker) -> None:
    results: list[str] = []

    @worker.task
    async def task_a() -> None:
        results.append("a")

    @worker.task
    async def task_b() -> None:
        results.append("b")

    task_a.enqueue()
    task_b.enqueue()
    await worker.stop(timeout=5.0)
    assert sorted(results) == ["a", "b"]


async def test_default_retry_from_worker() -> None:
    default = RetryConfig(max_attempts=7)
    w = Worker(default_retry=default)
    await w.start()

    @w.task
    async def noop() -> None:
        pass

    assert noop._options.retry  # noqa: SLF001
    assert noop._options.retry.max_attempts == 7  # noqa: SLF001
    await w.stop()


async def test_task_retry_overrides_default() -> None:
    default = RetryConfig(max_attempts=7)
    w = Worker(default_retry=default)
    await w.start()

    @w.task(retry=RetryConfig(max_attempts=2))
    async def noop() -> None:
        pass

    assert noop._options.retry  # noqa: SLF001
    assert noop._options.retry.max_attempts == 2  # noqa: SLF001
    await w.stop()
