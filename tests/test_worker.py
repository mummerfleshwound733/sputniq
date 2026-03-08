from __future__ import annotations

import asyncio

import pytest

from sputniq import RetryConfig, Worker, WorkerNotRunningError


async def test_start_idempotent() -> None:
    w = Worker()
    await w.start()
    await w.start()  # second call should be a no-op
    await w.stop()


async def test_stop_not_started() -> None:
    w = Worker()
    await w.stop()  # should not raise


async def test_stop_waits_for_in_flight(worker: Worker) -> None:
    started = asyncio.Event()
    finished = asyncio.Event()

    @worker.task
    async def slow_task() -> None:
        started.set()
        await asyncio.sleep(0.1)
        finished.set()

    slow_task.enqueue()
    await started.wait()
    await worker.stop(timeout=5.0)
    assert finished.is_set()


async def test_stop_timeout_cancels_stragglers() -> None:
    w = Worker()
    await w.start()

    @w.task
    async def forever() -> None:
        await asyncio.sleep(100)

    forever.enqueue()
    await asyncio.sleep(0.05)
    await w.stop(timeout=0.01)  # very short timeout — task should be cancelled


async def test_enqueue_before_start_raises() -> None:
    w = Worker()

    @w.task
    async def noop() -> None:
        pass

    with pytest.raises(WorkerNotRunningError):
        noop.enqueue()


async def test_enqueue_after_stop_raises(worker: Worker) -> None:
    @worker.task
    async def noop() -> None:
        pass

    await worker.stop()

    with pytest.raises(WorkerNotRunningError):
        noop.enqueue()


async def test_concurrency_limit() -> None:
    max_concurrent = 2
    w = Worker(max_concurrency=max_concurrent)
    await w.start()

    active: list[int] = []
    peak: list[int] = []
    barrier = asyncio.Event()
    n_tasks = 5

    @w.task
    async def tracked() -> None:
        active.append(1)
        peak.append(len(active))
        await barrier.wait()
        active.pop()

    for _ in range(n_tasks):
        tracked.enqueue()

    await asyncio.sleep(0.05)
    barrier.set()
    await w.stop(timeout=5.0)

    assert max(peak) <= max_concurrent


async def test_task_runs_successfully(worker: Worker) -> None:
    results: list[int] = []

    @worker.task
    async def add(x: int) -> None:
        results.append(x)

    add.enqueue(42)
    await worker.stop(timeout=5.0)
    assert results == [42]


async def test_task_timeout_cancels(worker: Worker) -> None:
    w = Worker(task_timeout=0.01, default_retry=RetryConfig(max_attempts=1))
    await w.start()
    reached = asyncio.Event()

    @w.task
    async def slow() -> None:
        await asyncio.sleep(10)
        reached.set()

    slow.enqueue()
    await w.stop(timeout=5.0)
    assert not reached.is_set()
