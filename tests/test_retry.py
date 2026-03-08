from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import pytest

from sputniq import MaxRetriesExceededError, RetryConfig, Worker
from sputniq._task import BoundTask, TaskOptions

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine


def make_bound_task(
    fn: Callable[..., Coroutine[Any, Any, Any]],
    retry: RetryConfig,
    worker: Worker,
) -> BoundTask[Any, Any]:
    return BoundTask(fn, worker, TaskOptions(retry=retry))


# ---------------------------------------------------------------------------
# RetryConfig.compute_delay math
# ---------------------------------------------------------------------------


def test_compute_delay_no_jitter_exact() -> None:
    cfg = RetryConfig(
        initial_delay=1.0,
        backoff_factor=2.0,
        max_delay=100.0,
        jitter=False,
    )
    assert cfg.compute_delay(0) == pytest.approx(1.0)
    assert cfg.compute_delay(1) == pytest.approx(2.0)
    assert cfg.compute_delay(2) == pytest.approx(4.0)


def test_compute_delay_caps_at_max() -> None:
    cfg = RetryConfig(
        initial_delay=1.0,
        backoff_factor=10.0,
        max_delay=5.0,
        jitter=False,
    )
    assert cfg.compute_delay(10) == pytest.approx(5.0)


def test_compute_delay_jitter_bounds() -> None:
    cfg = RetryConfig(
        initial_delay=1.0,
        backoff_factor=2.0,
        max_delay=100.0,
        jitter=True,
    )
    for attempt in range(5):
        delay = cfg.compute_delay(attempt)
        uncapped = 1.0 * (2.0**attempt)
        assert 0.0 <= delay <= uncapped


# ---------------------------------------------------------------------------
# Retry behaviour end-to-end through the worker
# ---------------------------------------------------------------------------


async def test_succeeds_on_second_attempt(worker: Worker) -> None:
    attempts: list[int] = []

    @worker.task(retry=RetryConfig(max_attempts=3, initial_delay=0.0, jitter=False))
    async def flaky() -> None:
        attempts.append(1)
        if len(attempts) < 2:
            msg = "boom"
            raise ValueError(msg)

    flaky.enqueue()
    await worker.stop(timeout=5.0)
    assert len(attempts) == 2


async def test_exhausted_retries_dont_crash_worker(worker: Worker) -> None:
    attempts: list[int] = []

    @worker.task(retry=RetryConfig(max_attempts=2, initial_delay=0.0, jitter=False))
    async def always_fails() -> None:
        attempts.append(1)
        msg = "always"
        raise RuntimeError(msg)

    always_fails.enqueue()
    await worker.stop(timeout=5.0)
    assert len(attempts) == 2
    assert worker._started is False  # worker shut down cleanly  # noqa: SLF001


async def test_max_retries_exceeded_error_raised_internally(worker: Worker) -> None:
    """_run_with_retry raises MaxRetriesExceededError after all attempts."""
    cfg = RetryConfig(max_attempts=2, initial_delay=0.0, jitter=False)

    async def always_fails() -> None:
        msg = "err"
        raise ValueError(msg)

    bt = make_bound_task(always_fails, cfg, worker)

    with pytest.raises(MaxRetriesExceededError):
        await bt._run_with_retry((), {})  # noqa: SLF001


async def test_single_attempt_no_retry(worker: Worker) -> None:
    attempts: list[int] = []

    @worker.task(retry=RetryConfig(max_attempts=1, initial_delay=0.0, jitter=False))
    async def fails_once() -> None:
        attempts.append(1)
        msg = "nope"
        raise ValueError(msg)

    fails_once.enqueue()
    await worker.stop(timeout=5.0)
    assert len(attempts) == 1


async def test_cancelled_error_not_retried() -> None:
    w = Worker()
    await w.start()

    attempts: list[int] = []

    @w.task(retry=RetryConfig(max_attempts=5, initial_delay=0.0, jitter=False))
    async def gets_cancelled() -> None:
        attempts.append(1)
        raise asyncio.CancelledError

    gets_cancelled.enqueue()
    await asyncio.sleep(0.05)
    await w.stop(timeout=5.0)
    # CancelledError should propagate immediately, no retry
    assert len(attempts) == 1
