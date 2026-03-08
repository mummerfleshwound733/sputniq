"""Tests for on_success, on_failure, and on_retry observability hooks."""

from __future__ import annotations

import pytest

from sputniq import RetryConfig, Worker


async def test_on_success_called(worker: Worker) -> None:
    fired: list[str] = []

    async def cb() -> None:
        fired.append("ok")

    @worker.task(on_success=cb)
    async def succeed() -> None:
        pass

    succeed.enqueue()
    await worker.stop(timeout=5.0)

    assert fired == ["ok"]


async def test_on_success_called_once_per_execution(worker: Worker) -> None:
    fired: list[str] = []

    async def cb() -> None:
        fired.append("ok")

    @worker.task(on_success=cb)
    async def succeed() -> None:
        pass

    succeed.enqueue()
    succeed.enqueue()
    await worker.stop(timeout=5.0)

    assert fired == ["ok", "ok"]


async def test_on_failure_called_after_all_retries_exhausted() -> None:
    failures: list[BaseException] = []

    async def cb(exc: BaseException) -> None:
        failures.append(exc)

    w = Worker()
    await w.start()

    @w.task(retry=RetryConfig(max_attempts=1), on_failure=cb)
    async def always_fail() -> None:
        raise ValueError("boom")

    always_fail.enqueue()
    await w.stop(timeout=5.0)

    assert len(failures) == 1
    assert isinstance(failures[0], ValueError)


async def test_on_failure_not_called_on_success(worker: Worker) -> None:
    fired: list[str] = []

    async def cb(exc: BaseException) -> None:
        fired.append("fail")

    @worker.task(on_failure=cb)
    async def succeed() -> None:
        pass

    succeed.enqueue()
    await worker.stop(timeout=5.0)

    assert fired == []


async def test_on_success_not_called_on_failure() -> None:
    fired: list[str] = []

    async def cb() -> None:
        fired.append("ok")

    w = Worker()
    await w.start()

    @w.task(retry=RetryConfig(max_attempts=1), on_success=cb)
    async def always_fail() -> None:
        raise ValueError("boom")

    always_fail.enqueue()
    await w.stop(timeout=5.0)

    assert fired == []


async def test_on_retry_called_with_attempt_number() -> None:
    retries: list[tuple[type[BaseException], int]] = []

    async def cb(exc: BaseException, attempt: int) -> None:
        retries.append((type(exc), attempt))

    w = Worker()
    await w.start()

    attempt_counter = 0

    retry_cfg = RetryConfig(max_attempts=3, initial_delay=0.0, jitter=False)

    @w.task(retry=retry_cfg, on_retry=cb)
    async def flaky() -> None:
        nonlocal attempt_counter
        attempt_counter += 1
        if attempt_counter < 3:
            raise RuntimeError("not yet")

    flaky.enqueue()
    await w.stop(timeout=5.0)

    assert retries == [(RuntimeError, 1), (RuntimeError, 2)]


async def test_on_retry_not_called_on_first_success(worker: Worker) -> None:
    fired: list[int] = []

    async def cb(exc: BaseException, attempt: int) -> None:
        fired.append(attempt)

    @worker.task(on_retry=cb)
    async def succeed() -> None:
        pass

    succeed.enqueue()
    await worker.stop(timeout=5.0)

    assert fired == []


async def test_on_success_exception_does_not_crash_worker(worker: Worker) -> None:
    results: list[str] = []

    async def bad_cb() -> None:
        raise RuntimeError("callback exploded")

    @worker.task(on_success=bad_cb)
    async def succeed() -> None:
        results.append("ran")

    succeed.enqueue()
    succeed.enqueue()
    await worker.stop(timeout=5.0)

    assert results == ["ran", "ran"]


async def test_on_failure_exception_does_not_crash_worker() -> None:
    w = Worker()
    await w.start()

    async def bad_cb(exc: BaseException) -> None:
        raise RuntimeError("callback exploded")

    @w.task(retry=RetryConfig(max_attempts=1), on_failure=bad_cb)
    async def fail() -> None:
        raise ValueError("task error")

    fail.enqueue()
    fail.enqueue()
    await w.stop(timeout=5.0)
    assert w._started is False  # noqa: SLF001


async def test_on_failure_receives_last_exception() -> None:
    """on_failure should receive the exception from the final attempt."""
    received: list[str] = []

    async def cb(exc: BaseException) -> None:
        received.append(str(exc))

    w = Worker()
    await w.start()

    call_count = 0

    retry_cfg = RetryConfig(max_attempts=2, initial_delay=0.0, jitter=False)

    @w.task(retry=retry_cfg, on_failure=cb)
    async def fail() -> None:
        nonlocal call_count
        call_count += 1
        raise ValueError(f"attempt {call_count}")

    fail.enqueue()
    await w.stop(timeout=5.0)

    assert received == ["attempt 2"]


@pytest.mark.parametrize("hook", ["on_success", "on_failure", "on_retry"])
async def test_hook_none_by_default(worker: Worker, hook: str) -> None:
    @worker.task
    async def succeed() -> None:
        pass

    assert getattr(succeed._options, hook) is None  # noqa: SLF001
