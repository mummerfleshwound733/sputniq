from __future__ import annotations

import asyncio
import functools
import logging
from collections.abc import Awaitable, Callable, Coroutine, Iterable
from typing import Any, Protocol, overload

from typing_extensions import Self

from sputniq._exceptions import MaxRetriesExceededError, WorkerNotRunningError
from sputniq._retry import RetryConfig
from sputniq._task import BoundTask, TaskOptions
from sputniq._types import P, R, TaskMiddleware

logger = logging.getLogger(__name__)

_INTERRUPT_SENTINEL = object()

# Internal type for queued items
_PendingItem = tuple[BoundTask[Any, Any], tuple[Any, ...], dict[str, Any]]


class _RegistryTask(Protocol):
    def _bind(self, worker: Worker) -> None: ...


class _RegistryProto(Protocol):
    _tasks: Iterable[_RegistryTask]


class Worker:
    """In-process async background task runner."""

    def __init__(
        self,
        max_concurrency: int | None = None,
        default_retry: RetryConfig | None = None,
        task_timeout: float | None = None,
        task_middleware: TaskMiddleware | None = None,
    ) -> None:
        self._max_concurrency = max_concurrency
        self._default_retry = default_retry or RetryConfig()
        self._task_timeout = task_timeout
        self._middlewares: list[TaskMiddleware] = (
            [task_middleware] if task_middleware is not None else []
        )

        self._queue: asyncio.Queue[_PendingItem] = asyncio.Queue()
        self._running: set[asyncio.Task[None]] = set()
        self._semaphore: asyncio.Semaphore | None = (
            asyncio.Semaphore(max_concurrency) if max_concurrency is not None else None
        )
        self._consumer: asyncio.Task[None] | None = None
        self._started = False
        self._stopping = False

    async def start(self) -> None:
        """Start the background consumer task."""
        if self._started:
            return
        self._started = True
        self._stopping = False
        self._queue = asyncio.Queue()
        self._running = set()
        self._semaphore = (
            asyncio.Semaphore(self._max_concurrency)
            if self._max_concurrency is not None
            else None
        )
        self._consumer = asyncio.create_task(self._consume(), name="sputniq-consumer")

    async def stop(self, timeout: float = 30.0) -> None:
        """Gracefully stop the worker, waiting for in-flight tasks."""
        if not self._started:
            return
        self._stopping = True
        await self._queue.put(_INTERRUPT_SENTINEL)  # type: ignore[arg-type]
        if self._consumer is not None:
            await self._consumer
        if self._running:
            _, pending = await asyncio.wait(self._running, timeout=timeout)
            if pending:
                for task in pending:
                    task.cancel()
                errors = await asyncio.gather(*pending, return_exceptions=True)
                logger.error("Task cancelled on shutdown with errors: %s", errors)
        self._started = False

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()

    def add_middleware(self, middleware: TaskMiddleware) -> None:
        """Append a middleware to the chain. First added runs outermost."""
        self._middlewares.append(middleware)

    def include_registry(self, registry: _RegistryProto) -> None:
        for reg_task in registry._tasks:  # noqa: SLF001
            reg_task._bind(self)  # noqa: SLF001

    def _enqueue(
        self,
        bound_task: BoundTask[Any, Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        if not self._started or self._stopping:
            raise WorkerNotRunningError(
                "Worker is not running. Call await worker.start() first.",
            )
        self._queue.put_nowait((bound_task, args, kwargs))

    async def _consume(self) -> None:
        while True:
            item = await self._queue.get()
            if item is _INTERRUPT_SENTINEL:
                break
            bound_task, args, kwargs = item
            t = asyncio.create_task(
                self._execute(bound_task, args, kwargs),
                name=f"sputniq-{bound_task.__name__}",
            )
            self._running.add(t)
            t.add_done_callback(self._running.discard)

    async def _apply_limits(self, fn: Callable[[], Awaitable[None]]) -> None:
        """Run fn() inside the semaphore and/or timeout if configured."""
        if self._semaphore is not None:
            async with self._semaphore:
                if self._task_timeout is not None:
                    await asyncio.wait_for(fn(), timeout=self._task_timeout)
                else:
                    await fn()
        elif self._task_timeout is not None:
            await asyncio.wait_for(fn(), timeout=self._task_timeout)
        else:
            await fn()

    async def _execute(
        self,
        bound_task: BoundTask[Any, Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        async def _run() -> None:
            try:
                await bound_task._run_with_retry(args, kwargs)  # noqa: SLF001
            except MaxRetriesExceededError:
                pass  # already logged inside _run_with_retry
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Unexpected error in task %r", bound_task.__name__)

        async def _invoke() -> None:
            call_next: Callable[[], Awaitable[None]] = _run
            for mw in reversed(self._middlewares):
                call_next = functools.partial(mw, call_next)
            await call_next()

        try:
            await self._apply_limits(_invoke)
        except TimeoutError:
            logger.warning(
                "Task %r timed out after %.2fs",
                bound_task.__name__,
                self._task_timeout,
            )

    @overload
    def task(self, fn: Callable[P, Coroutine[Any, Any, R]]) -> BoundTask[P, R]: ...

    @overload
    def task(
        self,
        *,
        retry: RetryConfig | None = ...,
        name: str | None = ...,
        on_success: Callable[[], Awaitable[None]] | None = ...,
        on_failure: Callable[[BaseException], Awaitable[None]] | None = ...,
        on_retry: Callable[[BaseException, int], Awaitable[None]] | None = ...,
    ) -> Callable[[Callable[P, Coroutine[Any, Any, R]]], BoundTask[P, R]]: ...

    def task(  # noqa: PLR0913
        self,
        fn: Callable[P, Coroutine[Any, Any, R]] | None = None,
        *,
        retry: RetryConfig | None = None,
        name: str | None = None,
        on_success: Callable[[], Awaitable[None]] | None = None,
        on_failure: Callable[[BaseException], Awaitable[None]] | None = None,
        on_retry: Callable[[BaseException, int], Awaitable[None]] | None = None,
    ) -> (
        BoundTask[P, R]
        | Callable[[Callable[P, Coroutine[Any, Any, R]]], BoundTask[P, R]]
    ):
        """Decorator to register an async function as a background task."""
        options = TaskOptions(
            retry=retry or self._default_retry,
            name=name,
            on_success=on_success,
            on_failure=on_failure,
            on_retry=on_retry,
        )

        def decorator(f: Callable[P, Coroutine[Any, Any, R]]) -> BoundTask[P, R]:
            return BoundTask(f, self, options)

        if fn is not None:
            return decorator(fn)
        return decorator
