from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Generic, Protocol, cast

from sputniq._exceptions import MaxRetriesExceededError
from sputniq._retry import RetryConfig
from sputniq._types import P, R


class _WorkerProto(Protocol):
    def _enqueue(
        self,
        bound_task: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None: ...


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TaskOptions:
    retry: RetryConfig | None = None
    name: str | None = None
    on_success: Callable[[], Awaitable[None]] | None = None
    on_failure: Callable[[BaseException], Awaitable[None]] | None = None
    on_retry: Callable[[BaseException, int], Awaitable[None]] | None = None


class BoundTask(Generic[P, R]):
    """A coroutine function bound to a Worker with retry logic."""

    def __init__(
        self,
        fn: Any,
        worker: _WorkerProto,
        options: TaskOptions,
    ) -> None:
        self._fn = fn
        self._worker = worker
        self._options = options
        self.__name__: str = options.name or fn.__name__
        self.__doc__ = fn.__doc__

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Awaitable[R]:
        """Direct await — bypasses the worker queue, useful in tests."""
        return cast("Awaitable[R]", self._fn(*args, **kwargs))

    def enqueue(self, *args: P.args, **kwargs: P.kwargs) -> None:
        """Schedule the task on the worker queue (sync, non-blocking)."""
        self._worker._enqueue(self, args, kwargs)  # noqa: SLF001

    async def _invoke_hook(self, hook_name: str, coro: Awaitable[None]) -> None:
        """Run a hook coroutine, logging and swallowing any exception it raises."""
        try:
            await coro
        except Exception:
            logger.exception("%s callback raised for task %r", hook_name, self.__name__)

    async def _run_with_retry(
        self,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        retry = self._options.retry or RetryConfig()
        last_exc: BaseException | None = None

        for attempt in range(retry.max_attempts):
            try:
                await self._fn(*args, **kwargs)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                last_exc = exc
                remaining = retry.max_attempts - attempt - 1
                if remaining > 0:
                    delay = retry.compute_delay(attempt)
                    logger.warning(
                        "Task %r failed (attempt %d/%d), retrying in %.2fs: %s",
                        self.__name__,
                        attempt + 1,
                        retry.max_attempts,
                        delay,
                        exc,
                    )
                    if self._options.on_retry is not None:
                        await self._invoke_hook(
                            "on_retry",
                            self._options.on_retry(exc, attempt + 1),
                        )
                    await asyncio.sleep(delay)
                else:
                    logger.exception(
                        "Task %r failed after %d attempt(s)",
                        self.__name__,
                        retry.max_attempts,
                    )
            else:
                if self._options.on_success is not None:
                    await self._invoke_hook("on_success", self._options.on_success())
                return

        assert last_exc is not None
        if self._options.on_failure is not None:
            await self._invoke_hook("on_failure", self._options.on_failure(last_exc))
        raise MaxRetriesExceededError(
            f"Task {self.__name__!r} exceeded {retry.max_attempts} attempt(s)",
        ) from last_exc
