from __future__ import annotations

import dataclasses
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any, Generic, cast, overload

from sputniq._retry import RetryConfig
from sputniq._task import BoundTask, TaskOptions
from sputniq._types import P, R
from sputniq._worker import Worker


class RegistryTask(Generic[P, R]):
    def __init__(
        self,
        fn: Callable[P, Coroutine[Any, Any, R]],
        options: TaskOptions,
    ) -> None:
        self._fn = fn
        self._options = options
        self._bound: BoundTask[P, R] | None = None
        self.__name__: str = options.name or fn.__name__  # ty: ignore[unresolved-attribute]
        self.__doc__ = fn.__doc__

    def _bind(self, worker: Worker) -> None:
        options = self._options
        if options.retry is None:
            options = dataclasses.replace(options, retry=worker._default_retry)  # noqa: SLF001
        self._bound = BoundTask(self._fn, worker, options)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Awaitable[R]:
        return cast("Awaitable[R]", self._fn(*args, **kwargs))

    def enqueue(self, *args: P.args, **kwargs: P.kwargs) -> None:
        if self._bound is None:
            raise RuntimeError(
                f"Task {self.__name__!r} is not bound to any worker. "
                "Call worker.include_registry() first.",
            )
        self._bound.enqueue(*args, **kwargs)


class TaskRegistry:
    """A collection of tasks that can be mounted onto a Worker via include_registry.

    Allows defining tasks without a global worker instance::

        registry = TaskRegistry()

        @registry.task(retry=RetryConfig(max_attempts=3))
        async def send_email(to: str) -> None: ...

    Then in your app setup::

        worker = Worker()
        worker.include_registry(registry)
    """

    def __init__(self) -> None:
        self._tasks: list[RegistryTask[Any, Any]] = []

    @overload
    def task(self, fn: Callable[P, Coroutine[Any, Any, R]]) -> RegistryTask[P, R]: ...

    @overload
    def task(
        self,
        *,
        retry: RetryConfig | None = ...,
        name: str | None = ...,
        on_success: Callable[[], Awaitable[None]] | None = ...,
        on_failure: Callable[[BaseException], Awaitable[None]] | None = ...,
        on_retry: Callable[[BaseException, int], Awaitable[None]] | None = ...,
    ) -> Callable[[Callable[P, Coroutine[Any, Any, R]]], RegistryTask[P, R]]: ...

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
        RegistryTask[P, R]
        | Callable[[Callable[P, Coroutine[Any, Any, R]]], RegistryTask[P, R]]
    ):
        options = TaskOptions(
            retry=retry,
            name=name,
            on_success=on_success,
            on_failure=on_failure,
            on_retry=on_retry,
        )

        def decorator(f: Callable[P, Coroutine[Any, Any, R]]) -> RegistryTask[P, R]:
            reg_task: RegistryTask[P, R] = RegistryTask(f, options)
            self._tasks.append(reg_task)
            return reg_task

        if fn is not None:
            return decorator(fn)
        return decorator
