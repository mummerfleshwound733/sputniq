"""dishka DI integration for sputniq.

Requires dishka to be installed::

    pip install dishka

Usage::

    from dishka import AsyncContainer, FromDishka, make_async_container
    from sputniq import Worker
    from sputniq.integrations.dishka import inject, setup_dishka

    container = make_async_container(MyProvider())
    worker = Worker()
    setup_dishka(worker, container)

    @worker.task
    @inject
    async def send_email(to: str, mailer: FromDishka[Mailer]) -> None:
        await mailer.send(to)

Note on typing: ``inject`` preserves the original function signature via a
TypeVar, so static checkers will still show ``FromDishka``-annotated parameters
at call sites.  This is a known limitation shared by all dishka integrations.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sputniq._worker import Worker

try:
    from dishka import AsyncContainer
    from dishka.integrations.base import wrap_injection
except ImportError as _err:
    raise ImportError(
        "dishka is required for sputniq.integrations.dishka. "
        "Install it with: pip install dishka",
    ) from _err


__all__ = ["DishkaMiddleware", "inject", "setup_dishka"]

_container_var: ContextVar[AsyncContainer] = ContextVar("sputniq_dishka_container")


class DishkaMiddleware:
    """Task middleware that opens a per-task dishka request scope.

    Pass an instance to ``Worker(task_middleware=...)``.  For every task
    execution a fresh child container is entered, its reference stored in a
    ``ContextVar``, and the scope is closed when the task finishes (even on
    error or cancellation).
    """

    def __init__(self, container: AsyncContainer) -> None:
        self._container = container

    async def __call__(self, call_next: Callable[[], Awaitable[None]]) -> None:
        async with self._container() as request_container:
            _container_var.set(request_container)
            await call_next()


def setup_dishka(worker: Worker, container: AsyncContainer) -> None:
    """Register a per-task dishka request scope on *worker*.

    Equivalent to ``worker.add_middleware(DishkaMiddleware(container))``.
    """
    worker.add_middleware(DishkaMiddleware(container))


def inject(func: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap a task coroutine so dishka resolves its ``FromDishka`` parameters.

    Apply *inside* ``@worker.task``::

        @worker.task
        @inject
        async def my_task(x: int, dep: FromDishka[MyService]) -> None: ...

    The container is read from the per-task ``ContextVar`` set by
    ``DishkaMiddleware``, so ``DishkaMiddleware`` must be configured on the
    worker.
    """
    return wrap_injection(
        func=func,
        is_async=True,
        container_getter=lambda _args, _kwargs: _container_var.get(),
    )
