import asyncio
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field

import pytest

dishka = pytest.importorskip("dishka", reason="dishka not installed")

from dishka import (
    AsyncContainer,
    FromDishka,
    Provider,
    Scope,
    make_async_container,
    provide,
)

from sputniq import RetryConfig, Worker
from sputniq.integrations.dishka import DishkaMiddleware, inject, setup_dishka


@dataclass
class Greeter:
    prefix: str = "Hello"

    def greet(self, name: str) -> str:
        return f"{self.prefix}, {name}!"


@dataclass
class Counter:
    calls: list[str] = field(default_factory=list)

    def record(self, tag: str) -> None:
        self.calls.append(tag)


@dataclass
class TaskId:
    value: int


class AppProvider(Provider):
    scope = Scope.REQUEST

    @provide
    def greeter(self) -> Greeter:
        return Greeter()


class AppProviderWithCounter(Provider):
    scope = Scope.REQUEST

    def __init__(self, counter: Counter) -> None:
        super().__init__()
        self._counter = counter

    @provide
    def greeter(self) -> Greeter:
        self._counter.record("created")
        return Greeter()


class TaskIdProvider(Provider):
    scope = Scope.REQUEST

    def __init__(self) -> None:
        super().__init__()
        self._n = 0

    @provide
    def task_id(self) -> TaskId:
        self._n += 1
        return TaskId(self._n)


@pytest.fixture
def container() -> AsyncContainer:
    return make_async_container(AppProvider())


@pytest.fixture
async def di_worker(container: AsyncContainer) -> AsyncGenerator[Worker]:
    w = Worker(task_middleware=DishkaMiddleware(container))
    await w.start()
    yield w
    await w.stop(timeout=5.0)


async def test_inject_resolves_dependency(di_worker: Worker) -> None:
    results: list[str] = []

    @di_worker.task
    @inject
    async def greet(name: str, greeter: FromDishka[Greeter]) -> None:
        results.append(greeter.greet(name))

    greet.enqueue("World")
    await di_worker.stop(timeout=5.0)

    assert results == ["Hello, World!"]


async def test_inject_resolves_on_every_execution() -> None:
    counter = Counter()
    c = make_async_container(AppProviderWithCounter(counter))
    w = Worker(task_middleware=DishkaMiddleware(c))
    await w.start()

    @w.task
    @inject
    async def greet(name: str, greeter: FromDishka[Greeter]) -> None:
        greeter.greet(name)

    greet.enqueue("Alice")
    greet.enqueue("Bob")
    await w.stop(timeout=5.0)
    await c.close()

    assert counter.calls == ["created", "created"]


async def test_scope_isolated_between_concurrent_tasks() -> None:
    seen_ids: list[int] = []
    barrier = asyncio.Event()

    c = make_async_container(TaskIdProvider())
    w = Worker(task_middleware=DishkaMiddleware(c), max_concurrency=5)
    await w.start()

    @w.task
    @inject
    async def record(task_id: FromDishka[TaskId]) -> None:
        await barrier.wait()
        seen_ids.append(task_id.value)

    for _ in range(5):
        record.enqueue()

    await asyncio.sleep(0.05)
    barrier.set()
    await w.stop(timeout=5.0)
    await c.close()

    assert len(seen_ids) == 5
    assert len(set(seen_ids)) == 5


async def test_direct_call_bypasses_middleware() -> None:
    c = make_async_container(AppProvider())
    w = Worker(task_middleware=DishkaMiddleware(c))

    @w.task
    @inject
    async def greet(name: str, greeter: FromDishka[Greeter]) -> None:
        pass

    with pytest.raises(LookupError):
        await greet("World")

    await c.close()


async def test_setup_dishka_registers_middleware() -> None:
    results: list[str] = []
    c = make_async_container(AppProvider())
    w = Worker()
    setup_dishka(w, c)
    await w.start()

    @w.task
    @inject
    async def greet(name: str, greeter: FromDishka[Greeter]) -> None:
        results.append(greeter.greet(name))

    greet.enqueue("World")
    await w.stop(timeout=5.0)
    await c.close()

    assert results == ["Hello, World!"]


async def test_middleware_missing_does_not_crash_worker() -> None:
    w = Worker()
    await w.start()

    @w.task(retry=RetryConfig(max_attempts=1))
    @inject
    async def broken(greeter: FromDishka[Greeter]) -> None:
        pass

    broken.enqueue()
    await w.stop(timeout=5.0)

    assert w._started is False  # noqa: SLF001
