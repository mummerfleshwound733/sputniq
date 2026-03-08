# sputniq

[![PyPI version](https://img.shields.io/pypi/v/sputniq)](https://pypi.org/project/sputniq/)
[![Python versions](https://img.shields.io/pypi/pyversions/sputniq)](https://pypi.org/project/sputniq/)
[![Tests](https://github.com/{owner}/sputniq/actions/workflows/pr_tests.yaml/badge.svg)](https://github.com/{owner}/sputniq/actions)
[![Coverage](https://codecov.io/gh/{owner}/sputniq/branch/main/graph/badge.svg)](https://codecov.io/gh/{owner}/sputniq)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![mypy](https://www.mypy-lang.org/static/mypy_badge.svg)](https://mypy-lang.org/)
[![ty](https://img.shields.io/badge/type--checked-ty-blue)](https://github.com/astral-sh/ty)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](https://opensource.org/licenses/MIT)

In-process async background task runner for Python.

## The problem

Running background tasks in an async Python application has an awkward gap between two extremes:

**Too heavy:** Celery and taskiq are full-featured distributed task queues. They require a separate worker process, a broker (Redis, RabbitMQ, etc.), and significant operational overhead. For many use cases — sending a welcome email, invalidating a cache, firing a webhook — this is massive overkill.

**Too light:** Starlette's `BackgroundTasks` runs coroutines after the response is sent, but it has no graceful shutdown (tasks are abandoned on process exit), no concurrency control, no retry logic, and no way to integrate with dependency injection frameworks like [dishka](https://github.com/reagento/dishka).

sputniq sits in the middle: it runs tasks in-process, within the same asyncio event loop as your application, with the features you actually need.

## Features

- **Graceful shutdown** — on application shutdown, the worker stops accepting new tasks and waits for running tasks to complete
- **Concurrency limits** — cap the number of tasks running at the same time
- **Retry logic** — configurable retry policy with exponential backoff and jitter per task
- **Observability hooks** — `on_success`, `on_failure`, `on_retry` callbacks for logging, metrics, and custom persistence
- **DI support** — first-class integration with [dishka](https://github.com/reagento/dishka); other DI containers are straightforward to wire up via the `task_middleware` hook
- **Framework-agnostic** — the core is pure asyncio; use `async with worker:` for lifecycle management in any framework

## How it compares

| | sputniq | Celery / taskiq | Starlette `BackgroundTasks` | APScheduler |
|---|---|---|---|---|
| In-process | yes | no (separate worker) | yes | yes |
| External broker required | no | yes | no | no |
| Graceful shutdown | yes | yes | no | partial |
| Concurrency control | yes | yes | no | yes |
| Retry logic | yes | yes | no | yes |
| DI integration (dishka) | first-class | workarounds | no | workarounds |
| Observability hooks | yes | yes | no | partial |
| Core primitive | run now, in background | distribute to worker | run after response | run at a time/interval |

**vs. APScheduler specifically:** APScheduler answers "when should this run" (cron, interval, one-shot triggers). sputniq answers "run this now, in the background, safely." APScheduler carries significant machinery — job stores, executors, trigger systems, serialization — that is dead weight when you just want to offload work during a request. DI support in APScheduler is essentially nonexistent; scoped dependencies per task execution require ugly workarounds. The two can coexist: APScheduler triggers a job that submits a task to sputniq.

## Non-goals

sputniq does **not** provide persistence or delivery guarantees. Tasks live in memory; if the process dies, enqueued tasks are lost. This is by design — if you need at-least-once delivery, use a broker. The observability hooks make it straightforward to implement your own persistence layer on top.

## Installation

```
pip install sputniq
```

## Quick start

```python
import asyncio
from sputniq import RetryConfig, Worker

worker = Worker(max_concurrency=10)


@worker.task(retry=RetryConfig(max_attempts=3))
async def send_email(to: str, subject: str) -> None: ...


# Enqueue from anywhere in your application (sync, non-blocking)
send_email.enqueue("user@example.com", "Welcome!")
```

## Observability hooks

Attach async callbacks to individual tasks for logging, metrics, or persistence:

```python
async def on_success() -> None:
    metrics.increment("tasks.success")


async def on_failure(exc: BaseException) -> None:
    logger.error("Task failed permanently: %s", exc)
    await db.save_failed_task(exc)


async def on_retry(exc: BaseException, attempt: int) -> None:
    logger.warning("Retry %d: %s", attempt, exc)


@worker.task(
    retry=RetryConfig(max_attempts=3),
    on_success=on_success,
    on_failure=on_failure,
    on_retry=on_retry,
)
async def process(item: str) -> None: ...
```

- `on_success` — called after a successful execution
- `on_failure(exc)` — called once when all retry attempts are exhausted
- `on_retry(exc, attempt)` — called before each retry (attempt is 1-based)

Exceptions raised inside a callback are logged and swallowed; they do not affect the worker.

## Lifecycle management

`Worker` is an async context manager. Use `async with worker:` to start and stop it:

```python
async with worker:
    ...  # your app runs here
```

For frameworks that accept an async context manager in their lifespan hook (FastAPI, Litestar, etc.):

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from sputniq import Worker

worker = Worker()


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with worker:
        yield


app = FastAPI(lifespan=lifespan)
```

## TaskRegistry

`TaskRegistry` lets you define tasks without a global `Worker` instance — useful for splitting tasks across modules.

```python
# emails/tasks.py
from sputniq import TaskRegistry, RetryConfig

registry = TaskRegistry()


@registry.task(retry=RetryConfig(max_attempts=3))
async def send_email(to: str, subject: str) -> None: ...
```

```python
# main.py
from sputniq import Worker
from emails.tasks import registry as email_registry

worker = Worker()
worker.include_registry(email_registry)
```

After `include_registry`, all tasks on the registry are bound to the worker and `.enqueue()` works normally. Multiple registries can be included into the same worker.

## dishka integration

```python
from dishka import make_async_container, FromDishka
from sputniq import Worker
from sputniq.integrations.dishka import DishkaMiddleware, inject

container = make_async_container(MyProvider())
worker = Worker(task_middleware=DishkaMiddleware(container))


@worker.task
@inject
async def send_email(to: str, mailer: FromDishka[Mailer]) -> None:
    await mailer.send(to)
```

Dependencies declared with `FromDishka` are resolved from a fresh request-scoped container for each task execution. The container is closed when the task finishes, even on error.

## License

MIT
