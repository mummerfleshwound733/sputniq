from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import ParamSpec, TypeAlias, TypeVar

P = ParamSpec("P")
R = TypeVar("R")

TaskMiddleware: TypeAlias = Callable[[Callable[[], Awaitable[None]]], Awaitable[None]]
