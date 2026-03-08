from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True)
class RetryConfig:
    """Configuration for task retry behaviour with exponential backoff."""

    max_attempts: int = 3
    initial_delay: float = 0.5
    max_delay: float = 60.0
    backoff_factor: float = 2.0
    jitter: bool = True

    def compute_delay(self, attempt: int) -> float:
        """Compute delay in seconds before the given attempt (0-indexed).

        attempt=0 means before the first retry (after 1st failure).
        """
        delay = min(self.initial_delay * (self.backoff_factor**attempt), self.max_delay)
        if self.jitter:
            delay = random.uniform(0, delay)  # noqa: S311
        return delay
