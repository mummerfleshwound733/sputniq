class SputniqError(Exception):
    """Base exception for sputniq."""


class WorkerNotRunningError(SputniqError):
    """Raised when enqueue is called before start() or after stop()."""


class MaxRetriesExceededError(SputniqError):
    """Raised when all retry attempts are exhausted."""
