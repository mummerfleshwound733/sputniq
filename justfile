default: lint static test

install:
    uv sync --group dev

lint:
    uv run ruff format sputniq tests
    uv run ruff check --fix sputniq tests

mypy:
    uv run mypy

ty:
    uv run ty check sputniq tests

static: mypy ty

test:
    uv run pytest

test-cov:
    uv run pytest --cov --cov-report=term-missing
