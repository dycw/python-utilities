from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager, suppress
from os import cpu_count, environ, getenv


def _get_cpu_count() -> int:
    """Get the CPU count."""
    count = cpu_count()
    if count is None:  # pragma: no cover
        raise UnableToDetermineCPUCountError
    return count


class UnableToDetermineCPUCountError(Exception):
    """Raised when unable to determine the CPU count."""


CPU_COUNT = _get_cpu_count()


@contextmanager
def temp_environ(
    env: Mapping[str, str | None] | None = None, **env_kwargs: str | None
) -> Iterator[None]:
    """Context manager with temporary environment variable set."""
    all_env: dict[str, str | None] = ({} if env is None else dict(env)) | env_kwargs
    prev = list(zip(all_env, map(getenv, all_env), strict=True))
    _apply_environment(all_env.items())
    try:
        yield
    finally:
        _apply_environment(prev)


def _apply_environment(items: Iterable[tuple[str, str | None]], /) -> None:
    for key, value in items:
        if value is None:
            with suppress(KeyError):
                del environ[key]
        else:
            environ[key] = value


__all__ = ["CPU_COUNT", "temp_environ", "UnableToDetermineCPUCountError"]
