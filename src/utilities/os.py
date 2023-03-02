from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager, suppress
from os import cpu_count, environ, getenv
from typing import Optional, cast

from beartype import beartype


@beartype
def _get_cpu_count() -> int:
    """Get the CPU count."""
    count = cpu_count()
    if count is None:  # pragma: no cover
        raise UnableToDetermineCPUCountError
    return count


class UnableToDetermineCPUCountError(ValueError):
    """Raised when unable to determine the CPU count."""


CPU_COUNT = _get_cpu_count()


@contextmanager
@beartype
def temp_environ(
    env: Optional[Mapping[str, Optional[str]]] = None, **env_kwargs: Optional[str]
) -> Iterator[None]:
    """Context manager with temporary environment variable set."""
    all_env = (cast(dict[str, Optional[str]], {}) if env is None else env) | env_kwargs
    prev = list(zip(all_env, map(getenv, all_env)))
    _apply_environment(all_env.items())
    try:
        yield
    finally:
        _apply_environment(prev)


@beartype
def _apply_environment(items: Iterable[tuple[str, Optional[str]]], /) -> None:
    for key, value in items:
        if value is None:
            with suppress(KeyError):
                del environ[key]
        else:
            environ[key] = value
