from __future__ import annotations

from contextlib import contextmanager, suppress
from dataclasses import dataclass
from os import cpu_count, environ, getenv
from typing import TYPE_CHECKING, assert_never

from typing_extensions import override

from utilities.iterables import (
    _OneStrCaseInsensitiveEmptyError,
    _OneStrCaseSensitiveEmptyError,
    one_str,
)

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping


def get_cpu_count() -> int:
    """Get the CPU count."""
    count = cpu_count()
    if count is None:  # pragma: no cover
        raise GetCPUCountError
    return count


@dataclass(kw_only=True)
class GetCPUCountError(Exception):
    @override
    def __str__(self) -> str:
        return "CPU count must not be None"  # pragma: no cover


CPU_COUNT = get_cpu_count()


def get_env_var(key: str, /, *, case_sensitive: bool = True) -> str | None:
    """Get an environment variable."""
    match case_sensitive:
        case True:
            error = _OneStrCaseSensitiveEmptyError
        case False:
            error = _OneStrCaseInsensitiveEmptyError
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)
    try:
        key_use = one_str(environ, key, case_sensitive=case_sensitive)
    except error:
        return None
    return environ[key_use]


@contextmanager
def temp_environ(
    env: Mapping[str, str | None] | None = None, **env_kwargs: str | None
) -> Iterator[None]:
    """Context manager with temporary environment variable set."""
    mapping: dict[str, str | None] = ({} if env is None else dict(env)) | env_kwargs
    prev = {key: getenv(key) for key in mapping}

    def apply(mapping: Mapping[str, str | None], /) -> None:
        for key, value in mapping.items():
            if value is None:
                with suppress(KeyError):
                    del environ[key]
            else:
                environ[key] = value

    apply(mapping)
    try:
        yield
    finally:
        apply(prev)


__all__ = [
    "CPU_COUNT",
    "GetCPUCountError",
    "get_cpu_count",
    "get_env_var",
    "temp_environ",
]
