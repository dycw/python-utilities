from __future__ import annotations

from contextlib import contextmanager, suppress
from dataclasses import dataclass
from os import cpu_count, environ, getenv
from typing import TYPE_CHECKING

from typing_extensions import override

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


__all__ = ["CPU_COUNT", "GetCPUCountError", "get_cpu_count", "temp_environ"]
