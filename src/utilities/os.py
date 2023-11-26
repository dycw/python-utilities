from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager, suppress
from os import cpu_count, environ, getenv


def get_cpu_count() -> int:
    """Get the CPU count."""
    count = cpu_count()
    if count is None:  # pragma: no cover
        msg = f"{count=}"
        raise GetCPUCountError(msg)
    return count


class GetCPUCountError(Exception):
    ...


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
