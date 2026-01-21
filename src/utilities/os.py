from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from os import environ, getenv
from typing import TYPE_CHECKING, assert_never, override

from utilities.constants import CPU_COUNT
from utilities.contextlib import enhanced_context_manager
from utilities.core import get_env

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from utilities.types import IntOrAll


def get_cpu_use(*, n: IntOrAll = "all") -> int:
    """Resolve for the number of CPUs to use."""
    match n:
        case int():
            if n >= 1:
                return n
            raise GetCPUUseError(n=n)
        case "all":
            return CPU_COUNT
        case never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class GetCPUUseError(Exception):
    n: int

    @override
    def __str__(self) -> str:
        return f"Invalid number of CPUs to use: {self.n}"


##


def is_debug() -> bool:
    """Check if we are in `DEBUG` mode."""
    return get_env("DEBUG", nullable=True) is not None


##


def is_pytest() -> bool:
    """Check if `pytest` is running."""
    return get_env("PYTEST_VERSION", nullable=True) is not None


##


@enhanced_context_manager
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


__all__ = ["GetCPUUseError", "get_cpu_use", "is_debug", "is_pytest", "temp_environ"]
