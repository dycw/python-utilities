from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, assert_never, override

from utilities.constants import CPU_COUNT

if TYPE_CHECKING:
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


__all__ = ["GetCPUUseError", "get_cpu_use"]
