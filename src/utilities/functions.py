from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, TypeVar

from typing_extensions import override

_T = TypeVar("_T")


class _HasName(Protocol):
    @property
    def name(self) -> Any: ...  # pragma: no cover


def check_name(obj: _HasName, name: Any, /) -> None:
    """Check if an object has the required name."""

    if obj.name != name:
        raise CheckNameError(obj=obj, name=name)


@dataclass(kw_only=True)
class CheckNameError(Exception):
    obj: _HasName
    name: Any

    @override
    def __str__(self) -> str:
        return f"Object must have name {self.name}; got {self.obj.name}."


def identity(obj: _T, /) -> _T:
    """The identity function."""
    return obj


__all__ = ["CheckNameError", "check_name", "identity"]
