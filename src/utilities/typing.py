from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, get_args, get_origin

from typing_extensions import override

try:  # pragma: version-ge-312
    from typing import TypeAliasType  # type: ignore[reportAttributeAccessIssue]
except ImportError:
    TypeAliasType = None


def get_literal_args(obj: Any, /) -> tuple[Any, ...]:
    """Get the arguments of a Literal."""
    if (TypeAliasType is not None) and isinstance(  # pragma: version-le-311
        obj, TypeAliasType
    ):
        return get_literal_args(obj.__value__)  # pragma: version-ge-312
    if (origin := get_origin(obj)) is not Literal:
        raise GetLiteralArgsError(obj=obj, origin=origin)
    return get_args(obj)


@dataclass(kw_only=True)
class GetLiteralArgsError(Exception):
    obj: Any
    origin: Any

    @override
    def __str__(self) -> str:
        return f"Object {self.obj} must be a Literal; got origin {self.origin} instead"


__all__ = ["GetLiteralArgsError", "get_literal_args"]
