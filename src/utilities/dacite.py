from __future__ import annotations

from dataclasses import fields
from typing import TYPE_CHECKING, Any, Literal

from utilities.iterables import one
from utilities.typing import get_args, get_type_hints, is_optional_type

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import Dataclass, StrMapping


try:  # skipif-version-ge-312
    from typing import TypeAliasType  # pyright: ignore[reportAttributeAccessIssue]
except ImportError:  # pragma: no cover
    TypeAliasType = None


def yield_literal_forward_references(
    cls: type[Dataclass],
    /,
    *,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
) -> Iterator[tuple[str, Any]]:
    """Yield forward references."""
    hints = get_type_hints(cls, globalns=globalns, localns=localns)
    for fld in filter(lambda f: isinstance(f.type, str), fields(cls)):
        type_ = hints[fld.name]
        result = _yield_literal_forward_references_core(type_)
        if result is not None:  # pragma: no cover
            yield result


def _yield_literal_forward_references_core(obj: Any, /) -> tuple[str, Any] | None:
    """Yield forward references."""
    if (TypeAliasType is not None) and isinstance(  # pragma: no cover
        obj, TypeAliasType
    ):
        return obj.__name__, Literal[get_args(obj)]
    if is_optional_type(obj):
        return _yield_literal_forward_references_core(one(get_args(obj)))
    return None


__all__ = ["yield_literal_forward_references"]
