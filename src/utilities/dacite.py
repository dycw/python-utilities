from __future__ import annotations

from dataclasses import fields
from typing import TYPE_CHECKING, Any, Literal, get_type_hints

from utilities.iterables import one
from utilities.typing import get_args, is_optional_type

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.dataclasses import Dataclass
    from utilities.types import StrMapping


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
    hints = get_type_hints(
        cls,
        globalns=globals() if globalns is None else dict(globalns),
        localns=locals() if localns is None else dict(localns),
    )
    for fld in fields(cls):
        if isinstance(fld.type, str):
            type_ = hints[fld.name]
            result = _yield_literal_forward_references_core(type_)
            if result is not None:
                yield result


def _yield_literal_forward_references_core(obj: Any, /) -> tuple[str, Any] | None:
    """Yield forward references."""
    if (TypeAliasType is not None) and isinstance(  # skipif-version-ge-312
        obj, TypeAliasType
    ):
        return obj.__name__, Literal[get_args(obj)]
    if is_optional_type(obj):
        return _yield_literal_forward_references_core(one(get_args(obj)))
    return None
