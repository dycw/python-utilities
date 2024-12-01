from __future__ import annotations

from dataclasses import fields
from typing import TYPE_CHECKING, Any, Literal, get_type_hints

from utilities.typing import get_args, is_literal_type

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.dataclasses import Dataclass
    from utilities.types import StrMapping


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
            if is_literal_type(type_):
                yield fld.type, Literal[get_args(type_)]
