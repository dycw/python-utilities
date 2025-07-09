from __future__ import annotations

from collections.abc import Sized
from typing import assert_never, cast

from inflect import Word, engine

_ENGINE = engine()


def counted_noun(obj: int | Sized, noun: str, /) -> str:
    """Construct a counted noun."""
    match obj:
        case int() as count:
            ...
        case Sized() as sized:
            count = len(sized)
        case _ as never:
            assert_never(never)
    word = cast("Word", noun)
    sin_or_plu = _ENGINE.plural_noun(word, count=count)
    return f"{count} {sin_or_plu}"


__all__ = ["counted_noun"]
