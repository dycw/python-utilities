from __future__ import annotations

from typing import cast

from inflect import Word, engine

_ENGINE = engine()


def counted_noun(num: int, noun: str, /) -> str:
    """Construct a counted noun."""
    word = cast("Word", noun)
    sin_or_plu = _ENGINE.plural_noun(word, count=num)
    return f"{num} {sin_or_plu}"


__all__ = ["counted_noun"]
