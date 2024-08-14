from __future__ import annotations

from contextlib import contextmanager
from locale import LC_CTYPE, LC_NUMERIC, getlocale, setlocale
from locale import atof as _atof
from locale import atoi as _atoi
from typing import TYPE_CHECKING, assert_never

from utilities.platform import SYSTEM, System

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator

    from utilities.iterables import MaybeIterable


def get_locale_for_platform(locale: str, /) -> str:
    """Get the platform-dependent locale."""
    match SYSTEM:
        case System.windows:  # os-ne-windows
            return locale
        case System.mac:  # os-ne-macos
            return locale
        case System.linux:  # os-ne-linux
            return f"{locale}.utf8"
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)


@contextmanager
def override_locale(
    *, category: int = LC_CTYPE, locale: str | Iterable[str | None] | None = None
) -> Iterator[None]:
    prev = getlocale(category)
    _ = setlocale(category, locale=locale)
    yield
    _ = setlocale(category, prev)


def atof(
    text: str,
    /,
    *,
    locale: MaybeIterable[str | None] = None,
    func: Callable[[str], float] = float,
) -> float:
    with override_locale(category=LC_NUMERIC, locale=locale):
        return _atof(text, func=func)


def atoi(text: str, /, *, locale: MaybeIterable[str | None] = None) -> float:
    with override_locale(category=LC_NUMERIC, locale=locale):
        return _atoi(text)


__all__ = ["atof", "atoi", "get_locale_for_platform", "override_locale"]
