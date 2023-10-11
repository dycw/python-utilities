from __future__ import annotations

from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Iterator
from contextlib import contextmanager
from locale import LC_NUMERIC
from locale import atof as _atof
from locale import getlocale
from locale import setlocale

from utilities.platform import SYSTEM
from utilities.platform import System
from utilities.typing import never


def get_locale_for_platform(locale: str, /) -> str:
    """Get the platform-dependent locale."""
    if SYSTEM is System.windows:  # pragma: os-ne-windows
        raise NotImplementedError
    if SYSTEM is System.mac_os:  # pragma: os-ne-macos
        return locale
    if SYSTEM is System.linux:  # pragma: os-ne-linux
        return f"{locale}.utf8"
    return never(SYSTEM)  # pragma: no cover


@contextmanager
def override_locale(
    category: int, /, *, locale: str | Iterable[str | None] | None = None
) -> Iterator[None]:
    prev = getlocale(category)
    _ = setlocale(category, locale=locale)
    yield
    _ = setlocale(category, prev)


def atof(
    text: str,
    /,
    *,
    locale: str | Iterable[str | None] | None = None,
    func: Callable[[str], float] = float,
) -> float:
    with override_locale(LC_NUMERIC, locale=locale):
        return _atof(text, func=func)
