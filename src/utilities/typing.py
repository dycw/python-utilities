from __future__ import annotations

from typing import NoReturn

Number = float | int


def never(x: NoReturn, /) -> NoReturn:
    """Never return. Used for exhaustive pattern matching."""
    msg = f'"never" was run with {x}'
    raise NeverError(msg)


class NeverError(Exception):
    """Raised when `never` is run."""
