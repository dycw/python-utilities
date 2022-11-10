from typing import NoReturn


def never(x: NoReturn, /) -> NoReturn:
    """Never return. Used for exhaustive pattern matching."""

    raise Never(f'"never" was run with {x}')


class Never(Exception):
    """Raised when `never` is run."""
