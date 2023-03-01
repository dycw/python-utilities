from typing import NoReturn
from typing import Union

Number = Union[float, int]


def never(x: NoReturn, /) -> NoReturn:
    """Never return. Used for exhaustive pattern matching."""
    msg = f'"never" was run with {x}'
    raise NeverError(msg)


class NeverError(Exception):
    """Raised when `never` is run."""
