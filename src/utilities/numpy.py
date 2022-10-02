from typing import Any

from beartype import beartype
from numpy import dtype

from utilities.iterables import is_iterable_not_str


datetime64ns = dtype("datetime64[ns]")


@beartype
def has_dtype(x: Any, dtype: Any, /) -> bool:
    """Check if an object has the required dtype."""

    if is_iterable_not_str(dtype):
        return any(has_dtype(x, d) for d in dtype)
    else:
        return x.dtype == dtype
