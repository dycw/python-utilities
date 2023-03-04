from typing import Any, Optional

from beartype import beartype
from bottleneck import push

from utilities.iterables import is_iterable_not_str
from utilities.numpy.typing import NDArrayF, datetime64D, datetime64ns, datetime64Y

_ = (datetime64D, datetime64Y, datetime64ns)


@beartype
def has_dtype(x: Any, dtype: Any, /) -> bool:
    """Check if an object has the required dtype."""
    if is_iterable_not_str(dtype):
        return any(has_dtype(x, d) for d in dtype)
    return x.dtype == dtype


@beartype
def ffill(
    array: NDArrayF, /, *, axis: Optional[int] = None, limit: Optional[int] = None
) -> NDArrayF:
    """Forward fill the elements in an array."""
    return push(array, n=limit, axis=-1 if axis is None else axis)
