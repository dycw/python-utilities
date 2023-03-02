from typing import Any, Optional

from beartype import beartype
from hypothesis.extra.numpy import array_shapes, arrays, from_dtype
from hypothesis.strategies import SearchStrategy, booleans, composite, integers, nothing
from numpy import bool_, dtype, float64, iinfo, int64
from numpy.typing import NDArray

from utilities.hypothesis import floats_extra, lift_draw
from utilities.hypothesis.typing import MaybeSearchStrategy, Shape

_ARRAY_SHAPES = array_shapes()


@composite
@beartype
def bool_arrays(
    _draw: Any,
    /,
    *,
    shape: MaybeSearchStrategy[Shape] = _ARRAY_SHAPES,
    fill: Optional[SearchStrategy[Any]] = None,
    unique: MaybeSearchStrategy[bool] = False,
) -> NDArray[bool_]:
    """Strategy for generating arrays of booleans."""
    draw = lift_draw(_draw)
    return draw(
        arrays(bool, draw(shape), elements=booleans(), fill=fill, unique=draw(unique))
    )


@composite
@beartype
def float_arrays(
    _draw: Any,
    /,
    *,
    shape: MaybeSearchStrategy[Shape] = _ARRAY_SHAPES,
    min_value: MaybeSearchStrategy[Optional[float]] = None,
    max_value: MaybeSearchStrategy[Optional[float]] = None,
    allow_nan: MaybeSearchStrategy[bool] = False,
    allow_inf: MaybeSearchStrategy[bool] = False,
    allow_pos_inf: MaybeSearchStrategy[bool] = False,
    allow_neg_inf: MaybeSearchStrategy[bool] = False,
    integral: MaybeSearchStrategy[bool] = False,
    fill: Optional[SearchStrategy[Any]] = None,
    unique: MaybeSearchStrategy[bool] = False,
) -> NDArray[float64]:
    """Strategy for generating arrays of floats."""
    draw = lift_draw(_draw)
    elements = floats_extra(
        min_value=min_value,
        max_value=max_value,
        allow_nan=allow_nan,
        allow_inf=allow_inf,
        allow_pos_inf=allow_pos_inf,
        allow_neg_inf=allow_neg_inf,
        integral=integral,
    )
    return draw(
        arrays(float, draw(shape), elements=elements, fill=fill, unique=draw(unique))
    )


@composite
@beartype
def int_arrays(
    _draw: Any,
    /,
    *,
    shape: MaybeSearchStrategy[Shape] = _ARRAY_SHAPES,
    min_value: MaybeSearchStrategy[Optional[int]] = None,
    max_value: MaybeSearchStrategy[Optional[int]] = None,
    unique: MaybeSearchStrategy[bool] = False,
) -> NDArray[int64]:
    """Strategy for generating arrays of ints."""
    draw = lift_draw(_draw)
    info = iinfo(int64)
    min_value_, max_value_ = draw(min_value), draw(max_value)
    min_value_use = info.min if min_value_ is None else min_value_
    max_value_use = info.max if max_value_ is None else max_value_
    elements = integers(min_value=min_value_use, max_value=max_value_use)
    return draw(
        arrays(int, draw(shape), elements=elements, fill=nothing(), unique=draw(unique))
    )


@beartype
def int64s() -> SearchStrategy[int]:
    """Strategy for generating int64s."""
    return from_dtype(dtype(int64)).map(int)
