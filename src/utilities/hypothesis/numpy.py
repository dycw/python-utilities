from typing import Optional

from beartype import beartype
from hypothesis.extra.numpy import array_shapes
from hypothesis.extra.numpy import arrays
from hypothesis.extra.numpy import from_dtype
from hypothesis.strategies import DrawFn
from hypothesis.strategies import SearchStrategy
from hypothesis.strategies import booleans
from hypothesis.strategies import composite
from hypothesis.strategies import floats
from hypothesis.strategies import integers
from hypothesis.strategies import nothing
from numpy import bool_
from numpy import dtype
from numpy import float64
from numpy import iinfo
from numpy import int64
from numpy.typing import NDArray

from utilities.hypothesis import lift_draw
from utilities.hypothesis.typing import MaybeSearchStrategy
from utilities.hypothesis.typing import Shape

_ARRAY_SHAPES = array_shapes()


@composite
def bool_arrays(
    _draw: DrawFn,
    /,
    *,
    shape: MaybeSearchStrategy[Shape] = _ARRAY_SHAPES,
    unique: MaybeSearchStrategy[bool] = False,
) -> NDArray[bool_]:
    """Strategy for generating arrays of booleans."""
    draw = lift_draw(_draw)
    return draw(
        arrays(
            bool,
            draw(shape),
            elements=booleans(),
            fill=nothing(),
            unique=draw(unique),
        )
    )


@composite
def float_arrays(
    _draw: DrawFn,
    /,
    *,
    shape: MaybeSearchStrategy[Shape] = _ARRAY_SHAPES,
    min_value: MaybeSearchStrategy[Optional[float]] = None,
    max_value: MaybeSearchStrategy[Optional[float]] = None,
    allow_nan: MaybeSearchStrategy[Optional[bool]] = None,
    allow_infinity: MaybeSearchStrategy[Optional[bool]] = None,
    unique: MaybeSearchStrategy[bool] = False,
) -> NDArray[float64]:
    """Strategy for generating arrays of floats."""
    draw = lift_draw(_draw)
    elements = floats(
        min_value=draw(min_value),
        max_value=draw(max_value),
        allow_nan=draw(allow_nan),
        allow_infinity=draw(allow_infinity),
    )
    return draw(
        arrays(
            float,
            draw(shape),
            elements=elements,
            fill=nothing(),
            unique=draw(unique),
        )
    )


@composite
def int_arrays(
    _draw: DrawFn,
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
        arrays(
            int,
            draw(shape),
            elements=elements,
            fill=nothing(),
            unique=draw(unique),
        )
    )


@beartype
def int64s() -> SearchStrategy[int]:
    """Strategy for generating int64s."""
    return from_dtype(dtype(int64)).map(int)
