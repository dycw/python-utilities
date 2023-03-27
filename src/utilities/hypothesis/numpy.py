from typing import Any, Optional

from beartype import beartype
from hypothesis.extra.numpy import array_shapes, arrays, from_dtype
from hypothesis.strategies import SearchStrategy, booleans, composite, integers, none
from numpy import (
    concatenate,
    dtype,
    expand_dims,
    iinfo,
    int32,
    int64,
    uint32,
    uint64,
    zeros,
)
from numpy.typing import NDArray

from utilities.hypothesis import floats_extra, lift_draw, lists_fixed_length, text_ascii
from utilities.hypothesis.typing import MaybeSearchStrategy, Shape
from utilities.math.typing import IntNonNeg
from utilities.numpy import (
    EmptyNumpyConcatenateError,
    redirect_to_empty_numpy_concatenate_error,
)
from utilities.numpy.typing import NDArrayB, NDArrayF, NDArrayI, NDArrayO


@composite
@beartype
def bool_arrays(
    _draw: Any,
    /,
    *,
    shape: MaybeSearchStrategy[Shape] = array_shapes(),
    fill: Optional[SearchStrategy[Any]] = None,
    unique: MaybeSearchStrategy[bool] = False,
) -> NDArrayB:
    """Strategy for generating arrays of booleans."""
    draw = lift_draw(_draw)
    return draw(
        arrays(bool, draw(shape), elements=booleans(), fill=fill, unique=draw(unique))
    )


@composite
@beartype
def concatenated_arrays(
    _draw: Any,
    strategy: SearchStrategy[NDArray[Any]],
    size: MaybeSearchStrategy[IntNonNeg],
    fallback: Shape,
    /,
    *,
    dtype: Any = float,
) -> NDArray[Any]:
    """Strategy for generating arrays from lower-dimensional strategies."""
    draw = lift_draw(_draw)
    size_ = draw(size)
    arrays = draw(lists_fixed_length(strategy, size_))
    expanded = [expand_dims(array, axis=0) for array in arrays]
    try:
        return concatenate(expanded)
    except ValueError as error:
        try:
            redirect_to_empty_numpy_concatenate_error(error)
        except EmptyNumpyConcatenateError:
            if isinstance(fallback, int):
                shape = size_, fallback
            else:
                shape = (size_, *fallback)
            return zeros(shape, dtype=dtype)


@composite
@beartype
def float_arrays(
    _draw: Any,
    /,
    *,
    shape: MaybeSearchStrategy[Shape] = array_shapes(),
    min_value: MaybeSearchStrategy[Optional[float]] = None,
    max_value: MaybeSearchStrategy[Optional[float]] = None,
    allow_nan: MaybeSearchStrategy[bool] = False,
    allow_inf: MaybeSearchStrategy[bool] = False,
    allow_pos_inf: MaybeSearchStrategy[bool] = False,
    allow_neg_inf: MaybeSearchStrategy[bool] = False,
    integral: MaybeSearchStrategy[bool] = False,
    fill: Optional[SearchStrategy[Any]] = None,
    unique: MaybeSearchStrategy[bool] = False,
) -> NDArrayF:
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
    shape: MaybeSearchStrategy[Shape] = array_shapes(),
    min_value: MaybeSearchStrategy[Optional[int]] = None,
    max_value: MaybeSearchStrategy[Optional[int]] = None,
    fill: Optional[SearchStrategy[Any]] = None,
    unique: MaybeSearchStrategy[bool] = False,
) -> NDArrayI:
    """Strategy for generating arrays of ints."""
    draw = lift_draw(_draw)
    info = iinfo(int64)
    min_value_, max_value_ = draw(min_value), draw(max_value)
    min_value_use = info.min if min_value_ is None else min_value_
    max_value_use = info.max if max_value_ is None else max_value_
    elements = integers(min_value=min_value_use, max_value=max_value_use)
    return draw(
        arrays(int, draw(shape), elements=elements, fill=fill, unique=draw(unique))
    )


@beartype
def int32s() -> SearchStrategy[int]:
    """Strategy for generating int32s."""
    return from_dtype(dtype(int32)).map(int)


@beartype
def int64s() -> SearchStrategy[int]:
    """Strategy for generating int64s."""
    return from_dtype(dtype(int64)).map(int)


@composite
@beartype
def str_arrays(
    _draw: Any,
    /,
    *,
    shape: MaybeSearchStrategy[Shape] = array_shapes(),
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[Optional[int]] = None,
    allow_none: MaybeSearchStrategy[bool] = False,
    fill: Optional[SearchStrategy[Any]] = None,
    unique: MaybeSearchStrategy[bool] = False,
) -> NDArrayO:
    """Strategy for generating arrays of strings."""
    draw = lift_draw(_draw)
    elements = text_ascii(min_size=min_size, max_size=max_size)
    if draw(allow_none):
        elements |= none()
    return draw(
        arrays(object, draw(shape), elements=elements, fill=fill, unique=draw(unique))
    )


@beartype
def uint32s() -> SearchStrategy[int]:
    """Strategy for generating uint32s."""
    return from_dtype(dtype(uint32)).map(int)


@beartype
def uint64s() -> SearchStrategy[int]:
    """Strategy for generating uint64s."""
    return from_dtype(dtype(uint64)).map(int)
