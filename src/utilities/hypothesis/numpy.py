from typing import Any, Optional, Union

from beartype import beartype
from hypothesis import assume
from hypothesis.extra.numpy import array_shapes, arrays
from hypothesis.strategies import (
    SearchStrategy,
    booleans,
    composite,
    integers,
    none,
    sampled_from,
)
from numpy import (
    concatenate,
    datetime64,
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
    Datetime64Unit,
    EmptyNumpyConcatenateError,
    datetime64_to_int,
    datetime64_unit_to_dtype,
    datetime64D,
    redirect_to_empty_numpy_concatenate_error,
)
from utilities.numpy.typing import NDArrayB, NDArrayDD, NDArrayF, NDArrayI, NDArrayO


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
def datetime64D_arrays(  # noqa: N802
    _draw: Any,
    /,
    *,
    shape: MaybeSearchStrategy[Shape] = array_shapes(),
    min_value: MaybeSearchStrategy[Optional[Union[int, datetime64]]] = None,
    max_value: MaybeSearchStrategy[Optional[Union[int, datetime64]]] = None,
    fill: Optional[SearchStrategy[Any]] = None,
    unique: MaybeSearchStrategy[bool] = False,
) -> NDArrayDD:
    """Strategy for generating arrays of dates."""
    draw = lift_draw(_draw)
    return draw(
        arrays(
            datetime64D,
            draw(shape),
            elements=datetime64s(min_value=min_value, max_value=max_value, unit="D"),
            fill=fill,
            unique=draw(unique),
        )
    )


@composite
@beartype
def datetime64s(
    _draw: Any,
    /,
    *,
    min_value: MaybeSearchStrategy[Optional[Union[datetime64, int]]] = None,
    max_value: MaybeSearchStrategy[Optional[Union[datetime64, int]]] = None,
    unit: MaybeSearchStrategy[Optional[Datetime64Unit]] = None,
) -> datetime64:
    """Strategy for generating datetime64s."""
    draw = lift_draw(_draw)
    min_value_, max_value_ = map(draw, [min_value, max_value])

    @beartype
    def convert(value: Optional[Union[int, datetime64]], /) -> Optional[int]:
        if (value is None) or isinstance(value, int):
            return value
        return datetime64_to_int(value)

    i = draw(int64s(min_value=convert(min_value_), max_value=convert(max_value_)))
    _ = assume(i != iinfo(int64).min)
    if (unit_ := draw(unit)) is None:
        unit_ = draw(datetime64_units())
    return datetime64(i, unit_)


@composite
@beartype
def datetime64_dtypes(_draw: Any, /) -> Any:
    """Strategy for generating datetime64 dtypes."""
    draw = lift_draw(_draw)
    unit = draw(datetime64_units())
    return datetime64_unit_to_dtype(unit)


@beartype
def datetime64_units() -> SearchStrategy[Datetime64Unit]:
    """Strategy for generating datetime64 units."""
    units: list[Datetime64Unit] = [
        "Y",
        "M",
        "W",
        "D",
        "h",
        "m",
        "s",
        "ms",
        "us",
        "ns",
        "ps",
        "fs",
        "as",
    ]
    return sampled_from(units)


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
def int32s(
    *,
    min_value: MaybeSearchStrategy[Optional[int]] = None,
    max_value: MaybeSearchStrategy[Optional[int]] = None,
) -> SearchStrategy[int]:
    """Strategy for generating int32s."""
    return _fixed_width_ints(int32, min_value=min_value, max_value=max_value)


@beartype
def int64s(
    *,
    min_value: MaybeSearchStrategy[Optional[int]] = None,
    max_value: MaybeSearchStrategy[Optional[int]] = None,
) -> SearchStrategy[int]:
    """Strategy for generating int64s."""
    return _fixed_width_ints(int64, min_value=min_value, max_value=max_value)


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
def uint32s(
    *,
    min_value: MaybeSearchStrategy[Optional[int]] = None,
    max_value: MaybeSearchStrategy[Optional[int]] = None,
) -> SearchStrategy[int]:
    """Strategy for generating uint32s."""
    return _fixed_width_ints(uint32, min_value=min_value, max_value=max_value)


@beartype
def uint64s(
    *,
    min_value: MaybeSearchStrategy[Optional[int]] = None,
    max_value: MaybeSearchStrategy[Optional[int]] = None,
) -> SearchStrategy[int]:
    """Strategy for generating uint64s."""
    return _fixed_width_ints(uint64, min_value=min_value, max_value=max_value)


@composite
@beartype
def _fixed_width_ints(
    _draw: Any,
    dtype: Any,
    /,
    *,
    min_value: MaybeSearchStrategy[Optional[int]] = None,
    max_value: MaybeSearchStrategy[Optional[int]] = None,
) -> int:
    """Strategy for generating int64s."""
    draw = lift_draw(_draw)
    min_value_, max_value_ = map(draw, [min_value, max_value])
    info = iinfo(dtype)
    min_value_use = info.min if min_value_ is None else max(info.min, min_value_)
    max_value_use = info.max if max_value_ is None else min(info.max, max_value_)
    return draw(integers(min_value_use, max_value_use))
