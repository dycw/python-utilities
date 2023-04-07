import datetime as dt
from typing import Any, Optional, Union, cast

import numpy as np
from beartype import beartype
from hypothesis import assume
from hypothesis.errors import InvalidArgument
from hypothesis.extra.numpy import array_shapes, arrays
from hypothesis.strategies import (
    SearchStrategy,
    booleans,
    composite,
    integers,
    none,
    nothing,
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
    DATE_MAX_AS_INT,
    DATE_MIN_AS_INT,
    DATETIME_MAX_AS_INT,
    DATETIME_MIN_AS_INT,
    Datetime64Kind,
    Datetime64Unit,
    EmptyNumpyConcatenateError,
    date_to_datetime64,
    datetime64_to_int,
    datetime64_unit_to_dtype,
    datetime64_unit_to_kind,
    datetime_to_datetime64,
    redirect_to_empty_numpy_concatenate_error,
)
from utilities.numpy.typing import (
    NDArrayB,
    NDArrayD,
    NDArrayD1,
    NDArrayDD1,
    NDArrayDus1,
    NDArrayF,
    NDArrayI,
    NDArrayO,
)


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
def datetime64_dtypes(
    _draw: Any, /, *, kind: MaybeSearchStrategy[Optional[Datetime64Kind]] = None
) -> Any:
    """Strategy for generating datetime64 dtypes."""
    draw = lift_draw(_draw)
    unit = draw(datetime64_units(kind=kind))
    return datetime64_unit_to_dtype(unit)


@beartype
def datetime64_kinds() -> SearchStrategy[Datetime64Kind]:
    """Strategy for generating datetime64 kinds."""
    kinds: list[Datetime64Kind] = ["date", "time"]
    return sampled_from(kinds)


@composite
@beartype
def datetime64_units(
    _draw: Any, /, *, kind: MaybeSearchStrategy[Optional[Datetime64Kind]] = None
) -> Datetime64Unit:
    """Strategy for generating datetime64 units."""
    draw = lift_draw(_draw)
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
    kind_ = draw(kind)
    if kind_ is not None:
        units = [unit for unit in units if datetime64_unit_to_kind(unit) == kind_]
    return draw(sampled_from(units))


@composite
@beartype
def datetime64_arrays(
    _draw: Any,
    /,
    *,
    shape: MaybeSearchStrategy[Shape] = array_shapes(),
    unit: MaybeSearchStrategy[Optional[Datetime64Unit]] = None,
    min_value: MaybeSearchStrategy[Optional[Union[datetime64, int, dt.date]]] = None,
    max_value: MaybeSearchStrategy[Optional[Union[datetime64, int, dt.date]]] = None,
    valid_dates: MaybeSearchStrategy[bool] = False,
    valid_datetimes: MaybeSearchStrategy[bool] = False,
    fill: Optional[SearchStrategy[Any]] = None,
    unique: MaybeSearchStrategy[bool] = False,
) -> NDArrayD:
    """Strategy for generating arrays of datetime64s."""
    draw = lift_draw(_draw)
    if (unit_ := draw(unit)) is None:
        unit_ = draw(datetime64_units())
    dtype = datetime64_unit_to_dtype(cast(Datetime64Unit, unit_))
    elements = datetime64s(
        unit=cast(Datetime64Unit, unit_),
        min_value=min_value,
        max_value=max_value,
        valid_dates=valid_dates,
        valid_datetimes=valid_datetimes,
    )
    return draw(
        arrays(dtype, draw(shape), elements=elements, fill=fill, unique=draw(unique))
    )


@composite
@beartype
def datetime64_indexes(
    _draw: Any,
    /,
    *,
    n: MaybeSearchStrategy[IntNonNeg] = integers(0, 10),
    unit: MaybeSearchStrategy[Optional[Datetime64Unit]] = None,
    min_value: MaybeSearchStrategy[Optional[Union[datetime64, int, dt.date]]] = None,
    max_value: MaybeSearchStrategy[Optional[Union[datetime64, int, dt.date]]] = None,
    valid_dates: MaybeSearchStrategy[bool] = False,
    valid_datetimes: MaybeSearchStrategy[bool] = False,
    unique: MaybeSearchStrategy[bool] = True,
    sort: MaybeSearchStrategy[bool] = True,
) -> NDArrayD1:
    """Strategy for generating indexes of datetime64s."""
    draw = lift_draw(_draw)
    array = draw(
        datetime64_arrays(
            shape=n,
            unit=unit,
            min_value=min_value,
            max_value=max_value,
            valid_dates=valid_dates,
            valid_datetimes=valid_datetimes,
            fill=nothing(),
            unique=unique,
        )
    )
    return np.sort(array) if sort else array


@beartype
def datetime64D_indexes(  # noqa: N802
    *,
    n: MaybeSearchStrategy[IntNonNeg] = integers(0, 10),
    min_value: MaybeSearchStrategy[Optional[Union[datetime64, int, dt.date]]] = None,
    max_value: MaybeSearchStrategy[Optional[Union[datetime64, int, dt.date]]] = None,
    valid_dates: MaybeSearchStrategy[bool] = True,
    unique: MaybeSearchStrategy[bool] = True,
    sort: MaybeSearchStrategy[bool] = True,
) -> SearchStrategy[NDArrayDD1]:
    """Strategy for generating arrays of dates."""
    return datetime64_indexes(
        n=n,
        unit="D",
        min_value=min_value,
        max_value=max_value,
        valid_dates=valid_dates,
        unique=unique,
        sort=sort,
    )


@composite
@beartype
def datetime64s(
    _draw: Any,
    /,
    *,
    unit: MaybeSearchStrategy[Optional[Datetime64Unit]] = None,
    min_value: MaybeSearchStrategy[Optional[Union[datetime64, int, dt.date]]] = None,
    max_value: MaybeSearchStrategy[Optional[Union[datetime64, int, dt.date]]] = None,
    valid_dates: MaybeSearchStrategy[bool] = False,
    valid_datetimes: MaybeSearchStrategy[bool] = False,
) -> datetime64:
    """Strategy for generating datetime64s."""
    draw = lift_draw(_draw)
    unit_ = draw(unit)
    min_value_, max_value_ = map(
        _datetime64s_convert, map(draw, [min_value, max_value])
    )
    valid_dates_, valid_datetimes_ = map(draw, [valid_dates, valid_datetimes])
    if valid_dates_:
        unit_, min_value_, max_value_ = _datetime64s_check_valid_dates(
            unit=cast(Optional[Datetime64Unit], unit_),
            min_value=min_value_,
            max_value=max_value_,
        )
    if valid_datetimes_:
        unit_, min_value_, max_value_ = _datetime64s_check_valid_datetimes(
            unit=cast(Optional[Datetime64Unit], unit_),
            min_value=min_value_,
            max_value=max_value_,
        )
    i = draw(int64s(min_value=min_value_, max_value=max_value_))
    _ = assume(i != iinfo(int64).min)
    if unit_ is None:
        unit_ = draw(datetime64_units())
    return datetime64(i, unit_)


@beartype
def _datetime64s_convert(
    value: Optional[Union[int, datetime64, dt.date]], /
) -> Optional[int]:
    """Convert a min/max value supplied into `datetime64s`."""
    if (value is None) or isinstance(value, int):
        return value
    if isinstance(value, datetime64):
        return datetime64_to_int(value)
    if isinstance(value, dt.datetime):
        return _datetime64s_convert(datetime_to_datetime64(value))
    return _datetime64s_convert(date_to_datetime64(value))


@beartype
def _datetime64s_check_valid_dates(
    *,
    unit: Optional[Datetime64Unit] = None,
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
) -> tuple[Datetime64Unit, Optional[int], Optional[int]]:
    """Check/clip the bounds to generate valid `dt.date`s."""
    if (unit is not None) and (unit != "D"):
        msg = f"{unit=}"
        raise InvalidArgument(msg)
    if min_value is None:
        min_value = DATE_MIN_AS_INT
    else:
        min_value = max(min_value, DATE_MIN_AS_INT)
    if max_value is None:
        max_value = DATE_MAX_AS_INT
    else:
        max_value = min(max_value, DATE_MAX_AS_INT)
    return "D", min_value, max_value


@beartype
def _datetime64s_check_valid_datetimes(
    *,
    unit: Optional[Datetime64Unit] = None,
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
) -> tuple[Datetime64Unit, Optional[int], Optional[int]]:
    """Check/clip the bounds to generate valid `dt.datetime`s."""
    if (unit is not None) and (unit != "us"):
        msg = f"{unit=}"
        raise InvalidArgument(msg)
    if min_value is None:
        min_value = DATETIME_MIN_AS_INT
    else:
        min_value = max(min_value, DATETIME_MIN_AS_INT)
    if max_value is None:
        max_value = DATETIME_MAX_AS_INT
    else:
        max_value = min(max_value, DATETIME_MAX_AS_INT)
    return "us", min_value, max_value


@beartype
def datetime64us_indexes(
    *,
    n: MaybeSearchStrategy[IntNonNeg] = integers(0, 10),
    min_value: MaybeSearchStrategy[
        Optional[Union[datetime64, int, dt.datetime]]
    ] = None,
    max_value: MaybeSearchStrategy[
        Optional[Union[datetime64, int, dt.datetime]]
    ] = None,
    valid_datetimes: MaybeSearchStrategy[bool] = True,
    unique: MaybeSearchStrategy[bool] = True,
    sort: MaybeSearchStrategy[bool] = True,
) -> SearchStrategy[NDArrayDus1]:
    """Strategy for generating arrays of datetimes."""
    return datetime64_indexes(
        n=n,
        unit="us",
        min_value=min_value,
        max_value=max_value,
        valid_datetimes=valid_datetimes,
        unique=unique,
        sort=sort,
    )


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
    min_value_ = info.min if min_value_ is None else max(min_value_, info.min)
    max_value_use = info.max if max_value_ is None else min(info.max, max_value_)
    return draw(integers(min_value_, max_value_use))
