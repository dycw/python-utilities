from collections.abc import Hashable
from typing import Any, Optional

from beartype import beartype
from hypothesis.extra.numpy import array_shapes
from hypothesis.strategies import SearchStrategy, composite
from pandas import Index
from xarray import DataArray

from utilities.hypothesis import lift_draw, lists_fixed_length, text_ascii
from utilities.hypothesis.numpy import bool_arrays, float_arrays, int_arrays, str_arrays
from utilities.hypothesis.pandas import int_indexes
from utilities.hypothesis.typing import MaybeSearchStrategy
from utilities.pandas.typing import IndexI
from utilities.xarray.typing import DataArrayB, DataArrayF, DataArrayI, DataArrayO


@composite
@beartype
def dicts_of_indexes(
    _draw: Any,
    /,
    *,
    min_dims: int = 1,
    max_dims: Optional[int] = None,
    min_side: int = 1,
    max_side: Optional[int] = None,
) -> dict[Hashable, IndexI]:
    """Strategy for generating dictionaries of indexes."""
    draw = lift_draw(_draw)
    shape = draw(
        array_shapes(
            min_dims=min_dims, max_dims=max_dims, min_side=min_side, max_side=max_side
        )
    )
    ndims = len(shape)
    dims = draw(lists_fixed_length(text_ascii(), ndims, unique=True))
    indexes = (draw(int_indexes(n=length)) for length in shape)
    return dict(zip(dims, indexes))


@composite
@beartype
def bool_data_arrays(
    _draw: Any,
    /,
    *,
    indexes: MaybeSearchStrategy[dict[Hashable, Index]] = dicts_of_indexes(),
    fill: Optional[SearchStrategy[Any]] = None,
    unique: MaybeSearchStrategy[bool] = False,
    name: MaybeSearchStrategy[Hashable] = None,
) -> DataArrayB:
    """Strategy for generating data arrays of booleans."""
    draw = lift_draw(_draw)
    indexes_ = draw(indexes)
    shape = tuple(map(len, indexes_.values()))
    values = draw(bool_arrays(shape=shape, fill=fill, unique=unique))
    return DataArray(data=values, coords=indexes_, dims=list(indexes_), name=draw(name))


@composite
@beartype
def float_data_arrays(
    _draw: Any,
    /,
    *,
    indexes: MaybeSearchStrategy[dict[Hashable, Index]] = dicts_of_indexes(),
    min_value: MaybeSearchStrategy[Optional[float]] = None,
    max_value: MaybeSearchStrategy[Optional[float]] = None,
    allow_nan: MaybeSearchStrategy[bool] = False,
    allow_inf: MaybeSearchStrategy[bool] = False,
    allow_pos_inf: MaybeSearchStrategy[bool] = False,
    allow_neg_inf: MaybeSearchStrategy[bool] = False,
    integral: MaybeSearchStrategy[bool] = False,
    fill: Optional[SearchStrategy[Any]] = None,
    unique: MaybeSearchStrategy[bool] = False,
    name: MaybeSearchStrategy[Hashable] = None,
) -> DataArrayF:
    """Strategy for generating data arrays of floats."""
    draw = lift_draw(_draw)
    indexes_ = draw(indexes)
    shape = tuple(map(len, indexes_.values()))
    values = draw(
        float_arrays(
            shape=shape,
            min_value=min_value,
            max_value=max_value,
            allow_nan=allow_nan,
            allow_inf=allow_inf,
            allow_pos_inf=allow_pos_inf,
            allow_neg_inf=allow_neg_inf,
            integral=integral,
            fill=fill,
            unique=unique,
        )
    )
    return DataArray(data=values, coords=indexes_, dims=list(indexes_), name=draw(name))


@composite
@beartype
def int_data_arrays(
    _draw: Any,
    /,
    *,
    indexes: MaybeSearchStrategy[dict[Hashable, Index]] = dicts_of_indexes(),
    min_value: MaybeSearchStrategy[Optional[int]] = None,
    max_value: MaybeSearchStrategy[Optional[int]] = None,
    fill: Optional[SearchStrategy[Any]] = None,
    unique: MaybeSearchStrategy[bool] = False,
    name: MaybeSearchStrategy[Hashable] = None,
) -> DataArrayI:
    """Strategy for generating data arrays of ints."""
    draw = lift_draw(_draw)
    indexes_ = draw(indexes)
    shape = tuple(map(len, indexes_.values()))
    values = draw(
        int_arrays(
            shape=shape,
            min_value=min_value,
            max_value=max_value,
            fill=fill,
            unique=unique,
        )
    )
    return DataArray(data=values, coords=indexes_, dims=list(indexes_), name=draw(name))


@composite
@beartype
def str_data_arrays(
    _draw: Any,
    /,
    *,
    indexes: MaybeSearchStrategy[dict[Hashable, Index]] = dicts_of_indexes(),
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[Optional[int]] = None,
    allow_none: MaybeSearchStrategy[bool] = False,
    fill: Optional[SearchStrategy[Any]] = None,
    unique: MaybeSearchStrategy[bool] = False,
    name: MaybeSearchStrategy[Hashable] = None,
) -> DataArrayO:
    """Strategy for generating data arrays of strings."""
    draw = lift_draw(_draw)
    indexes_ = draw(indexes)
    shape = tuple(map(len, indexes_.values()))
    values = draw(
        str_arrays(
            shape=shape,
            min_size=min_size,
            max_size=max_size,
            allow_none=allow_none,
            fill=fill,
            unique=unique,
        )
    )
    return DataArray(data=values, coords=indexes_, dims=list(indexes_), name=draw(name))
