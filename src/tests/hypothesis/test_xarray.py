from collections.abc import Hashable
from typing import Optional

from hypothesis import given
from hypothesis.errors import InvalidArgument
from hypothesis.strategies import DataObject, booleans, data, floats, integers, none
from pandas import Index
from pandas.testing import assert_index_equal

from utilities.hypothesis import assume_does_not_raise, hashables
from utilities.hypothesis.numpy import int64s
from utilities.hypothesis.xarray import (
    bool_data_arrays,
    dicts_of_indexes,
    float_data_arrays,
    int_data_arrays,
    str_data_arrays,
)


class TestBoolDataArrays:
    @given(data=data(), indexes=dicts_of_indexes(), name=hashables())
    def test_main(
        self, data: DataObject, indexes: dict[Hashable, Index], name: Hashable
    ) -> None:
        array = data.draw(bool_data_arrays(indexes=indexes, name=name))
        assert set(array.coords) == set(indexes)
        assert array.dims == tuple(indexes)
        assert array.dtype == bool
        assert array.name == name
        for arr, exp in zip(array.indexes.values(), indexes.values()):
            assert_index_equal(arr, exp, check_names=False)


class TestDictsOfIndexes:
    @given(
        data=data(),
        min_dims=integers(1, 3),
        max_dims=integers(1, 3) | none(),
        min_side=integers(1, 10),
        max_side=integers(1, 10) | none(),
    )
    def test_main(
        self,
        data: DataObject,
        min_dims: int,
        max_dims: Optional[int],
        min_side: int,
        max_side: Optional[int],
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            indexes = data.draw(
                dicts_of_indexes(
                    min_dims=min_dims,
                    max_dims=max_dims,
                    min_side=min_side,
                    max_side=max_side,
                )
            )
        ndims = len(indexes)
        assert ndims >= min_dims
        if max_dims is not None:
            assert ndims <= max_dims
        for index in indexes.values():
            length = len(index)
            assert length >= min_side
            if max_side is not None:
                assert length <= max_side


class TestFloatDataArrays:
    @given(
        data=data(),
        indexes=dicts_of_indexes(),
        min_value=floats() | none(),
        max_value=floats() | none(),
        allow_nan=booleans(),
        allow_inf=booleans(),
        allow_pos_inf=booleans(),
        allow_neg_inf=booleans(),
        integral=booleans(),
        unique=booleans(),
        name=hashables(),
    )
    def test_main(
        self,
        data: DataObject,
        indexes: dict[Hashable, Index],
        min_value: Optional[float],
        max_value: Optional[float],
        allow_nan: bool,
        allow_inf: bool,
        allow_pos_inf: bool,
        allow_neg_inf: bool,
        integral: bool,
        unique: bool,
        name: Hashable,
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            array = data.draw(
                float_data_arrays(
                    indexes=indexes,
                    min_value=min_value,
                    max_value=max_value,
                    allow_nan=allow_nan,
                    allow_inf=allow_inf,
                    allow_pos_inf=allow_pos_inf,
                    allow_neg_inf=allow_neg_inf,
                    integral=integral,
                    unique=unique,
                    name=name,
                )
            )
        assert set(array.coords) == set(indexes)
        assert array.dims == tuple(indexes)
        assert array.dtype == float
        assert array.name == name
        for arr, exp in zip(array.indexes.values(), indexes.values()):
            assert_index_equal(arr, exp, check_names=False)


class TestIntDataArrays:
    @given(
        data=data(),
        indexes=dicts_of_indexes(),
        min_value=int64s() | none(),
        max_value=int64s() | none(),
        unique=booleans(),
        name=hashables(),
    )
    def test_main(
        self,
        data: DataObject,
        indexes: dict[Hashable, Index],
        min_value: Optional[int],
        max_value: Optional[int],
        unique: bool,
        name: Hashable,
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            array = data.draw(
                int_data_arrays(
                    indexes=indexes,
                    min_value=min_value,
                    max_value=max_value,
                    unique=unique,
                    name=name,
                )
            )
        assert set(array.coords) == set(indexes)
        assert array.dims == tuple(indexes)
        assert array.dtype == int
        assert array.name == name
        for arr, exp in zip(array.indexes.values(), indexes.values()):
            assert_index_equal(arr, exp, check_names=False)


class TestStrDataArrays:
    @given(
        data=data(),
        indexes=dicts_of_indexes(),
        min_size=integers(0, 100),
        max_size=integers(0, 100) | none(),
        allow_none=booleans(),
        unique=booleans(),
        name=hashables(),
    )
    def test_main(
        self,
        data: DataObject,
        indexes: dict[Hashable, Index],
        min_size: int,
        max_size: Optional[int],
        allow_none: bool,
        unique: bool,
        name: Hashable,
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            array = data.draw(
                str_data_arrays(
                    indexes=indexes,
                    min_size=min_size,
                    max_size=max_size,
                    allow_none=allow_none,
                    unique=unique,
                    name=name,
                )
            )
        assert set(array.coords) == set(indexes)
        assert array.dims == tuple(indexes)
        assert array.dtype == object
        assert array.name == name
        for arr, exp in zip(array.indexes.values(), indexes.values()):
            assert_index_equal(arr, exp, check_names=False)
