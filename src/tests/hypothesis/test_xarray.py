from __future__ import annotations

from collections.abc import Mapping

from hypothesis import given
from hypothesis.errors import InvalidArgument
from hypothesis.strategies import DataObject, booleans, data, floats, integers, none
from numpy import int64
from pandas.testing import assert_index_equal

from utilities._hypothesis.xarray import _merge_into_dict_of_indexes
from utilities.hypothesis import (
    assume_does_not_raise,
    bool_data_arrays,
    dicts_of_indexes,
    float_data_arrays,
    int64s,
    int_data_arrays,
    str_data_arrays,
    text_ascii,
)
from utilities.pandas import IndexA


class TestBoolDataArrays:
    @given(data=data(), indexes=dicts_of_indexes(), name=text_ascii() | none())
    def test_main(
        self, *, data: DataObject, indexes: Mapping[str, IndexA], name: str | None
    ) -> None:
        array = data.draw(bool_data_arrays(indexes, name=name))
        assert set(array.coords) == set(indexes)
        assert array.dims == tuple(indexes)
        assert array.dtype == bool
        assert array.name == name
        for arr, exp in zip(array.indexes.values(), indexes.values(), strict=True):
            assert_index_equal(arr, exp, check_names=False)  # type: ignore


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
        *,
        data: DataObject,
        min_dims: int,
        max_dims: int | None,
        min_side: int,
        max_side: int | None,
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
        name=text_ascii() | none(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        indexes: Mapping[str, IndexA],
        min_value: float | None,
        max_value: float | None,
        allow_nan: bool,
        allow_inf: bool,
        allow_pos_inf: bool,
        allow_neg_inf: bool,
        integral: bool,
        unique: bool,
        name: str | None,
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            array = data.draw(
                float_data_arrays(
                    indexes,
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
        for arr, exp in zip(array.indexes.values(), indexes.values(), strict=True):
            assert_index_equal(arr, exp, check_names=False)  # type: ignore


class TestIntDataArrays:
    @given(
        data=data(),
        indexes=dicts_of_indexes(),
        min_value=int64s() | none(),
        max_value=int64s() | none(),
        unique=booleans(),
        name=text_ascii() | none(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        indexes: Mapping[str, IndexA],
        min_value: int | None,
        max_value: int | None,
        unique: bool,
        name: str | None,
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            array = data.draw(
                int_data_arrays(
                    indexes,
                    min_value=min_value,
                    max_value=max_value,
                    unique=unique,
                    name=name,
                )
            )
        assert set(array.coords) == set(indexes)
        assert array.dims == tuple(indexes)
        assert array.dtype == int64
        assert array.name == name
        for arr, exp in zip(array.indexes.values(), indexes.values(), strict=True):
            assert_index_equal(arr, exp, check_names=False)  # type: ignore


class TestMergeIntoDictOfIndexes:
    @given(data=data())
    def test_empty(self, *, data: DataObject) -> None:
        _ = data.draw(_merge_into_dict_of_indexes())

    @given(
        data=data(), indexes1=dicts_of_indexes() | none(), indexes2=dicts_of_indexes()
    )
    def test_non_empty(
        self,
        *,
        data: DataObject,
        indexes1: Mapping[str, IndexA] | None,
        indexes2: Mapping[str, IndexA],
    ) -> None:
        indexes_ = data.draw(_merge_into_dict_of_indexes(indexes1, **indexes2))
        expected = (set() if indexes1 is None else set(indexes1)) | set(indexes2)
        assert set(indexes_) == expected


class TestStrDataArrays:
    @given(
        data=data(),
        indexes=dicts_of_indexes(),
        min_size=integers(0, 100),
        max_size=integers(0, 100) | none(),
        allow_none=booleans(),
        unique=booleans(),
        name=text_ascii() | none(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        indexes: Mapping[str, IndexA],
        min_size: int,
        max_size: int | None,
        allow_none: bool,
        unique: bool,
        name: str | None,
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            array = data.draw(
                str_data_arrays(
                    indexes,
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
        for arr, exp in zip(array.indexes.values(), indexes.values(), strict=True):
            assert_index_equal(arr, exp, check_names=False)  # type: ignore
