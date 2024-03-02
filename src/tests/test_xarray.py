from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from beartype.door import die_if_unbearable
from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    data,
    dictionaries,
    integers,
    none,
    sampled_from,
)
from numpy import empty, int64, zeros
from pytest import mark, param
from xarray import DataArray

from utilities.hypothesis import (
    assume_does_not_raise,
    float_data_arrays,
    int_indexes,
    text_ascii,
)
from utilities.numpy import datetime64ns
from utilities.pandas import IndexA
from utilities.xarray import (
    DataArray0,
    DataArray1,
    DataArray2,
    DataArray3,
    DataArrayB,
    DataArrayB0,
    DataArrayB1,
    DataArrayB2,
    DataArrayB3,
    DataArrayDns,
    DataArrayDns0,
    DataArrayDns1,
    DataArrayDns2,
    DataArrayDns3,
    DataArrayF,
    DataArrayF0,
    DataArrayF1,
    DataArrayF2,
    DataArrayF3,
    DataArrayI,
    DataArrayI0,
    DataArrayI1,
    DataArrayI2,
    DataArrayI3,
    DataArrayO,
    DataArrayO0,
    DataArrayO1,
    DataArrayO2,
    DataArrayO3,
    ewma,
    exp_moving_sum,
    rename_data_arrays,
)


class TestAnnotations:
    @mark.parametrize(
        ("dtype", "hint"),
        [
            param(bool, DataArrayB),
            param(datetime64ns, DataArrayDns),
            param(float, DataArrayF),
            param(int64, DataArrayI),
            param(object, DataArrayO),
        ],
    )
    def test_dtype(self, *, dtype: Any, hint: Any) -> None:
        arr = DataArray(empty(0, dtype=dtype))
        die_if_unbearable(arr, hint)

    @mark.parametrize(
        ("ndim", "hint"),
        [
            param(0, DataArray0),
            param(1, DataArray1),
            param(2, DataArray2),
            param(3, DataArray3),
        ],
    )
    def test_ndim(self, *, ndim: int, hint: Any) -> None:
        arr = DataArray(empty(zeros(ndim, dtype=int), dtype=float))
        die_if_unbearable(arr, hint)

    @mark.parametrize(
        ("dtype", "ndim", "hint"),
        [
            # ndim 0
            param(bool, 0, DataArrayB0),
            param(datetime64ns, 0, DataArrayDns0),
            param(float, 0, DataArrayF0),
            param(int64, 0, DataArrayI0),
            param(object, 0, DataArrayO0),
            # ndim 1
            param(bool, 1, DataArrayB1),
            param(datetime64ns, 1, DataArrayDns1),
            param(float, 1, DataArrayF1),
            param(int64, 1, DataArrayI1),
            param(object, 1, DataArrayO1),
            # ndim 2
            param(bool, 2, DataArrayB2),
            param(datetime64ns, 2, DataArrayDns2),
            param(float, 2, DataArrayF2),
            param(int64, 2, DataArrayI2),
            param(object, 2, DataArrayO2),
            # ndim 3
            param(bool, 3, DataArrayB3),
            param(datetime64ns, 3, DataArrayDns3),
            param(float, 3, DataArrayF3),
            param(int64, 3, DataArrayI3),
            param(object, 3, DataArrayO3),
        ],
    )
    def test_compound(self, *, dtype: Any, ndim: int, hint: Any) -> None:
        arr = DataArray(empty(zeros(ndim, dtype=int64), dtype=dtype))
        die_if_unbearable(arr, hint)


class TestBottleNeckInstalled:
    def test_main(self) -> None:
        array = DataArray([], {"dim": []}, ["dim"])
        _ = array.ffill(dim="dim")


class TestEwma:
    @given(
        data=data(),
        indexes=dictionaries(text_ascii(), int_indexes(), min_size=1, max_size=3),
        halflife=integers(1, 10),
    )
    def test_main(
        self, data: DataObject, indexes: Mapping[str, IndexA], halflife: int
    ) -> None:
        array = data.draw(float_data_arrays(indexes))
        dim = data.draw(sampled_from(list(indexes)))
        with assume_does_not_raise(RuntimeWarning):
            _ = ewma(array, {dim: halflife})


class TestExpMovingSum:
    @given(
        data=data(),
        indexes=dictionaries(text_ascii(), int_indexes(), min_size=1, max_size=3),
        halflife=integers(1, 10),
    )
    def test_main(
        self, data: DataObject, indexes: Mapping[str, IndexA], halflife: int
    ) -> None:
        array = data.draw(float_data_arrays(indexes))
        dim = data.draw(sampled_from(list(indexes)))
        with assume_does_not_raise(RuntimeWarning):
            _ = exp_moving_sum(array, {dim: halflife})


class TestNumbaggInstalled:
    def test_main(self) -> None:
        array = DataArray([], {"dim": []}, ["dim"])
        _ = array.rolling_exp(dim=1.0).sum()


class TestRenameDataArrays:
    @given(name_array=text_ascii() | none(), name_other=text_ascii() | none())
    def test_main(self, *, name_array: str | None, name_other: str | None) -> None:
        @dataclass
        class Other:
            name: str | None

        @dataclass
        class Example:
            array: DataArray
            other: Other

            def __post_init__(self) -> None:
                rename_data_arrays(self)

        array = DataArray(name=name_array)
        other = Other(name=name_other)
        example = Example(array, other)
        assert example.array is not array
        assert example.other is other
        assert example.array.name == "array"
        assert example.other.name == name_other
