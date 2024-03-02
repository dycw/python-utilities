from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    data,
    dictionaries,
    integers,
    none,
    sampled_from,
)
from typing_extensions import Self
from xarray import DataArray

from utilities.hypothesis import (
    assume_does_not_raise,
    float_data_arrays,
    int_indexes,
    text_ascii,
)
from utilities.pandas import IndexA
from utilities.xarray import ewma, exp_moving_sum, rename_data_arrays


class TestBottleNeckInstalled:
    def test_main(self: Self) -> None:
        array = DataArray([], {"dim": []}, ["dim"])
        _ = array.ffill(dim="dim")


class TestEwma:
    @given(
        data=data(),
        indexes=dictionaries(text_ascii(), int_indexes(), min_size=1, max_size=3),
        halflife=integers(1, 10),
    )
    def test_main(
        self: Self, data: DataObject, indexes: Mapping[str, IndexA], halflife: int
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
        self: Self, data: DataObject, indexes: Mapping[str, IndexA], halflife: int
    ) -> None:
        array = data.draw(float_data_arrays(indexes))
        dim = data.draw(sampled_from(list(indexes)))
        with assume_does_not_raise(RuntimeWarning):
            _ = exp_moving_sum(array, {dim: halflife})


class TestNumbaggInstalled:
    def test_main(self: Self) -> None:
        array = DataArray([], {"dim": []}, ["dim"])
        _ = array.rolling_exp(dim=1.0).sum()


class TestRenameDataArrays:
    @given(name_array=text_ascii() | none(), name_other=text_ascii() | none())
    def test_main(
        self: Self, *, name_array: str | None, name_other: str | None
    ) -> None:
        @dataclass
        class Other:
            name: str | None

        @dataclass
        class Example:
            array: DataArray
            other: Other

            def __post_init__(self: Self) -> None:
                rename_data_arrays(self)

        array = DataArray(name=name_array)
        other = Other(name=name_other)
        example = Example(array, other)
        assert example.array is not array
        assert example.other is other
        assert example.array.name == "array"
        assert example.other.name == name_other
