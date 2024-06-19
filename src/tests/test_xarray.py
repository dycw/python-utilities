from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    data,
    dictionaries,
    integers,
    none,
    sampled_from,
)
from xarray import DataArray

from utilities.hypothesis import (
    assume_does_not_raise,
    float_data_arrays,
    int_indexes,
    text_ascii,
)
from utilities.xarray import ewma, exp_moving_sum, rename_data_arrays

if TYPE_CHECKING:
    from collections.abc import Mapping

    from utilities.pandas import IndexA


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
