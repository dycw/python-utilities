from __future__ import annotations

from collections.abc import Mapping

from hypothesis import given
from hypothesis.strategies import DataObject, data, dictionaries, integers, sampled_from
from xarray import DataArray

from utilities.hypothesis import (
    assume_does_not_raise,
    float_data_arrays,
    int_indexes,
    text_ascii,
)
from utilities.pandas import IndexA
from utilities.xarray import ewma, exp_moving_sum


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
