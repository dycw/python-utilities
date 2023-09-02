from collections.abc import Hashable
from typing import cast

from hypothesis import given
from hypothesis.strategies import DataObject, data, dictionaries, integers, sampled_from
from pandas import Index
from pytest import mark
from utilities.hypothesis import assume_does_not_raise, text_ascii
from utilities.hypothesis.pandas import int_indexes
from utilities.hypothesis.xarray import float_data_arrays
from utilities.xarray import ewma, exp_moving_sum
from xarray import DataArray


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
        self, data: DataObject, indexes: dict[str, Index], halflife: int
    ) -> None:
        array = data.draw(float_data_arrays(cast(dict[Hashable, Index], indexes)))
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
        self, data: DataObject, indexes: dict[str, Index], halflife: int
    ) -> None:
        array = data.draw(float_data_arrays(cast(dict[Hashable, Index], indexes)))
        dim = data.draw(sampled_from(list(indexes)))
        with assume_does_not_raise(RuntimeWarning):
            _ = exp_moving_sum(array, {dim: halflife})


class TestNumbaggInstalled:
    @mark.xfail(
        reason="RuntimeError: Cannot install on Python version 3.11.4; "
        "only versions >=3.7,<3.11 are supported."
    )
    def test_main(self) -> None:
        array = DataArray([], {"dim": []}, ["dim"])
        _ = array.rolling_exp(dim=1.0).sum()
