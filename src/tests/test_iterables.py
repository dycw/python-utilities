from itertools import chain
from typing import Any

from hypothesis import given
from hypothesis.strategies import DataObject
from hypothesis.strategies import data
from hypothesis.strategies import integers
from hypothesis.strategies import lists
from hypothesis.strategies import sampled_from
from hypothesis.strategies import sets
from pytest import mark
from pytest import param
from pytest import raises

from utilities.iterables import IterableContainsDuplicatesError
from utilities.iterables import check_duplicates
from utilities.iterables import is_iterable_not_str


class TestCheckDuplicates:
    @given(x=sets(integers()))
    def test_main(self, x: set[int]) -> None:
        check_duplicates(x)

    @given(data=data(), x=lists(integers(), min_size=1))
    def test_error(self, data: DataObject, x: list[int]) -> None:
        x_i = data.draw(sampled_from(x))
        y = chain(x, [x_i])
        with raises(IterableContainsDuplicatesError):
            check_duplicates(y)


class TestIsIterableNotStr:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(None, False),
            param([], True),
            param((), True),
            param("", False),
        ],
    )
    def test_main(self, x: Any, expected: bool) -> None:
        assert is_iterable_not_str(x) is expected
