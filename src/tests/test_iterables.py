from itertools import chain
from typing import Any

from hypothesis import given
from hypothesis.strategies import DataObject, data, integers, lists, sampled_from, sets
from pytest import mark, param, raises

from utilities.iterables import (
    IterableContainsDuplicatesError,
    check_duplicates,
    is_iterable_not_str,
)


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
        [param(None, False), param([], True), param((), True), param("", False)],
    )
    def test_main(self, x: Any, expected: bool) -> None:
        assert is_iterable_not_str(x) is expected
