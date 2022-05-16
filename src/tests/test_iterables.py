from typing import Any

from hypothesis import given
from hypothesis.strategies import sampled_from

from dycw_utilities.iterables import is_iterable_not_str


class TestIsIterableNotStr:
    @given(
        case=sampled_from([(None, False), ([], True), ((), True), ("", False)])
    )
    def test_main(self, case: tuple[Any, bool]) -> None:
        x, expected = case
        assert is_iterable_not_str(x) is expected
