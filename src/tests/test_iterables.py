from typing import Any

from pytest import mark
from pytest import param

from utilities.iterables import is_iterable_not_str


class TestIsIterableNotStr:
    @mark.parametrize(
        "x, expected",
        [
            param(None, False),
            param([], True),
            param((), True),
            param("", False),
        ],
    )
    def test_main(self, x: Any, expected: bool) -> None:
        assert is_iterable_not_str(x) is expected
