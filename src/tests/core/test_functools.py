from __future__ import annotations

from pytest import mark, param

from utilities.core import not_func


class TestNotFunc:
    @mark.parametrize("x", [param(True), param(False)])
    def test_main(self, *, x: bool) -> None:
        def return_x() -> bool:
            return x

        return_not_x = not_func(return_x)
        result = return_not_x()
        expected = not x
        assert result is expected
