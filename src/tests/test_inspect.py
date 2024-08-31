from __future__ import annotations

from typing import Any

from pytest import mark, param

from utilities.functions import identity
from utilities.inspect import bind_args_custom_repr


class TestBindArgsCustomRepr:
    @mark.parametrize(
        ("obj", "expected"),
        [
            param(None, "obj=None"),
            param([], "obj=[]"),
            param([1], "obj=[1]"),
            param([1, 2], "obj=[1, 2]"),
            param([1, 2, 3], "obj=[1, 2, 3]"),
            param([1, 2, 3, 4], "obj=[1, 2, 3, 4]"),
            param([1, 2, 3, 4, 5], "obj=[1, 2, 3, 4, 5]"),
            param([1, 2, 3, 4, 5, 6], "obj=[1, 2, 3, 4, 5, 6]"),
            param([1, 2, 3, 4, 5, 6, 7], "obj=[1, 2, 3, 4, 5, 6, ...]"),
            param([1, 2, 3, 4, 5, 6, 7, 8], "obj=[1, 2, 3, 4, 5, 6, ...]"),
        ],
    )
    def test_main(self, *, obj: Any, expected: str) -> None:
        result = bind_args_custom_repr(identity, obj)
        assert result == expected
