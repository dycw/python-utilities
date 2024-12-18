from __future__ import annotations

from typing import Any

from hypothesis import given
from hypothesis.strategies import sampled_from

from utilities.reprlib import get_repr_and_class


class TestGetReprAndClass:
    @given(
        case=sampled_from([
            (None, "Object 'None' of type 'NoneType'"),
            (0, "Object '0' of type 'int'"),
        ])
    )
    def test_main(self, *, case: tuple[Any, str]) -> None:
        obj, expected = case
        result = get_repr_and_class(obj)
        assert result == expected
