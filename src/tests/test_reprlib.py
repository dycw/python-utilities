from __future__ import annotations

from typing import Any

from hypothesis import given
from hypothesis.strategies import sampled_from

from utilities.reprlib import get_repr, get_repr_and_class


class TestGetRepr:
    @given(
        case=sampled_from([
            (None, "None"),
            (0, "0"),
            (
                list(range(21)),
                "[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, ... +1]",
            ),
        ])
    )
    def test_main(self, *, case: tuple[Any, str]) -> None:
        obj, expected = case
        result = get_repr(obj)
        assert result == expected


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
