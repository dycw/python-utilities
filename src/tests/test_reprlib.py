from __future__ import annotations

from typing import Any

from hypothesis import given
from hypothesis.strategies import sampled_from

from utilities.reprlib import get_call_args_mapping, get_repr, get_repr_and_class


class TestYieldCallArgsRepr:
    def test_main(self) -> None:
        mapping = get_call_args_mapping(1, 2, 3, x=4, y=5, z=6)
        expected = {
            "args[0]": 1,
            "args[1]": 2,
            "args[2]": 3,
            "kwargs[x]": 4,
            "kwargs[y]": 5,
            "kwargs[z]": 6,
        }
        assert mapping == expected


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
