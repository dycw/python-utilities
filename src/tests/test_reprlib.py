from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pytest import mark, param

from utilities.reprlib import _custom_mapping_repr, _custom_repr

if TYPE_CHECKING:
    from collections.abc import Mapping


class TestCustomRepr:
    @mark.parametrize(
        ("mapping", "expected"),
        [
            param({}, ""),
            param({"a": 1}, "a=1"),
            param({"a": 1, "b": 2}, "a=1, b=2"),
            param({"a": 1, "b": 2, "c": 3}, "a=1, b=2, c=3"),
            param({"a": 1, "b": 2, "c": 3, "d": 4}, "a=1, b=2, c=3, d=4"),
            param({"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}, "a=1, b=2, c=3, d=4, ..."),
        ],
    )
    def test_main(self, *, mapping: Mapping[str, Any], expected: str) -> None:
        result = _custom_repr(mapping)
        assert result == expected


class TestCustomMappingRepr:
    @mark.parametrize(
        ("mapping", "expected"),
        [
            param({}, ""),
            param({"a": 1}, "a=1"),
            param({"a": 1, "b": 2}, "a=1, b=2"),
            param({"a": 1, "b": 2, "c": 3}, "a=1, b=2, c=3"),
            param({"a": 1, "b": 2, "c": 3, "d": 4}, "a=1, b=2, c=3, d=4"),
            param({"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}, "a=1, b=2, c=3, d=4, e=5"),
            param({"a": [1, 2, 3, 4, 5]}, "a=[1, 2, 3, 4, 5]"),
            param({"a": [1, 2, 3, 4, 5, 6]}, "a=[1, 2, 3, 4, 5, 6]"),
            param({"a": [1, 2, 3, 4, 5, 6, 7]}, "a=[1, 2, 3, 4, 5, 6, ...]"),
        ],
    )
    def test_main(self, *, mapping: Mapping[str, Any], expected: str) -> None:
        result = _custom_mapping_repr(mapping)
        assert result == expected
