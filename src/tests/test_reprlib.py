from __future__ import annotations

from itertools import chain
from typing import TYPE_CHECKING, Any

from pytest import mark, param

from utilities.reprlib import (
    ReprLocals,
    _custom_mapping_repr,
    _custom_repr,
    _filter_mapping,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


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
            param(
                {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6},
                "a=1, b=2, c=3, d=4, ...",
            ),
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
            param(
                {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6},
                "a=1, b=2, c=3, d=4, e=5, f=6",
            ),
            param({"a": [1, 2, 3, 4, 5]}, "a=[1, 2, 3, 4, 5]"),
            param({"a": [1, 2, 3, 4, 5, 6]}, "a=[1, 2, 3, 4, 5, 6]"),
            param({"a": [1, 2, 3, 4, 5, 6, 7]}, "a=[1, 2, 3, 4, 5, 6, ...]"),
            param({"a": [1, 2, 3, 4, 5, 6, 7, 8]}, "a=[1, 2, 3, 4, 5, 6, ...]"),
        ],
    )
    def test_main(self, *, mapping: Mapping[str, Any], expected: str) -> None:
        result = _custom_mapping_repr(mapping)
        assert result == expected


class TestFilterMapping:
    @mark.parametrize(
        ("include_underscore", "include_none", "expected"),
        [
            param(False, False, {"a": 1, "c": 3}),
            param(False, True, {"a": 1, "b": None, "c": 3}),
            param(True, False, {"a": 1, "c": 3, "_underscore": 4}),
            param(True, True, {"a": 1, "b": None, "c": 3, "_underscore": 4}),
        ],
    )
    def test_main(
        self,
        *,
        include_underscore: bool,
        include_none: bool,
        expected: Mapping[str, Any],
    ) -> None:
        mapping = {"a": 1, "b": None, "c": 3, "_underscore": 4}
        result = _filter_mapping(
            mapping, include_underscore=include_underscore, include_none=include_none
        )
        assert result == expected


class TestReprLocals:
    @mark.parametrize(
        ("b", "include_none", "expected"),
        [
            param(2, False, "a=1, b=2, total=3"),
            param(2, True, "a=1, b=2, total=3"),
            param(None, False, "a=1, total=1"),
            param(None, True, "a=1, b=None, total=1"),
        ],
    )
    def test_main(self, *, b: int | None, include_none: bool, expected: str) -> None:
        def func(a: int, /, *, b: int | None = None) -> str:
            init = ReprLocals(locals(), func, include_none=include_none)
            total = a if b is None else (a + b)
            return f"{init}, total={total}"

        result = func(1, b=b)
        assert result == expected

    def test_fill(self) -> None:
        def func(
            *,
            a: Sequence[int],
            b: Sequence[int],
            c: Sequence[int],
            d: Sequence[int],
            e: Sequence[int],
            f: Sequence[int],
        ) -> str:
            init = ReprLocals(locals(), func)
            total = sum(chain(a, b, c, d, e, f))
            return f"{init}, total={total}"

        eight = [1, 2, 3, 4, 5, 6, 7, 8]
        result = func(a=eight, b=eight, c=eight, d=eight, e=eight, f=eight)
        expected = "a=[1, 2, 3, 4, 5, 6, ...], b=[1, 2, 3, 4, 5, 6, ...], c=[1, 2, 3, 4, 5, 6, ...], d=[1, 2, 3, 4, 5, 6, ...], e=[1, 2, 3, 4, 5, 6, ...], f=[1, 2, 3, 4, 5, 6, ...], total=216"
        assert result == expected
