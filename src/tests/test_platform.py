from __future__ import annotations

from typing import TYPE_CHECKING, Any, assert_never

from hypothesis import given
from hypothesis.strategies import sets
from pytest import mark, param

from utilities.hypothesis import text_ascii
from utilities.platform import (
    IS_LINUX,
    IS_MAC,
    IS_NOT_LINUX,
    IS_NOT_MAC,
    IS_NOT_WINDOWS,
    IS_WINDOWS,
    SYSTEM,
    System,
    get_system,
    maybe_yield_lower_case,
)

if TYPE_CHECKING:
    from collections.abc import Set as AbstractSet


class TestMaybeYieldLowerCase:
    @given(text=sets(text_ascii()))
    def test_main(self, *, text: AbstractSet[str]) -> None:
        result = set(maybe_yield_lower_case(text))
        match SYSTEM:
            case System.windows:  # skipif-not-windows
                assert all(text == text.lower() for text in result)
            case System.mac:  # skipif-not-macos
                assert all(text == text.lower() for text in result)
            case System.linux:  # skipif-not-linux
                assert result == text
            case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
                assert_never(never)


class TestSystem:
    def test_function(self) -> None:
        assert isinstance(get_system(), System)

    @mark.parametrize(
        ("constant", "cls"),
        [
            param(SYSTEM, System),
            param(IS_WINDOWS, bool),
            param(IS_MAC, bool),
            param(IS_LINUX, bool),
            param(IS_NOT_WINDOWS, bool),
            param(IS_NOT_MAC, bool),
            param(IS_NOT_LINUX, bool),
        ],
    )
    def test_constants(self, *, constant: Any, cls: type) -> None:
        assert isinstance(constant, cls)
