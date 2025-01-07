from __future__ import annotations

from typing import TYPE_CHECKING, assert_never

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
from utilities.typing import get_args

if TYPE_CHECKING:
    from collections.abc import Set as AbstractSet


class TestMaybeYieldLowerCase:
    @given(text=sets(text_ascii()))
    def test_main(self, *, text: AbstractSet[str]) -> None:
        result = set(maybe_yield_lower_case(text))
        match SYSTEM:
            case "windows":  # skipif-not-windows
                assert all(text == text.lower() for text in result)
            case "mac":  # skipif-not-macos
                assert all(text == text.lower() for text in result)
            case "linux":  # skipif-not-linux
                assert result == text
            case _ as never:
                assert_never(never)


class TestSystem:
    def test_function(self) -> None:
        assert get_system() in get_args(System)

    def test_constant(self) -> None:
        assert SYSTEM in get_args(System)

    @mark.parametrize(
        "predicate",
        [
            param(IS_WINDOWS),
            param(IS_MAC),
            param(IS_LINUX),
            param(IS_NOT_WINDOWS),
            param(IS_NOT_MAC),
            param(IS_NOT_LINUX),
        ],
    )
    def test_predicates(self, *, predicate: bool) -> None:
        assert isinstance(predicate, bool)
