from __future__ import annotations

from typing import TYPE_CHECKING, assert_never

from hypothesis import given
from hypothesis.strategies import sampled_from, sets

from utilities.hypothesis import text_ascii
from utilities.platform import (
    IS_LINUX,
    IS_MAC,
    IS_NOT_LINUX,
    IS_NOT_MAC,
    IS_NOT_WINDOWS,
    IS_WINDOWS,
    MAX_PID,
    SYSTEM,
    System,
    get_max_pid,
    get_system,
    maybe_yield_lower_case,
)
from utilities.typing import get_args

if TYPE_CHECKING:
    from collections.abc import Set as AbstractSet


class TestGetMaxPID:
    def test_function(self) -> None:
        result = get_max_pid()
        match SYSTEM:
            case "windows":  # skipif-not-windows
                assert result is None
            case "mac":  # skipif-not-macos
                assert isinstance(result, int)
            case "linux":  # skipif-not-linux
                assert isinstance(result, int)
            case _ as never:
                assert_never(never)

    def test_constant(self) -> None:
        match SYSTEM:
            case "windows":  # skipif-not-windows
                assert MAX_PID is None
            case "mac":  # skipif-not-macos
                assert isinstance(MAX_PID, int)
            case "linux":  # skipif-not-linux
                assert isinstance(MAX_PID, int)
            case _ as never:
                assert_never(never)


class TestGetSystem:
    def test_function(self) -> None:
        assert get_system() in get_args(System)

    def test_constant(self) -> None:
        assert SYSTEM in get_args(System)

    @given(
        predicate=sampled_from([
            IS_WINDOWS,
            IS_MAC,
            IS_LINUX,
            IS_NOT_WINDOWS,
            IS_NOT_MAC,
            IS_NOT_LINUX,
        ])
    )
    def test_predicates(self, *, predicate: bool) -> None:
        assert isinstance(predicate, bool)


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
