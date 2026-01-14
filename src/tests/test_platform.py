from __future__ import annotations

from re import search
from typing import TYPE_CHECKING, assert_never

from hypothesis import assume, given
from pytest import mark, param

from utilities.hypothesis import text_clean
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
    get_strftime,
    get_system,
    maybe_lower_case,
)
from utilities.text import unique_str
from utilities.typing import get_args

if TYPE_CHECKING:
    from pathlib import Path


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
            case never:
                assert_never(never)

    def test_constant(self) -> None:
        match SYSTEM:
            case "windows":  # skipif-not-windows
                assert MAX_PID is None
            case "mac":  # skipif-not-macos
                assert isinstance(MAX_PID, int)
            case "linux":  # skipif-not-linux
                assert isinstance(MAX_PID, int)
            case never:
                assert_never(never)


class TestGetStrftime:
    @given(text=text_clean())
    def test_main(self, *, text: str) -> None:
        result = get_strftime(text)
        _ = assume(not search("%Y", result))
        assert not search("%Y", result)


class TestGetSystem:
    def test_function(self) -> None:
        assert get_system() in get_args(System)

    def test_constant(self) -> None:
        assert SYSTEM in get_args(System)

    @mark.parametrize(
        "predicate",
        [
            param(IS_WINDOWS, id="IS_WINDOWS"),
            param(IS_MAC, id="IS_MAC"),
            param(IS_LINUX, id="IS_LINUX"),
            param(IS_NOT_WINDOWS, id="IS_NOT_WINDOWS"),
            param(IS_NOT_MAC, id="IS_NOT_MAC"),
            param(IS_NOT_LINUX, id="IS_NOT_LINUX"),
        ],
    )
    def test_predicates(self, *, predicate: bool) -> None:
        assert isinstance(predicate, bool)


class TestMaybeLowerCase:
    def test_main(self, *, tmp_path: Path) -> None:
        upper = unique_str().upper()
        lower = upper.lower()
        file_upper = tmp_path / upper
        file_upper.touch()
        file_lower = tmp_path / lower
        file_lower.touch()
        maybe_lower = maybe_lower_case(upper)
        result = len(list(tmp_path.iterdir()))
        expected = 2 if maybe_lower == upper else 1
        assert result == expected
