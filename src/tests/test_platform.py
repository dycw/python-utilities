from __future__ import annotations

from re import search
from typing import TYPE_CHECKING, assert_never

from hypothesis import assume, given
from pytest import mark, param

from utilities.hypothesis import text_clean
from utilities.platform import (
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


class TestGetStrftime:
    @given(text=text_clean())
    def test_main(self, *, text: str) -> None:
        result = get_strftime(text)
        _ = assume(not search("%Y", result))
        assert not search("%Y", result)


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
