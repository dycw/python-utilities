from __future__ import annotations

from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import integers

from utilities.hypothesis import temp_paths, text_ascii
from utilities.shelve import yield_shelf

if TYPE_CHECKING:
    from pathlib import Path


class TestYieldShelf:
    @given(path=temp_paths(), key=text_ascii(), value=integers())
    def test_main(self, *, path: Path, key: str, value: int) -> None:
        with yield_shelf(path) as shelf:
            shelf[key] = value
        with yield_shelf(path) as shelf:
            result = shelf[key]
        assert result == value
