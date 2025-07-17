from __future__ import annotations

from typing import TYPE_CHECKING

from hypothesis import HealthCheck, given, settings
from hypothesis.strategies import integers

from utilities.hypothesis import text_ascii
from utilities.shelve import yield_shelf

if TYPE_CHECKING:
    from pathlib import Path


class TestYieldShelf:
    @given(key=text_ascii(), value=integers())
    @settings(
        max_examples=1, suppress_health_check={HealthCheck.function_scoped_fixture}
    )
    def test_main(self, *, key: str, value: int, tmp_path: Path) -> None:
        path = tmp_path.joinpath("shelf")
        with yield_shelf(path) as shelf:
            shelf[key] = value
        with yield_shelf(path) as shelf:
            result = shelf[key]
        assert result == value
