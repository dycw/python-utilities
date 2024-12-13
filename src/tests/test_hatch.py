from __future__ import annotations

from re import search

from utilities.hatch import get_version


class TestGetVersion:
    def test_main(self) -> None:
        result = get_version()
        assert result is not None
        assert search(r"^\d+\.\d+\.\d+$", result)
