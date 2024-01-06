from __future__ import annotations

from utilities.sys import VERSION_MAJOR_MINOR, is_pytest


class TestIsPytest:
    def test_main(self) -> None:
        assert is_pytest()


class TestVersionMajorMinor:
    def test_main(self) -> None:
        assert isinstance(VERSION_MAJOR_MINOR, tuple)
        expected = 2
        assert len(VERSION_MAJOR_MINOR) == expected
