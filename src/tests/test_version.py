from __future__ import annotations
from hypothesis import given,integers

from re import search

from utilities.version import Version, get_hatch_version


class TestGetHatchVersion:
    def test_main(self) -> None:
        version = get_hatch_version()
        assert isinstance(version, Version)

class TestParseVersion:
    @given(major=integers(min_value=0))
