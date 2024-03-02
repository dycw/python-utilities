from __future__ import annotations

from typing_extensions import Self

from utilities.hatch import get_hatch_version


class TestGetHatchVersion:
    def test_main(self: Self) -> None:
        major, minor, patch = get_hatch_version()
        assert major >= 0
        assert minor >= 0
        assert patch >= 0
