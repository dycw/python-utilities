from re import search

from beartype import beartype
from hypothesis import given

from utilities.hypothesis import text_clean
from utilities.platform import SYSTEM, System
from utilities.platform.datetime import maybe_sub_pct_y
from utilities.typing import never


class TestMaybeMaybeSubPctY:
    @given(text=text_clean())
    @beartype
    def test_main(self, text: str) -> None:
        result = maybe_sub_pct_y(text)
        if SYSTEM is System.windows:  # noqa: SIM114 # pragma: os-ne-windows
            assert not search("%Y", text)
        elif SYSTEM is System.mac_os:  # pragma: os-ne-macos
            assert not search("%Y", text)
        elif SYSTEM is System.linux:  # pragma: os-ne-linux
            assert result == text
        else:  # pragma: no cover
            never(SYSTEM)
