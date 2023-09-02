from __future__ import annotations

from re import search

from hypothesis import given
from pytest import mark

from utilities.hypothesis import text_clean
from utilities.platform import SYSTEM, System
from utilities.platform.datetime import maybe_sub_pct_y


class TestMaybeMaybeSubPctY:
    @given(text=text_clean())
    @mark.skipif(SYSTEM is not System.linux, reason="Linux only")
    def test_main(self, text: str) -> None:
        result = maybe_sub_pct_y(text)
        assert not search("%Y", result)
