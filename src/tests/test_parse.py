from __future__ import annotations

from hypothesis import given
from hypothesis.strategies import booleans


class TestParseText:
    @given(bool_=booleans())
    def test_bool(self, *, bool_: bool) -> None:
        pass
