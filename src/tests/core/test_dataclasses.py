from __future__ import annotations

from tests.test_typing_funcs.with_future import DataClassFutureIntDefault
from utilities.constants import sentinel
from utilities.core import replace_non_sentinel


class TestReplaceNonSentinel:
    def test_main(self) -> None:
        obj = DataClassFutureIntDefault()
        assert obj.int_ == 0
        obj1 = replace_non_sentinel(obj, int_=1)
        assert obj1.int_ == 1
        obj2 = replace_non_sentinel(obj1, int_=sentinel)
        assert obj2.int_ == 1

    def test_in_place(self) -> None:
        obj = DataClassFutureIntDefault()
        assert obj.int_ == 0
        replace_non_sentinel(obj, int_=1, in_place=True)
        assert obj.int_ == 1
        replace_non_sentinel(obj, int_=sentinel, in_place=True)
        assert obj.int_ == 1
