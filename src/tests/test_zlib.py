from __future__ import annotations

from typing import Any

from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    SearchStrategy,
    data,
    dictionaries,
    frozensets,
    lists,
    sets,
    tuples,
)
from pytest import mark, param

from utilities.hypothesis import int64s, text_ascii
from utilities.zlib import crc32_hash


class TestCRC32Hash:
    @given(data=data())
    @mark.parametrize(
        "strategy",
        [
            param(dictionaries(text_ascii(), int64s(), max_size=3)),
            param(frozensets(int64s(), max_size=3)),
            param(lists(int64s(), max_size=3)),
            param(sets(int64s(), max_size=3)),
        ],
    )
    def test_main(self, *, data: DataObject, strategy: SearchStrategy[Any]) -> None:
        x, y = data.draw(tuples(strategy, strategy))
        res = crc32_hash(x) == crc32_hash(y)
        expected = x == y
        assert res is expected
