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

from utilities.hashlib import md5_hash
from utilities.hypothesis import int64s, text_ascii


class TestMD5Hash:
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
        res = md5_hash(x) == md5_hash(y)
        expected = x == y
        assert res is expected
