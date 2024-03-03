from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest
from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    SearchStrategy,
    data,
    dictionaries,
    frozensets,
    integers,
    lists,
    sets,
    tuples,
)

from utilities.hashlib import md5_hash


@dataclass
class _Example:
    value: int


class TestMD5Hash:
    @given(data=data())
    @pytest.mark.parametrize(
        "strategy",
        [
            pytest.param(dictionaries(integers(), integers(), max_size=3)),
            pytest.param(frozensets(integers(), max_size=3)),
            pytest.param(lists(integers(), max_size=3)),
            pytest.param(sets(integers(), max_size=3)),
        ],
    )
    def test_main(self, *, data: DataObject, strategy: SearchStrategy[Any]) -> None:
        x, y = data.draw(tuples(strategy, strategy))
        res = md5_hash(x) == md5_hash(y)
        expected = x == y
        assert res is expected

    @given(x=integers(), y=integers())
    def test_non_json_serializable(self, *, x: int, y: int) -> None:
        res = md5_hash(_Example(x)) == md5_hash(_Example(y))
        expected = x == y
        assert res is expected
