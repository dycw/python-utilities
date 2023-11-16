from __future__ import annotations

from typing import Any

from hypothesis import given
from hypothesis.strategies import dictionaries, frozensets, integers, sets
from pytest import mark, param

from utilities.hashlib import md5_hash


class TestMD5Hash:
    @mark.parametrize(
        ("obj", "expected"),
        [
            param(b"", "d41d8cd98f00b204e9800998ecf8427e"),
            param(b"bytes", "4b3a6218bb3e3a7303e8a171a60fcf92"),
            param("", "d41d8cd98f00b204e9800998ecf8427e"),
            param("text", "1cb251ec0d568de6a929b520c4aed8d1"),
            param(None, "7d1e55650014e21b4568ed3e3d1fc531"),
        ],
    )
    def test_main(self, *, obj: Any, expected: str) -> None:
        assert md5_hash(obj) == expected

    @given(
        x=dictionaries(integers(0, 10), integers(0, 10), max_size=2),
        y=dictionaries(integers(0, 10), integers(0, 10), max_size=2),
    )
    def test_dicts(self, *, x: dict[int, int], y: dict[int, int]) -> None:
        res = md5_hash(x) == md5_hash(y)
        expected = x == y
        assert res is expected

    @given(
        x=sets(integers(0, 10), max_size=2),
        y=sets(integers(0, 10), max_size=2),
    )
    def test_sets(self, *, x: set[int], y: set[int]) -> None:
        res = md5_hash(x) == md5_hash(y)
        expected = x == y
        assert res is expected

    @given(
        x=frozensets(integers(0, 10), max_size=2),
        y=frozensets(integers(0, 10), max_size=2),
    )
    def test_frozensets(self, *, x: frozenset[int], y: frozenset[int]) -> None:
        res = md5_hash(x) == md5_hash(y)
        expected = x == y
        assert res is expected
