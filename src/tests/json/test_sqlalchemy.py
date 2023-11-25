from __future__ import annotations

from hypothesis import given
from sqlalchemy import Engine

from utilities.hypothesis import sqlite_engines
from utilities.json import deserialize, serialize


class TestSerializeAndDeserialize:
    @given(x=sqlite_engines(), y=sqlite_engines())
    def test_engine(self, *, x: Engine, y: Engine) -> None:
        ser_x = serialize(x)
        assert deserialize(ser_x).url == x.url
        res = ser_x == serialize(y)
        expected = x.url == y.url
        assert res is expected
