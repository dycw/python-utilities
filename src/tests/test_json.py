import datetime as dt
from pathlib import Path
from typing import Any

from pytest import mark, param, raises

from utilities.datetime import UTC
from utilities.json import serialize


class TestSerialize:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(dt.date(2000, 1, 1), '"2000-01-01"'),
            param(
                dt.datetime(2000, 1, 1, 12, tzinfo=UTC), '"2000-01-01T12:00:00+00:00"'
            ),
            param(Path("a/b/c"), '"a/b/c"'),
            param({1, 2, 3}, '"set([1, 2, 3])"'),
            param({"a", "b", "c"}, '"set([\\"a\\", \\"b\\", \\"c\\"])"'),
            param(frozenset([1, 2, 3]), '"frozenset([1, 2, 3])"'),
            param(
                frozenset(["a", "b", "c"]), '"frozenset([\\"a\\", \\"b\\", \\"c\\"])"'
            ),
        ],
    )
    def test_main(self, x: Any, expected: str) -> None:
        assert serialize(x) == expected

    def test_error(self) -> None:
        class Example:
            pass

        with raises(TypeError, match="Invalid type"):
            _ = serialize(Example())
