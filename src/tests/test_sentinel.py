from __future__ import annotations

from typing import Any

from pytest import mark, param

from utilities.sentinel import is_sentinel, sentinel


class TestIsSentinel:
    @mark.parametrize(("obj", "expected"), [param(None, False), param(sentinel, True)])
    def test_main(self, *, obj: Any, expected: bool) -> None:
        assert is_sentinel(obj) is expected
