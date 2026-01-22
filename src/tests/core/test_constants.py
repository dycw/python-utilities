from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pytest import mark, param

from utilities.constants import sentinel
from utilities.core import is_none, is_not_none, is_sentinel

if TYPE_CHECKING:
    from collections.abc import Callable


class TestIsNoneAndIsNotNone:
    @mark.parametrize(
        ("func", "obj", "expected"),
        [
            param(is_none, None, True),
            param(is_none, 0, False),
            param(is_not_none, None, False),
            param(is_not_none, 0, True),
        ],
    )
    def test_main(
        self, *, func: Callable[[Any], bool], obj: Any, expected: bool
    ) -> None:
        assert func(obj) is expected


class TestIsSentinel:
    @mark.parametrize(("obj", "expected"), [param(None, False), param(sentinel, True)])
    def test_main(self, *, obj: Any, expected: bool) -> None:
        assert is_sentinel(obj) is expected
