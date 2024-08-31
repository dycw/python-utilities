from __future__ import annotations

from typing import Any

from pytest import mark, param

from utilities.functions import identity
from utilities.inspect import bind_custom_repr


class TestBindCustomBindRepr:
    @mark.parametrize(
        ("obj", "expected"),
        [
            param(None, "<BoundArguments (obj='None')>"),
            param([], "<BoundArguments (obj='[]')>"),
            param([1], "<BoundArguments (obj='[1]')>"),
            param([1, 2], "<BoundArguments (obj='[1, 2]')>"),
            param([1, 2, 3], "<BoundArguments (obj='[1, 2, 3]')>"),
            param([1, 2, 3, 4], "<BoundArguments (obj='[1, 2, 3, 4]')>"),
            param([1, 2, 3, 4, 5], "<BoundArguments (obj='[1, 2, 3, 4, 5]')>"),
            param([1, 2, 3, 4, 5, 6], "<BoundArguments (obj='[1, 2, 3, 4, 5, 6]')>"),
            param(
                [1, 2, 3, 4, 5, 6, 7],
                "<BoundArguments (obj='[1, 2, 3, 4, 5, 6, ...]')>",
            ),
            param(
                [1, 2, 3, 4, 5, 6, 7, 8],
                "<BoundArguments (obj='[1, 2, 3, 4, 5, 6, ...]')>",
            ),
        ],
    )
    def test_main(self, *, obj: Any, expected: str) -> None:
        result = bind_custom_repr(identity, obj)
        assert str(result) == expected
