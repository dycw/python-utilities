from __future__ import annotations

from typing import Any

from polars import int_range
from pytest import mark, param

from utilities.functions import identity
from utilities.inspect import bind_custom_repr, extract_bound_args_repr


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


class TestExtractBoundArgsRepr:
    @mark.parametrize(
        ("obj", "expected"),
        [
            param(None, "obj='None'"),
            param(
                int_range(end=5, eager=True).rename("name"),
                r'''obj="shape: (5,)\nSeries: 'name' [i64]\n[\n\t0\n\t1\n\t2\n\t3\n\t4\n]"''',
            ),
        ],
    )
    def test_main(self, *, obj: Any, expected: str) -> None:
        result = extract_bound_args_repr(bind_custom_repr(identity, obj))
        assert result == expected
