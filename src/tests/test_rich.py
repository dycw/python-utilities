from __future__ import annotations

from re import escape, search
from typing import TYPE_CHECKING

from tests.rich.funcs import func1
from utilities.pytest import skipif_windows
from utilities.rich import get_printed_exception

if TYPE_CHECKING:
    from rich.console import Console


class TestGetPrintedException:
    @skipif_windows
    def test_main(self) -> None:
        try:
            _ = func1(1)
        except ZeroDivisionError:
            result = get_printed_exception()
            expected = [
                """
│    3                                                                                             │
│    4 def func1(x: int, /) -> int:                                                                │
│    5 │   y = 2                                                                                   │
│ ❱  6 │   return func2(x, y)                                                                      │
│    7                                                                                             │
│    8                                                                                             │
│    9 def func2(x: int, y: int, /) -> int:                                                        │
""",
                """
│    8                                                                                             │
│    9 def func2(x: int, y: int, /) -> int:                                                        │
│   10 │   z = 3                                                                                   │
│ ❱ 11 │   return func3(x, y, z)                                                                   │
│   12                                                                                             │
│   13                                                                                             │
│   14 def func3(x: int, y: int, z: int, /) -> int:                                                │
""",
                """
│   12                                                                                             │
│   13                                                                                             │
│   14 def func3(x: int, y: int, z: int, /) -> int:                                                │
│ ❱ 15 │   return (x + y + z) // 0                                                                 │
│   16                                                                                             │
""",
                """
ZeroDivisionError: integer division or modulo by zero
""",
            ]
            for exp in expected:
                pattern = escape(exp.strip("\n"))
                assert search(pattern, result)

    def test_before(self) -> None:
        def before(console: Console, /) -> None:
            console.log("before called")

        try:
            _ = func1(1)
        except ZeroDivisionError:
            result = get_printed_exception(before=before)
            assert search("before called", result)

    def test_after(self) -> None:
        def after(console: Console, /) -> None:
            console.log("after called")

        try:
            _ = func1(1)
        except ZeroDivisionError:
            result = get_printed_exception(after=after)
            assert search("after called", result)
