from re import escape, search

from rich.console import Console

from tests.rich.funcs import func1
from utilities.pytest import skipif_windows
from utilities.rich import get_printed_exception


class TestGetPrintedException:
    @skipif_windows
    def test_main(self) -> None:
        try:
            _ = func1(1)
        except ZeroDivisionError:
            result = get_printed_exception()
            expected = [
                """
│    1 def func1(x: int, /) -> int:                                                                │
│    2 │   y = 2                                                                                   │
│ ❱  3 │   return func2(x, y)                                                                      │
│    4                                                                                             │
│    5                                                                                             │
│    6 def func2(x: int, y: int, /) -> int:                                                        │
""",
                """
│    5                                                                                             │
│    6 def func2(x: int, y: int, /) -> int:                                                        │
│    7 │   z = 3                                                                                   │
│ ❱  8 │   return func3(x, y, z)                                                                   │
│    9                                                                                             │
│   10                                                                                             │
│   11 def func3(x: int, y: int, z: int, /) -> int:                                                │
""",
                """
│    9                                                                                             │
│   10                                                                                             │
│   11 def func3(x: int, y: int, z: int, /) -> int:                                                │
│ ❱ 12 │   return (x + y + z) // 0                                                                 │
│   13                                                                                             │
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
