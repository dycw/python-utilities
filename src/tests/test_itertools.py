from pytest import mark
from pytest import param

from utilities.beartype import IterableStrs
from utilities.itertools import chunked
from utilities.itertools import take


class TestChunked:
    @mark.parametrize(
        ["iterable", "n", "expected"],
        [
            param("ABCDEF", 3, [["A", "B", "C"], ["D", "E", "F"]]),
            param("ABCDE", 3, [["A", "B", "C"], ["D", "E"]]),
            param("ABCDE", None, [["A", "B", "C", "D", "E"]]),
        ],
    )
    def test_main(
        self, iterable: IterableStrs, n: int | None, expected: list[list[str]]
    ) -> None:
        assert list(chunked(iterable, n)) == expected


class TestTake:
    @mark.parametrize(
        ["n", "expected"],
        [
            param(0, []),
            param(1, [0]),
            param(2, [0, 1]),
            param(3, [0, 1, 2]),
            param(4, [0, 1, 2]),
        ],
    )
    def test_main(self, n: int, expected: list[int]) -> None:
        assert take(n, range(3)) == expected
