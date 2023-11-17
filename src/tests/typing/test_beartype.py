from __future__ import annotations

from beartype.door import die_if_unbearable
from beartype.roar import BeartypeAbbyHintViolation
from pytest import mark, param, raises

from utilities.types import Number
from utilities.typing import IterableStrs, SequenceStrs


class TestIterableStrs:
    @mark.parametrize(
        "x",
        [
            param(["a", "b", "c"]),
            param(("a", "b", "c")),
            param({"a", "b", "c"}),
            param({"a": 1, "b": 2, "c": 3}),
        ],
    )
    def test_pass(self, *, x: IterableStrs) -> None:
        die_if_unbearable(x, IterableStrs)

    def test_fail(self) -> None:
        with raises(BeartypeAbbyHintViolation):
            die_if_unbearable("abc", SequenceStrs)


class TestSequenceStrs:
    @mark.parametrize("x", [param(["a", "b", "c"]), param(("a", "b", "c"))])
    def test_pass(self, *, x: SequenceStrs) -> None:
        die_if_unbearable(x, SequenceStrs)

    @mark.parametrize(
        "x", [param({"a", "b", "c"}), param({"a": 1, "b": 2, "c": 3}), param("abc")]
    )
    def test_fail(self, *, x: IterableStrs | str) -> None:
        with raises(BeartypeAbbyHintViolation):
            die_if_unbearable(x, SequenceStrs)


class TestNumber:
    @mark.parametrize("x", [param(0), param(0.0)])
    def test_main(self, *, x: Number) -> None:
        die_if_unbearable(x, Number)
