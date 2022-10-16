from collections.abc import Iterable

from beartype.door import die_if_unbearable
from beartype.roar import BeartypeAbbyHintViolation
from pytest import raises

from utilities.beartype import IterableStrs


class TestIterableStrs:
    def test_main(self) -> None:
        die_if_unbearable(["a", "b", "c"], IterableStrs)
        die_if_unbearable("abc", Iterable[str])
        with raises(BeartypeAbbyHintViolation):
            die_if_unbearable("abc", IterableStrs)
