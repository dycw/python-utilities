from typing import Any
from typing import cast

from attrs import define
from attrs import fields
from beartype import beartype
from beartype.door import die_if_unbearable
from beartype.roar import BeartypeAbbyHintViolation
from pytest import raises

from utilities.attrs import AttrsBase
from utilities.timer import Timer


class TestAttrsBase:
    def test_main(self) -> None:
        @define
        class Example(AttrsBase):
            x: list[int]

        with raises(BeartypeAbbyHintViolation):
            _ = Example(["0"])  # type: ignore

    def test_empty(self) -> None:
        @define
        class Example(AttrsBase):
            pass

        _ = Example()

    def test_speed(self) -> None:
        @define
        class Example:
            x: int
            y: int

        @define
        class Full:
            x: int
            y: int

            @beartype
            def __attrs_post_init__(self) -> None:
                for field in fields(cast(Any, type(self))):
                    die_if_unbearable(getattr(self, field.name), field.type)

        n = int(1e4)
        with Timer() as timer1:
            for _ in range(n):
                _ = Example(0, 0)
        with Timer() as timer2:
            for _ in range(n):
                _ = Full(0, 0)
        assert timer1 < timer2
