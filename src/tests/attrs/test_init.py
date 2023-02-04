from typing import Any, cast

from attrs import define, fields
from beartype.door import die_if_unbearable
from pytest import raises

from utilities.attrs import AttrsBase, FieldTypeError
from utilities.timer import Timer


class TestAttrsBase:
    def test_main(self) -> None:
        @define
        class Example(AttrsBase):
            x: int

        match = "module = tests.attrs.test_init, class = Example, field = x"
        with raises(FieldTypeError, match=match):
            _ = Example(None)  # type: ignore[]

    def test_no_fields(self) -> None:
        @define
        class Example(AttrsBase):
            ...

        _ = Example()

    def test_speed(self) -> None:
        @define
        class Example(AttrsBase):
            x: int
            y: int
            z: int

        @define
        class Full:
            x: int
            y: int
            z: int

            def __attrs_post_init__(self) -> None:
                for field in fields(cast(Any, type(self))):
                    die_if_unbearable(getattr(self, field.name), field.type)

        n = int(1e4)
        with Timer() as timer1:
            for _ in range(n):
                _ = Example(0, 0, 0)
        with Timer() as timer2:
            for _ in range(n):
                _ = Full(0, 0, 0)
        assert timer1 < timer2
