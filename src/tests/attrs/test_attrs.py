from functools import cached_property
from typing import Any
from typing import cast

from attrs import define
from attrs import fields
from beartype.door import die_if_unbearable
from pytest import raises

from utilities.attrs import AttrsBase
from utilities.attrs import DictMixin
from utilities.attrs import FieldTypeError
from utilities.attrs import make_dict_field
from utilities.timer import Timer


class TestAttrsBase:
    def test_main(self) -> None:
        @define
        class Example(AttrsBase):
            x: int

        match = "module = tests.attrs.test_attrs, class = Example, field = x"
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


class TestCachedProperties:
    def test_with_base(self) -> None:
        class Base:
            ...

        counter = 0

        @define
        class Example(Base, DictMixin):
            @cached_property
            def value(self) -> int:
                nonlocal counter
                counter += 1
                return counter

        obj = Example()
        for _ in range(2):
            assert obj.value == 1

    def test_without_base(self) -> None:
        counter = 0

        @define
        class Example:
            __dict__ = make_dict_field()

            @cached_property
            def value(self) -> int:
                nonlocal counter
                counter += 1
                return counter

        obj = Example()
        for _ in range(2):
            assert obj.value == 1
