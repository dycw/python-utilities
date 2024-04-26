from __future__ import annotations

from dataclasses import dataclass

from hypothesis import given
from hypothesis.strategies import integers
from pytest import raises

from utilities.functions import CheckNameError, check_name, identity


class TestCheckName:
    def test_main(self) -> None:
        @dataclass(kw_only=True)
        class Example:
            name: str

        check_name(Example(name="name"), "name")

    def test_error(self) -> None:
        @dataclass(kw_only=True)
        class Example:
            name: str

        with raises(CheckNameError, match=r"Object must have name .*; got .*\."):
            check_name(Example(name="foo"), "bar")


class TestIdentity:
    @given(x=integers())
    def test_main(self, *, x: int) -> None:
        assert identity(x) == x
