from __future__ import annotations

from types import NoneType

from pyrsistent import PTypeError
from pytest import raises

from utilities.pyrsistent import PRecord, field


class TestPRecord:
    def test_mandatory_field_without_type_checking(self) -> None:
        class ARecord(PRecord):
            x: int = field()

        r = ARecord(x=3)
        assert repr(r) == "ARecord(x=3)"
        assert r.x == 3
        assert r.set(x=2) == ARecord(x=2)
        with raises(
            AttributeError, match="'y' is not among the specified fields for ARecord"
        ):
            _ = r.set(y=2)

    def test_optional_field_without_type_checking(self) -> None:
        class ARecord(PRecord):
            x: int | None = field(default=None)

        r = ARecord()
        assert repr(r) == "ARecord(x=None)"
        assert r.x is None

    def test_mandatory_field_with_type_checking(self) -> None:
        class ARecord(PRecord):
            x: int = field(type=int)

        r = ARecord(x=3)
        assert repr(r) == "ARecord(x=3)"
        with raises(PTypeError, match="Invalid type for field ARecord.x, was str"):
            _ = r.set(x="2")

    def test_optional_field_with_type_checking(self) -> None:
        class ARecord(PRecord):
            x: int | None = field(type=(int, NoneType), default=None)

        r = ARecord()
        assert repr(r) == "ARecord(x=None)"
        with raises(PTypeError, match="Invalid type for field ARecord.x, was str"):
            _ = r.set(x="2")
