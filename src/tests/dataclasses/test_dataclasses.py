from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pytest import mark, param, raises

from utilities.dataclasses import (
    NotADataClassNorADataClassInstanceError,
    get_dataclass_class,
    is_dataclass_class,
    is_dataclass_instance,
)
from utilities.dataclasses.dataclasses import replace_non_sentinel, yield_field_names
from utilities.sentinel import sentinel
from utilities.types import NoneType


class TestGetDataClassClass:
    def test_main(self) -> None:
        @dataclass
        class Example:
            x: None = None

        for obj in [Example(), Example]:
            assert get_dataclass_class(obj) is Example

    def test_error(self) -> None:
        with raises(NotADataClassNorADataClassInstanceError):
            _ = get_dataclass_class(None)  # type: ignore


class TestIsDataClassClass:
    def test_main(self) -> None:
        @dataclass
        class Example:
            x: None = None

        assert is_dataclass_class(Example)
        assert not is_dataclass_class(Example())

    @mark.parametrize("obj", [param(None), param(NoneType)])
    def test_others(self, *, obj: Any) -> None:
        assert not is_dataclass_class(obj)


class TestIsDataClassInstance:
    def test_main(self) -> None:
        @dataclass
        class Example:
            x: None = None

        assert not is_dataclass_instance(Example)
        assert is_dataclass_instance(Example())

    @mark.parametrize("obj", [param(None), param(NoneType)])
    def test_others(self, *, obj: Any) -> None:
        assert not is_dataclass_instance(obj)


class TestReplaceNonSentinel:
    def test_main(self) -> None:
        @dataclass
        class Example:
            x: int = 0

        curr = Example()
        assert replace_non_sentinel(curr, x=1).x == 1
        assert replace_non_sentinel(curr, x=sentinel).x == 0

    @mark.parametrize("obj", [param(None), param(NoneType)])
    def test_others(self, *, obj: Any) -> None:
        assert not is_dataclass_instance(obj)


class TestYieldDataClassFieldNames:
    def test_main(self) -> None:
        @dataclass
        class Example:
            x: None = None

        for obj in [Example(), Example]:
            assert list(yield_field_names(obj)) == ["x"]
