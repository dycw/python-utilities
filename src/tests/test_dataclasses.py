from __future__ import annotations

from dataclasses import dataclass
from types import NoneType
from typing import Any

import pytest

from utilities.dataclasses import (
    GetDataClassClassError,
    get_dataclass_class,
    is_dataclass_class,
    is_dataclass_instance,
    replace_non_sentinel,
    yield_field_names,
)
from utilities.sentinel import sentinel


class TestGetDataClassClass:
    def test_main(self) -> None:
        @dataclass
        class Example:
            x: None = None

        for obj in [Example(), Example]:
            assert get_dataclass_class(obj) is Example

    def test_error(self) -> None:
        with pytest.raises(GetDataClassClassError):
            _ = get_dataclass_class(None)  # type: ignore[]


class TestIsDataClassClass:
    def test_main(self) -> None:
        @dataclass
        class Example:
            x: None = None

        assert is_dataclass_class(Example)
        assert not is_dataclass_class(Example())

    @pytest.mark.parametrize("obj", [pytest.param(None), pytest.param(NoneType)])
    def test_others(self, *, obj: Any) -> None:
        assert not is_dataclass_class(obj)


class TestIsDataClassInstance:
    def test_main(self) -> None:
        @dataclass
        class Example:
            x: None = None

        assert not is_dataclass_instance(Example)
        assert is_dataclass_instance(Example())

    @pytest.mark.parametrize("obj", [pytest.param(None), pytest.param(NoneType)])
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

    @pytest.mark.parametrize("obj", [pytest.param(None), pytest.param(NoneType)])
    def test_others(self, *, obj: Any) -> None:
        assert not is_dataclass_instance(obj)


class TestYieldDataClassFieldNames:
    def test_main(self) -> None:
        @dataclass
        class Example:
            x: None = None

        for obj in [Example(), Example]:
            assert list(yield_field_names(obj)) == ["x"]
