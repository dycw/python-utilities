from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from zoneinfo import available_timezones

from hypothesis import given
from hypothesis.strategies import sampled_from
from pytest import mark, param

from utilities.platform import SYSTEM
from utilities.types import Dataclass, Number, PathLike, TimeZone
from utilities.typing import get_literal_elements


class TestDataClassProtocol:
    def test_main(self) -> None:
        def identity[T: Dataclass](x: T, /) -> T:
            return x

        @dataclass(kw_only=True, slots=True)
        class Example:
            x: None = None

        _ = identity(Example())


class TestNumber:
    @given(x=sampled_from([0, 0.0]))
    def test_ok(self, *, x: Number) -> None:
        assert isinstance(x, int | float)

    def test_error(self) -> None:
        assert not isinstance(None, int | float)


class TestPathLike:
    @mark.parametrize("path", [param(Path.home()), param("~")])
    def test_main(self, *, path: PathLike) -> None:
        assert isinstance(path, Path | str)

    def test_error(self) -> None:
        assert not isinstance(None, Path | str)


class TestTimeZone:
    def test_main(self) -> None:
        result = set(get_literal_elements(TimeZone))
        expected = available_timezones()
        match SYSTEM:
            case "windows" | "mac":
                assert result == expected
            case "linux":
                assert result | {"localtime"} == expected
