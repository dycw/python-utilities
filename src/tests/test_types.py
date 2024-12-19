from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TypeVar

from pytest import mark, param

from utilities.datetime import ZERO_TIME
from utilities.types import Dataclass, Duration, Number, PathLike


class TestDataClassProtocol:
    def test_main(self) -> None:
        TDataclass = TypeVar("TDataclass", bound=Dataclass)

        def identity(x: TDataclass, /) -> TDataclass:
            return x

        @dataclass(kw_only=True, slots=True)
        class Example:
            x: None = None

        _ = identity(Example())


class TestDuration:
    @mark.parametrize("x", [param(0), param(0.0), param(ZERO_TIME)])
    def test_success(self, *, x: Duration) -> None:
        assert isinstance(x, Duration)

    def test_error(self) -> None:
        assert not isinstance("0", Duration)


class TestNumber:
    @mark.parametrize("x", [param(0), param(0.0)])
    def test_ok(self, *, x: Number) -> None:
        assert isinstance(x, Number)

    def test_error(self) -> None:
        assert not isinstance(None, Number)


class TestPathLike:
    @mark.parametrize("path", [param(Path.home()), param("~")])
    def test_main(self, *, path: PathLike) -> None:
        assert isinstance(path, PathLike)

    def test_error(self) -> None:
        assert not isinstance(None, PathLike)
