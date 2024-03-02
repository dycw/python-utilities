from __future__ import annotations

from pathlib import Path

from pathvalidate import ValidationError
from pytest import raises
from typing_extensions import Self

from utilities.pathvalidate import valid_path, valid_path_cwd, valid_path_home


class TestValidPath:
    def test_main(self: Self) -> None:
        assert isinstance(valid_path(Path("abc")), Path)

    def test_error_validation(self: Self) -> None:
        with raises(ValidationError):
            _ = valid_path("\0")

    def test_error_sanitized(self: Self) -> None:
        assert valid_path("a\0b", sanitize=True) == Path("ab")


class TestValidPathCwd:
    def test_main(self: Self) -> None:
        assert valid_path_cwd() == Path.cwd()


class TestValidPathHome:
    def test_main(self: Self) -> None:
        assert valid_path_home() == Path.home()
