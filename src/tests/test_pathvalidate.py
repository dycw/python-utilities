from __future__ import annotations

from pathlib import Path

from pathvalidate import ValidationError
from pytest import raises

from utilities.pathvalidate import valid_path, valid_path_cwd, valid_path_home


class TestValidPath:
    def test_main(self) -> None:
        assert isinstance(valid_path(Path("abc")), Path)

    def test_error_validation(self) -> None:
        with raises(ValidationError):
            _ = valid_path("\0")

    def test_error_sanitized(self) -> None:
        assert valid_path("a\0b", sanitize=True) == Path("ab")


class TestValidPathCwd:
    def test_main(self) -> None:
        assert valid_path_cwd() == Path.cwd()


class TestValidPathHome:
    def test_main(self) -> None:
        assert valid_path_home() == Path.home()
