from __future__ import annotations

from pathlib import Path
from string import printable

from pathvalidate import ValidationError
from pytest import raises

from utilities.pathvalidate import ensure_path


class TestEnsurePath:
    def test_main(self, *, tmp_path: Path) -> None:
        assert ensure_path(tmp_path) == tmp_path

    def test_error(self, *, tmp_path: Path) -> None:
        name = 100 * printable
        with raises(ValidationError):
            _ = ensure_path(tmp_path.joinpath(name))

    def test_sanitize(self, *, tmp_path: Path) -> None:
        name = 100 * printable
        path = ensure_path(tmp_path.joinpath(name), sanitize=True)
        path.parent.mkdir(parents=True)
        path.touch()
        assert path.exists()
