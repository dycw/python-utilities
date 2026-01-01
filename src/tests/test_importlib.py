from __future__ import annotations

from pytest import mark, param

from utilities.importlib import files, is_valid_import
from utilities.pathlib import get_repo_root


class TestFiles:
    def test_main(self) -> None:
        result = files(anchor="utilities")
        expected = get_repo_root() / "src/utilities"
        assert result == expected


class TestIsValidImport:
    @mark.parametrize(
        ("module", "name", "expected"),
        [
            param("importlib", None, True),
            param("invalid", None, False),
            param("utilities.importlib", "is_valid_import", True),
            param("utilities.importlib", "invalid", False),
            param("invalid", "invalid", False),
        ],
    )
    def test_main(self, *, module: str, name: str | None, expected: bool) -> None:
        result = is_valid_import(module, name=name)
        assert result is expected
