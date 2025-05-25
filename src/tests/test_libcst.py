from __future__ import annotations

from hypothesis import given
from hypothesis.strategies import sampled_from
from libcst import Module, SimpleStatementLine

from utilities.libcst import generate_from_import, generate_import


class TestGenerateFromImport:
    @given(
        case=sampled_from([
            ("foo", "bar", None, "from foo import bar"),
            ("foo", "bar", "bar2", "from foo import bar as bar2"),
            ("foo.bar", "baz", None, "from foo.bar import baz"),
            ("foo.bar", "baz", "baz2", "from foo.bar import baz2"),
        ])
    )
    def test_main(self, *, case: tuple[str, str, str | None, str]) -> None:
        module, name, asname, expected = case
        imp = generate_from_import(module, name, asname=asname)
        result = Module([SimpleStatementLine([imp])]).code.strip("\n")
        assert result == expected


class TestGenerateImport:
    @given(
        case=sampled_from([
            ("foo", None, "import foo"),
            ("foo", "foo2", "import foo as foo2"),
            ("foo.bar", None, "import foo.bar"),
            ("foo.bar", "bar2", "import foo.bar as bar2"),
        ])
    )
    def test_main(self, *, case: tuple[str, str | None, str]) -> None:
        module, asname, expected = case
        imp = generate_import(module, asname=asname)
        result = Module([SimpleStatementLine([imp])]).code.strip("\n")
        assert result == expected
