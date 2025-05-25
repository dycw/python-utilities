from __future__ import annotations

from libcst import SimpleStatementLine

from utilities.libcst import generate_from_import


class TestGenerateFromImport:
    def test_main(self) -> None:
        imp = generate_from_import("foo", "bar")
        result = SimpleStatementLine([imp])
        expected = "from foo import bar"
        assert result == expected
