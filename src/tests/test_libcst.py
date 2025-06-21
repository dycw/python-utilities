from __future__ import annotations

import re
from re import DOTALL

from hypothesis import given
from hypothesis.strategies import sampled_from
from libcst import (
    Attribute,
    Dot,
    Expr,
    Import,
    ImportAlias,
    ImportFrom,
    ImportStar,
    Module,
    Name,
    SimpleStatementLine,
)
from pytest import raises

from tests.conftest import SKIPIF_CI
from utilities.iterables import one
from utilities.libcst import (
    GenerateImportFromError,
    _ParseImportAliasError,
    _ParseImportEmptyModuleError,
    generate_f_string,
    generate_import,
    generate_import_from,
    join_dotted_str,
    parse_import,
    render_module,
    split_dotted_str,
)


class TestGenerateFString:
    def test_main(self) -> None:
        string = generate_f_string("foo", "bar")
        result = Module([SimpleStatementLine([Expr(string)])]).code.strip("\n")
        expected = 'f"{foo}bar"'
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


class TestGenerateImportFrom:
    @given(
        case=sampled_from([
            ("foo", "bar", None, "from foo import bar"),
            ("foo", "bar", "bar2", "from foo import bar as bar2"),
            ("foo", "*", None, "from foo import *"),
            ("foo.bar", "baz", None, "from foo.bar import baz"),
            ("foo.bar", "baz", "baz2", "from foo.bar import baz as baz2"),
            ("foo.bar", "*", None, "from foo.bar import *"),
        ])
    )
    def test_main(self, *, case: tuple[str, str, str | None, str]) -> None:
        module, name, asname, expected = case
        imp = generate_import_from(module, name, asname=asname)
        result = Module([SimpleStatementLine([imp])]).code.strip("\n")
        assert result == expected

    def test_error(self) -> None:
        with raises(
            GenerateImportFromError,
            match=r"Invalid import: 'from foo import \* as bar'",
        ):
            _ = generate_import_from("foo", "*", asname="bar")


class TestParseImport:
    def test_import_one_name(self) -> None:
        imp = Import(names=[ImportAlias(Name("foo"))])
        parsed = one(parse_import(imp))
        assert parsed.module == "foo"
        assert parsed.name is None

    def test_import_one_attr(self) -> None:
        imp = Import(names=[ImportAlias(Attribute(Name("foo"), Name("bar")))])
        parsed = one(parse_import(imp))
        assert parsed.module == "foo.bar"
        assert parsed.name is None

    def test_import_many(self) -> None:
        imp = Import(names=[ImportAlias(Name("foo")), ImportAlias(Name("bar"))])
        result = parse_import(imp)
        assert len(result) == 2
        first, second = result
        assert first.module == "foo"
        assert first.name is None
        assert second.module == "bar"
        assert second.name is None

    def test_from_import_one(self) -> None:
        imp = ImportFrom(module=Name("foo"), names=[ImportAlias(Name("bar"))])
        result = one(parse_import(imp))
        assert result.module == "foo"
        assert result.name == "bar"

    def test_from_import_many(self) -> None:
        imp = ImportFrom(
            module=Name("foo"),
            names=[ImportAlias(Name("bar")), ImportAlias(Name("baz"))],
        )
        result = parse_import(imp)
        first, second = result
        assert first.module == "foo"
        assert first.name == "bar"
        assert second.module == "foo"
        assert second.name == "baz"

    def test_from_import_star(self) -> None:
        imp = ImportFrom(module=Name("foo"), names=ImportStar())
        result = one(parse_import(imp))
        assert result.module == "foo"
        assert result.name == "*"

    def test_error_empty_module(self) -> None:
        alias = ImportAlias(name=Name("foo"))
        imp = ImportFrom(module=None, names=[alias], relative=[Dot()])
        with raises(
            _ParseImportEmptyModuleError, match="Module must not be None; got .*"
        ):
            _ = parse_import(imp)

    def test_error_alias(self) -> None:
        alias = ImportAlias(name=Attribute(Name("foo"), Name("bar")))
        imp = ImportFrom(module=Name("baz"), names=[alias])
        with raises(
            _ParseImportAliasError,
            match=re.compile(
                r"Invalid alias name; got module 'baz' and attribute 'Name\(.*\)'",
                flags=DOTALL,
            ),
        ):
            _ = parse_import(imp)


class TestRenderModule:
    @SKIPIF_CI
    def test_main(self) -> None:
        module = Module([SimpleStatementLine([generate_import("foo")])])
        result = render_module(module)
        expected = "import foo\n"
        assert result == expected


class TestSplitAndJoinDottedStr:
    @given(text=sampled_from(["foo", "foo.bar", "foo.bar.baz"]))
    def test_main(self, *, text: str) -> None:
        result = join_dotted_str(split_dotted_str(text))
        assert result == text
