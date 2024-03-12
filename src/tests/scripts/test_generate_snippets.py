from __future__ import annotations

import abc
from abc import ABC, ABCMeta
from ast import ImportFrom, alias

import pytest
from click.testing import CliRunner

from utilities.iterables import one
from utilities.scripts.generate_snippets import (
    Method,
    _generate_ipython_imports,
    _generate_snippet,
    _generate_snippets,
    _node_to_key,
    _yield_import_nodes_directly,
    _yield_import_nodes_from_module_all,
    _yield_import_nodes_from_text,
    main,
    yield_imports,
)


class TestCLI:
    def test_main(self) -> None:
        runner = CliRunner()
        result = runner.invoke(main)
        assert result.exit_code == 0


class TestGenerateIPythonImports:
    def test_main(self) -> None:
        imp = ImportFrom(module="abc", names=[alias(name="ABC")])
        result = _generate_ipython_imports([imp])
        expected = "from abc import ABC  # noqa: F401"
        assert result == expected


class TestGenerateSnippet:
    def test_main(self) -> None:
        imp = ImportFrom(module="abc", names=[alias(name="ABC")])
        template = "{key}: {value}"
        result = _generate_snippet(imp, template)
        expected = "fab-abc: from abc import ABC"
        assert result == expected

    def test_as(self) -> None:
        imp = ImportFrom(
            module="collections.abc", names=[alias(name="Set", asname="AbstractSet")]
        )
        template = "{key}: {value}"
        result = _generate_snippet(imp, template)
        expected = "fco-set: from collections.abc import Set as AbstractSet"
        assert result == expected


class TestGenerateSnippets:
    def test_main(self) -> None:
        imports = {
            ImportFrom(module="abc", names=[alias(name=name)])
            for name in ["ABC", "ABCMeta"]
        }
        template = "{key}: {value}"
        result = _generate_snippets(imports, template)
        expected = "fab-abc: from abc import ABC,fab-abc-meta: from abc import ABCMeta,"
        assert result == expected

    def test_duplicated_keys(self, *, caplog: pytest.LogCaptureFixture) -> None:
        imports = {
            ImportFrom(module="dataclasses", names=[alias(name=name)])
            for name in ["field", "Field"]
        }
        template = "{key}: {value}"
        _ = _generate_snippets(imports, template)
        (record,) = caplog.records
        assert record.message == "Duplicated keys: {'fda-field'}"


class TestNodeToKey:
    @pytest.mark.parametrize(
        ("module", "name", "expected"),
        [
            pytest.param("abc", "ABC", "fab-abc"),
            pytest.param("abc", "ABCMeta", "fab-abc-meta"),
        ],
    )
    def test_main(self, *, module: str, name: str, expected: str) -> None:
        node = ImportFrom(module=module, names=[alias(name=name)])
        key = _node_to_key(node)
        assert key == expected


class TestYieldImportNodesDirectly:
    def test_main(self) -> None:
        imp1, imp2 = _yield_import_nodes_directly(abc, [ABC, ABCMeta])
        assert imp1.module == "abc"
        assert one(imp1.names).name == "ABC"
        assert imp2.module == "abc"
        assert one(imp2.names).name == "ABCMeta"


class TestYieldImportNodesFromModuleAll:
    def test_main(self) -> None:
        imports = list(_yield_import_nodes_from_module_all(pytest))
        assert len(imports) == 80
        for imp in imports:
            assert imp.module == "pytest"


class TestYieldImportNodesFromText:
    def test_single(self) -> None:
        text = """
            from abc import ABC
        """
        imp = one(_yield_import_nodes_from_text(text))
        assert imp.module == "abc"
        assert one(imp.names).name == "ABC"

    @pytest.mark.parametrize(
        "text",
        [
            pytest.param(
                """
                from abc import ABC
                from abc import ABCMeta
                """
            ),
            pytest.param(
                """
                from abc import (
                    ABC,
                    ABCMeta,
                )
                """
            ),
        ],
    )
    def test_two(self, *, text: str) -> None:
        imp1, imp2 = _yield_import_nodes_from_text(text)
        assert imp1.module == "abc"
        assert one(imp1.names).name == "ABC"
        assert imp2.module == "abc"
        assert one(imp2.names).name == "ABCMeta"

    def test_suppress_include(self) -> None:
        text = """
            from contextlib import suppress

            with suppress(ModuleNotFoundError):
                from abc import ABC
        """
        (imp1, imp2) = _yield_import_nodes_from_text(text, include_suppress=True)
        assert imp1.module == "contextlib"
        assert one(imp1.names).name == "suppress"
        assert imp2.module == "abc"
        assert one(imp2.names).name == "ABC"

    def test_suppress_do_not_include(self) -> None:
        text = """
            from contextlib import suppress

            with suppress(ModuleNotFoundError):
                from abc import ABC
        """
        (imp,) = _yield_import_nodes_from_text(text, include_suppress=False)
        assert imp.module == "abc"
        assert one(imp.names).name == "ABC"

    @pytest.mark.parametrize(
        "text",
        [
            pytest.param(
                """
                from contextlib import suppress

                with suppress(ModuleNotFoundError):
                    from abc import ABC
                    with suppress(ModuleNotFoundError):
                        from abc import ABCMeta
                """
            ),
            pytest.param(
                """
                from contextlib import suppress

                with suppress(ModuleNotFoundError):
                    with suppress(ModuleNotFoundError):
                        from abc import ABC
                    from abc import ABCMeta
                """
            ),
        ],
    )
    def test_nested_suppress(self, *, text: str) -> None:
        imp1, imp2 = _yield_import_nodes_from_text(text)
        assert imp1.module == "abc"
        assert one(imp1.names).name == "ABC"
        assert imp2.module == "abc"
        assert one(imp2.names).name == "ABCMeta"


class TestYieldImports:
    @pytest.mark.parametrize(
        "method", [pytest.param(Method.direct), pytest.param(Method.parse)]
    )
    def test_yield_imports(self, *, method: Method) -> None:
        for imp in yield_imports(method=method):
            assert isinstance(imp, ImportFrom)
