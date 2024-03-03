from __future__ import annotations

from ast import AST, Call, ImportFrom, Module, Name, With, alias, expr, parse
from collections import Counter
from enum import Enum, auto
from operator import itemgetter
from sys import stdout
from typing import TYPE_CHECKING, Any

import click
from ast_comments import Comment, unparse
from click import command, option
from loguru import logger
from typing_extensions import assert_never

from utilities.humps import snake_case
from utilities.iterables import one
from utilities.text import ensure_str, strip_and_dedent

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator
    from types import ModuleType


class Method(Enum):
    """An enumeration of the import generation methods."""

    direct = auto()
    module = auto()
    parse = auto()


def yield_imports(
    *, method: Method = Method.module, include_suppress: bool = True
) -> Iterator[ImportFrom]:
    match method:
        case Method.direct:
            return _yield_import_nodes_directly(click, [command, option])
        case Method.module:
            import utilities.iterables

            return _yield_import_nodes_from_module_all(utilities.iterables)
        case Method.parse:
            text = """
from itertools import (
    accumulate,  # noqa: F401
    chain,  # noqa: F401
    combinations,  # noqa: F401
)
            """
            return _yield_import_nodes_from_text(
                text, include_suppress=include_suppress
            )
        case _ as never:  # type: ignore[]
            assert_never(never)


@command()
def main() -> None:
    imports = list(yield_imports())

    logger.info("IPython imports")
    _ = stdout.write(_generate_ipython_imports(imports) + "\n")

    logger.info("Neovim snippets")
    neovim_template = 's("{key}", {{ t("{value}") }})'
    _ = stdout.write(_generate_snippets(imports, neovim_template) + "\n")

    logger.info("VSCode snippets")
    vscode_template = '"{key}": {{ "prefix": "{key}", "body": ["{value}", "$0"] }}'
    _ = stdout.write(_generate_snippets(imports, vscode_template) + "\n")


def _yield_import_nodes_directly(
    module: ModuleType, objs: Iterable[Any], /
) -> Iterator[ImportFrom]:
    mod_name = module.__name__
    for obj in objs:
        yield ImportFrom(module=mod_name, names=[alias(name=obj.__qualname__)])


def _yield_import_nodes_from_module_all(module: ModuleType, /) -> Iterator[ImportFrom]:
    for key in module.__all__:
        yield ImportFrom(module=module.__name__, names=[alias(name=key)])


def _yield_import_nodes_from_text(
    text: str, /, *, include_suppress: bool = False
) -> Iterator[ImportFrom]:
    text = strip_and_dedent(text)
    module = parse(text)
    yield from _yield_imports_from_nodes(module.body, include_suppress=include_suppress)


def _yield_imports_from_nodes(
    nodes: Iterable[AST], /, *, include_suppress: bool = False
) -> Iterator[ImportFrom]:
    for node in nodes:
        if isinstance(node, ImportFrom):
            is_suppress = _is_suppress_import(node)
            if (is_suppress and include_suppress) or not is_suppress:
                for ali in node.names:
                    yield ImportFrom(module=node.module, names=[ali])
        elif isinstance(node, With) and _is_suppress_context_manager(
            node
        ):  # pragma: no cover
            yield from _yield_imports_from_nodes(
                node.body, include_suppress=include_suppress
            )


def _is_suppress_import(node: ImportFrom, /) -> bool:
    return (node.module == "contextlib") and any(  # pragma: no cover
        ali.name == "suppress" for ali in node.names
    )


def _is_suppress_context_manager(node: With, /) -> bool:
    return any(  # pragma: no cover
        _is_suppress_context_manager_1(i.context_expr) for i in node.items
    )


def _is_suppress_context_manager_1(node: expr, /) -> bool:
    if not isinstance(node, Call):  # pragma: no cover
        return False  # pragma: no cover
    return isinstance(func := node.func, Name) and (func.id == "suppress")


def _generate_ipython_imports(imports: Iterable[ImportFrom], /) -> str:
    def yield_imports() -> Iterator[list[AST]]:
        for imp in imports:
            yield [imp, Comment(value="# noqa: F401", inline=True)]

    body = list(yield_imports())
    mod_ast = Module(body=body, type_ignores=[])
    return unparse(mod_ast)


def _generate_snippets(imports: Iterable[ImportFrom], template: str, /) -> str:
    items = ((_node_to_key(imp), _generate_snippet(imp, template)) for imp in imports)
    sorted_items = sorted(items, key=itemgetter(0))
    counts = Counter(k for k, _ in sorted_items)
    duplicated = {k for k, v in counts.items() if v >= 2}  # noqa: PLR2004
    if len(duplicated) >= 1:
        logger.warning(f"Duplicated keys: {duplicated}")
    snippets = (f"{s}," for _, s in sorted_items)
    return "".join(snippets)


def _generate_snippet(imp: ImportFrom, template: str, /) -> str:
    key = _node_to_key(imp)
    value = unparse(imp)
    return strip_and_dedent(template).format(key=key, value=value)


def _node_to_key(imp: ImportFrom, /) -> str:
    module = ensure_str(imp.module)
    alias = one(imp.names)
    value = snake_case(alias.name).replace("_", "-")
    return f"f{module[:2]}-{value}"
