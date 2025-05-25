from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from subprocess import check_output

from libcst import (
    AsName,
    Attribute,
    BaseExpression,
    BaseSmallStatement,
    FormattedString,
    FormattedStringExpression,
    FormattedStringText,
    Import,
    ImportAlias,
    ImportFrom,
    ImportStar,
    Module,
    Name,
    SimpleStatementLine,
)


def generate_import(module: str, /, *, asname: str | None = None) -> Import:
    """Generate an `Import` object."""
    alias = ImportAlias(
        name=_split_dotted(module), asname=AsName(Name(asname)) if asname else None
    )
    return Import(names=[alias])


def generate_from_import(
    module: str, name: str, /, *, asname: str | None = None
) -> ImportFrom:
    """Generate an `ImportFrom` object."""
    alias = ImportAlias(
        name=Name(name), asname=AsName(Name(asname)) if asname else None
    )
    return ImportFrom(module=_split_dotted(module), names=[alias])


def _split_dotted(dotted: str, /) -> Name | Attribute:
    parts = dotted.split(".")
    node = Name(parts[0])
    for part in parts[1:]:
        node = Attribute(value=node, attr=Name(part))
    return node


@dataclass(kw_only=True, slots=True)
class _ParseImportOutput:
    module: str
    name: str | None = None


def parse_import(import_: Import | ImportFrom, /) -> _ParseImportOutput:
    """Parse an import."""
    match import_:
        case Import():
            attr_or_name = import_.names[0].name
            return _ParseImportOutput(module=_join_dotted(attr_or_name))
        case ImportFrom():
            if (attr_or_name := import_.module) is None:
                return _ParseImportOutput(module="")
            module = _join_dotted(attr_or_name)
            match import_.names:
                case Sequence() as imports:
                    first = imports[0]
                    match first.name:
                        case Name(name):
                            ...
                        case _:
                            raise TypeError(*[f"{first.name=}"])
                case ImportStar():
                    name = "*"
            return _ParseImportOutput(module=module, name=name)


def _join_dotted(name_or_attr: Name | Attribute, /) -> str:
    parts: Sequence[str] = []
    curr: BaseExpression | Name | Attribute = name_or_attr
    while True:
        match curr:
            case Name(value=value):
                parts.append(value)
                break
            case Attribute(value=value, attr=Name(value=attr_value)):
                parts.append(attr_value)
                curr = value
            case BaseExpression():
                break
    return ".".join(reversed(parts))


def fstring(var: str, suffix: str, /) -> FormattedString:
    return FormattedString([
        FormattedStringExpression(expression=Name(var)),
        FormattedStringText(suffix),
    ])


def render_base_small_statement(node: BaseSmallStatement, /) -> str:
    return Module([SimpleStatementLine([node])]).code


def render_module(source: str | Module, /) -> str:
    """Render a module."""
    match source:
        case str() as text:
            return check_output(["ruff", "format", "-"], input=text, text=True)
        case Module() as module:
            return render_module(module.code)
