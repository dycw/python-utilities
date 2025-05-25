from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from subprocess import check_output

from libcst import (
    AsName,
    Attribute,
    BaseExpression,
    FormattedString,
    FormattedStringExpression,
    FormattedStringText,
    Import,
    ImportAlias,
    ImportFrom,
    ImportStar,
    Module,
    Name,
)


def generate_from_import(
    module: str, name: str, /, *, asname: str | None = None
) -> ImportFrom:
    """Generate an `ImportFrom` object."""
    alias = ImportAlias(
        name=Name(name), asname=AsName(Name(asname)) if asname else None
    )
    return ImportFrom(module=split_dotted_str(module), names=[alias])


def generate_f_string(var: str, suffix: str, /) -> FormattedString:
    """Generate an f-string."""
    return FormattedString([
        FormattedStringExpression(expression=Name(var)),
        FormattedStringText(suffix),
    ])


def generate_import(module: str, /, *, asname: str | None = None) -> Import:
    """Generate an `Import` object."""
    alias = ImportAlias(
        name=split_dotted_str(module), asname=AsName(Name(asname)) if asname else None
    )
    return Import(names=[alias])


##


@dataclass(kw_only=True, slots=True)
class _ParseImportOutput:
    module: str
    name: str | None = None


def parse_import(import_: Import | ImportFrom, /) -> _ParseImportOutput:
    """Parse an import."""
    match import_:
        case Import():
            attr_or_name = import_.names[0].name
            return _ParseImportOutput(module=join_dotted_str(attr_or_name))
        case ImportFrom():
            if (attr_or_name := import_.module) is None:
                return _ParseImportOutput(module="")
            module = join_dotted_str(attr_or_name)
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


##


def split_dotted_str(dotted: str, /) -> Name | Attribute:
    """Split a dotted string into a name/attribute."""
    parts = dotted.split(".")
    node = Name(parts[0])
    for part in parts[1:]:
        node = Attribute(value=node, attr=Name(part))
    return node


def join_dotted_str(name_or_attr: Name | Attribute, /) -> str:
    """Join a dotted from from a name/attribute."""
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


##


def render_module(source: str | Module, /) -> str:
    """Render a module."""
    match source:
        case str() as text:
            return check_output(["ruff", "format", "-"], input=text, text=True)
        case Module() as module:
            return render_module(module.code)


##


__all__ = [
    "generate_f_string",
    "generate_from_import",
    "generate_import",
    "render_module",
]
