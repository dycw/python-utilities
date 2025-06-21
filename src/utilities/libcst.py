from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from subprocess import CalledProcessError, check_output
from typing import assert_never, override

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

from utilities.errors import ImpossibleCaseError


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


def generate_import_from(
    module: str, name: str, /, *, asname: str | None = None
) -> ImportFrom:
    """Generate an `ImportFrom` object."""
    match name, asname:
        case "*", None:
            names = ImportStar()
        case "*", str():
            raise GenerateImportFromError(module=module, asname=asname)
        case _, None:
            alias = ImportAlias(name=Name(name))
            names = [alias]
        case _, str():
            alias = ImportAlias(name=Name(name), asname=AsName(Name(asname)))
            names = [alias]
        case _ as never:
            assert_never(never)
    return ImportFrom(module=split_dotted_str(module), names=names)


@dataclass(kw_only=True, slots=True)
class GenerateImportFromError(Exception):
    module: str
    asname: str | None = None

    @override
    def __str__(self) -> str:
        return f"Invalid import: 'from {self.module} import * as {self.asname}'"


##


@dataclass(kw_only=True, slots=True)
class _ParseImportOutput:
    module: str
    name: str | None = None


def parse_import(import_: Import | ImportFrom, /) -> Sequence[_ParseImportOutput]:
    """Parse an import."""
    match import_:
        case Import():
            return [_parse_import_one(n) for n in import_.names]
        case ImportFrom():
            if (attr_or_name := import_.module) is None:
                raise _ParseImportEmptyModuleError(import_=import_)
            module = join_dotted_str(attr_or_name)
            match import_.names:
                case Sequence() as names:
                    return [_parse_import_from_one(module, n) for n in names]
                case ImportStar():
                    return [_ParseImportOutput(module=module, name="*")]
                case _ as never:
                    assert_never(never)
        case _ as never:
            assert_never(never)


def _parse_import_one(alias: ImportAlias, /) -> _ParseImportOutput:
    return _ParseImportOutput(module=join_dotted_str(alias.name))


def _parse_import_from_one(module: str, alias: ImportAlias, /) -> _ParseImportOutput:
    match alias.name:
        case Name(name):
            return _ParseImportOutput(module=module, name=name)
        case Attribute() as attr:
            raise _ParseImportAliasError(module=module, attr=attr)
        case _ as never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class ParseImportError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _ParseImportEmptyModuleError(ParseImportError):
    import_: ImportFrom

    @override
    def __str__(self) -> str:
        return f"Module must not be None; got {self.import_}"


@dataclass(kw_only=True, slots=True)
class _ParseImportAliasError(ParseImportError):
    module: str
    attr: Attribute

    @override
    def __str__(self) -> str:
        attr = self.attr
        return f"Invalid alias name; got module {self.module!r} and attribute '{attr.value}.{attr.attr}'"


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
            case BaseExpression():  # pragma: no cover
                raise ImpossibleCaseError(case=[f"{curr=}"])
            case _ as never:
                assert_never(never)
    return ".".join(reversed(parts))


##


def render_module(source: str | Module, /) -> str:
    """Render a module."""
    match source:  # skipif-ci
        case str() as text:
            try:
                return check_output(["ruff", "format", "-"], input=text, text=True)
            except CalledProcessError:  # pragma: no cover
                return text
        case Module() as module:
            return render_module(module.code)
        case _ as never:
            assert_never(never)


##


__all__ = [
    "GenerateImportFromError",
    "ParseImportError",
    "generate_f_string",
    "generate_import",
    "generate_import_from",
    "join_dotted_str",
    "parse_import",
    "render_module",
    "split_dotted_str",
]
