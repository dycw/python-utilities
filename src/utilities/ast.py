from __future__ import annotations

from ast import Assign, AugAssign, Constant, List, Name, Try, expr, parse
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import Iterator


def yield_dunder_all(text: str, /) -> Iterator[list[str]]:
    """Yield all the `__all__` terms in a source file."""

    module = parse(text)
    for stmt in module.body:
        if isinstance(stmt, Assign):
            yield from _yield_from_assign(stmt)
        if isinstance(stmt, Try):
            yield from _yield_from_try(stmt)


def _yield_from_assign(assign: Assign, /) -> Iterator[list[str]]:
    """Yield the `__all__` terms from an `Assign`."""

    match assign.targets:
        case [target]:
            yield from _yield_from_target_and_value(target, assign.value)
        case _:
            pass


def _yield_from_try(try_: Try, /) -> Iterator[list[str]]:
    """Yield the `__all__` terms from the `else` block of a `Try`."""
    for stmt in try_.orelse:
        if isinstance(stmt, AugAssign):
            yield from _yield_from_target_and_value(stmt.target, stmt.value)


def _yield_from_target_and_value(target: expr, value: expr, /) -> Iterator[list[str]]:
    """Yield the `__all__` terms from a target and value."""
    if not (  # pragma: no cover
        isinstance(target, Name)
        and (target.id == "__all__")
        and isinstance(value, List)
    ):
        return
    elts = value.elts
    if not all(isinstance(c, Constant) for c in elts):  # pragma: no cover
        return
    constants = cast(list[Constant], elts)
    yield [c.value for c in constants]


__all__ = ["yield_dunder_all"]
