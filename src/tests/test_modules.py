from __future__ import annotations

from collections.abc import Callable
from functools import partial
from inspect import getsource
from operator import le, lt
from re import search
from types import ModuleType
from typing import Any

from pytest import mark, param

import utilities
from tests.modules import package_with, package_without, standalone
from utilities.ast import yield_dunder_all
from utilities.iterables import check_duplicates
from utilities.modules import (
    yield_module_contents,
    yield_module_subclasses,
    yield_modules,
)
from utilities.types import get_class_name


class TestYieldModules:
    @mark.parametrize(
        ("module", "recursive", "expected"),
        [
            param(standalone, False, 1),
            param(standalone, True, 1),
            param(package_without, False, 2),
            param(package_without, True, 2),
            param(package_with, False, 3),
            param(package_with, True, 6),
        ],
    )
    def test_main(self, *, module: ModuleType, recursive: bool, expected: int) -> None:
        res = list(yield_modules(module, recursive=recursive))
        assert len(res) == expected

    def test_all(self) -> None:
        for module in yield_modules(utilities, recursive=True):
            source = getsource(module)
            for dunder_all in yield_dunder_all(source):
                check_duplicates(dunder_all)
                expected = sorted(dunder_all)
                msg = (
                    f"Please paste in\n\t{module.__name__}\nthe following:\n\n\n"
                    f"\t__all__ = {expected}\n\n"
                )
                assert dunder_all == expected, msg


class TestYieldModuleContents:
    @mark.parametrize(
        ("module", "recursive", "factor"),
        [
            param(standalone, False, 1),
            param(standalone, True, 1),
            param(package_without, False, 2),
            param(package_without, True, 2),
            param(package_with, False, 2),
            param(package_with, True, 5),
        ],
    )
    @mark.parametrize(
        ("type_", "predicate", "expected"),
        [
            param(int, None, 3),
            param(float, None, 3),
            param((int, float), None, 6),
            param(type, None, 3),
            param(int, partial(le, 0), 2),
            param(int, partial(lt, 0), 1),
            param(float, partial(le, 0), 2),
            param(float, partial(lt, 0), 1),
        ],
    )
    def test_main(
        self,
        *,
        module: ModuleType,
        type_: type[Any] | tuple[type[Any], ...] | None,
        recursive: bool,
        predicate: Callable[[Any], bool],
        expected: int,
        factor: int,
    ) -> None:
        res = list(
            yield_module_contents(
                module, type=type_, recursive=recursive, predicate=predicate
            )
        )
        assert len(res) == (factor * expected)


class TestYieldModuleSubclasses:
    def predicate(self: Any, /) -> bool:
        return bool(search("1", get_class_name(self)))

    @mark.parametrize(
        ("module", "recursive", "factor"),
        [
            param(standalone, False, 1),
            param(standalone, True, 1),
            param(package_without, False, 2),
            param(package_without, True, 2),
            param(package_with, False, 2),
            param(package_with, True, 5),
        ],
    )
    @mark.parametrize(
        ("type_", "predicate", "expected"),
        [
            param(int, None, 1),
            param(int, predicate, 0),
            param(float, None, 2),
            param(float, predicate, 1),
        ],
    )
    def test_main(
        self,
        *,
        module: ModuleType,
        type_: type[Any],
        recursive: bool,
        predicate: Callable[[type[Any]], bool],
        expected: int,
        factor: int,
    ) -> None:
        it = yield_module_subclasses(
            module, type_, recursive=recursive, predicate=predicate
        )
        assert len(list(it)) == (factor * expected)
