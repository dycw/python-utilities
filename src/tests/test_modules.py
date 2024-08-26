from __future__ import annotations

from functools import partial
from operator import le, lt
from re import search
from typing import TYPE_CHECKING, Any

from pytest import mark, param

from tests.modules import package_with, package_without, standalone
from utilities.functions import get_class_name
from utilities.modules import (
    is_installed,
    yield_module_contents,
    yield_module_subclasses,
    yield_modules,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import ModuleType


class TestIsInstalled:
    @mark.parametrize(
        ("module", "expected"), [param("importlib", True), param("invalid", False)]
    )
    def test_main(self, *, module: str, expected: int) -> None:
        result = is_installed(module)
        assert result is expected


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
