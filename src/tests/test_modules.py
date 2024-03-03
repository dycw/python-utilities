from __future__ import annotations

from functools import partial
from operator import le, lt
from re import search
from typing import TYPE_CHECKING, Any

import pytest

from tests.modules import package_with, package_without, standalone
from utilities.modules import (
    yield_module_contents,
    yield_module_subclasses,
    yield_modules,
)
from utilities.types import get_class_name

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import ModuleType


class TestYieldModules:
    @pytest.mark.parametrize(
        ("module", "recursive", "expected"),
        [
            pytest.param(standalone, False, 1),
            pytest.param(standalone, True, 1),
            pytest.param(package_without, False, 2),
            pytest.param(package_without, True, 2),
            pytest.param(package_with, False, 3),
            pytest.param(package_with, True, 6),
        ],
    )
    def test_main(self, *, module: ModuleType, recursive: bool, expected: int) -> None:
        res = list(yield_modules(module, recursive=recursive))
        assert len(res) == expected


class TestYieldModuleContents:
    @pytest.mark.parametrize(
        ("module", "recursive", "factor"),
        [
            pytest.param(standalone, False, 1),
            pytest.param(standalone, True, 1),
            pytest.param(package_without, False, 2),
            pytest.param(package_without, True, 2),
            pytest.param(package_with, False, 2),
            pytest.param(package_with, True, 5),
        ],
    )
    @pytest.mark.parametrize(
        ("type_", "predicate", "expected"),
        [
            pytest.param(int, None, 3),
            pytest.param(float, None, 3),
            pytest.param((int, float), None, 6),
            pytest.param(type, None, 3),
            pytest.param(int, partial(le, 0), 2),
            pytest.param(int, partial(lt, 0), 1),
            pytest.param(float, partial(le, 0), 2),
            pytest.param(float, partial(lt, 0), 1),
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

    @pytest.mark.parametrize(
        ("module", "recursive", "factor"),
        [
            pytest.param(standalone, False, 1),
            pytest.param(standalone, True, 1),
            pytest.param(package_without, False, 2),
            pytest.param(package_without, True, 2),
            pytest.param(package_with, False, 2),
            pytest.param(package_with, True, 5),
        ],
    )
    @pytest.mark.parametrize(
        ("type_", "predicate", "expected"),
        [
            pytest.param(int, None, 1),
            pytest.param(int, predicate, 0),
            pytest.param(float, None, 2),
            pytest.param(float, predicate, 1),
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
