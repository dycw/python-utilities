from __future__ import annotations

from functools import partial
from operator import le, lt
from re import search
from typing import TYPE_CHECKING, Any

from hypothesis import given
from hypothesis.strategies import sampled_from
from pytest import raises

from tests.modules import (
    package_missing,
    package_with,
    package_without,
    standalone,
    with_imports,
)
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
    @given(case=sampled_from([("importlib", True), ("invalid", False)]))
    def test_main(self, *, case: tuple[str, int]) -> None:
        module, expected = case
        result = is_installed(module)
        assert result is expected


class TestYieldModules:
    @given(
        case=sampled_from([
            (standalone, False, 1),
            (standalone, True, 1),
            (package_without, False, 2),
            (package_without, True, 2),
            (package_with, False, 3),
            (package_with, True, 6),
        ])
    )
    def test_main(self, *, case: tuple[ModuleType, bool, int]) -> None:
        module, recursive, expected = case
        res = list(yield_modules(module, recursive=recursive))
        assert len(res) == expected

    def test_missing_ok(self) -> None:
        _ = list(
            yield_modules(
                package_missing, missing_ok=["missing_package"], recursive=True
            )
        )

    def test_error(self) -> None:
        with raises(ModuleNotFoundError, match="No module named 'missing_package'"):
            _ = list(yield_modules(package_missing, recursive=True))


class TestYieldModuleContents:
    @given(
        case1=sampled_from([
            (standalone, False, 1),
            (standalone, True, 1),
            (package_without, False, 2),
            (package_without, True, 2),
            (package_with, False, 2),
            (package_with, True, 5),
        ]),
        case2=sampled_from([
            (int, None, 3),
            (float, None, 3),
            ((int, float), None, 6),
            (type, None, 3),
            (int, partial(le, 0), 2),
            (int, partial(lt, 0), 1),
            (float, partial(le, 0), 2),
            (float, partial(lt, 0), 1),
        ]),
    )
    def test_main(
        self,
        *,
        case1: tuple[ModuleType, bool, int],
        case2: tuple[type[Any] | tuple[type[Any], ...], Callable[[Any], bool], int],
    ) -> None:
        module, recursive, factor = case1
        type_, predicate, expected = case2
        res = list(
            yield_module_contents(
                module, type=type_, recursive=recursive, predicate=predicate
            )
        )
        assert len(res) == (factor * expected)


class TestYieldModuleSubclasses:
    def predicate(self: Any, /) -> bool:
        return bool(search("1", get_class_name(self)))

    @given(
        case1=sampled_from([
            (standalone, False, 1),
            (standalone, True, 1),
            (package_without, False, 2),
            (package_without, True, 2),
            (package_with, False, 2),
            (package_with, True, 5),
        ]),
        case2=sampled_from([
            (int, None, 1),
            (int, predicate, 0),
            (float, None, 2),
            (float, predicate, 1),
        ]),
    )
    def test_main(
        self,
        *,
        case1: tuple[ModuleType, bool, int],
        case2: tuple[type[Any], Callable[[Any], bool], int],
    ) -> None:
        module, recursive, factor = case1
        type_, predicate, expected = case2
        it = yield_module_subclasses(
            module, type_, recursive=recursive, predicate=predicate
        )
        assert len(list(it)) == (factor * expected)

    @given(case=sampled_from([(True, 1), (False, 2)]))
    def test_is_module(self, *, case: tuple[bool, int]) -> None:
        is_module, expected = case
        it = yield_module_subclasses(with_imports, object, is_module=is_module)
        assert len(list(it)) == expected
