from collections.abc import Callable
from functools import partial
from operator import le
from operator import lt
from re import search
from types import ModuleType
from typing import Any
from typing import Optional
from typing import Union

from pytest import mark
from pytest import param

from tests.modules import package_with
from tests.modules import package_without
from tests.modules import standalone
from utilities.class_name import get_class_name
from utilities.modules import yield_module_contents
from utilities.modules import yield_module_subclasses
from utilities.modules import yield_modules


class TestYieldModules:
    @mark.parametrize(
        ("module", "recursive", "expected"),
        [
            param(standalone, False, 1),
            param(standalone, True, 1),
            param(package_without, False, 2),
            param(package_without, True, 2),
            param(package_with, False, 2),
            param(package_with, True, 5),
        ],
    )
    def test_main(
        self,
        module: ModuleType,
        recursive: bool,
        expected: int,
    ) -> None:
        assert len(list(yield_modules(module, recursive=recursive))) == expected


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
            param(None, None, 17),
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
        module: ModuleType,
        type_: Optional[Union[type[Any], tuple[type[Any], ...]]],
        recursive: bool,
        predicate: Callable[[Any], bool],
        expected: int,
        factor: int,
    ) -> None:
        it = yield_module_contents(
            module,
            type=type_,
            recursive=recursive,
            predicate=predicate,
        )
        assert len(list(it)) == (factor * expected)


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
        module: ModuleType,
        type_: type[Any],
        recursive: bool,
        predicate: Callable[[type[Any]], bool],
        expected: int,
        factor: int,
    ) -> None:
        it = yield_module_subclasses(
            module,
            type_,
            recursive=recursive,
            predicate=predicate,
        )
        assert len(list(it)) == (factor * expected)
