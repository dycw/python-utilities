from types import ModuleType

from pytest import mark
from pytest import param

from dycw_utilities.modules import yield_modules
from tests.modules import package_with
from tests.modules import package_without
from tests.modules import standalone


class TestYieldModules:
    @mark.parametrize(
        "module, recursive, expected",
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
        self, module: ModuleType, recursive: bool, expected: int
    ) -> None:
        assert len(list(yield_modules(module, recursive=recursive))) == expected
