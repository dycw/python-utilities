from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from pytest import fixture
from pytest_regressions.file_regression import FileRegressionFixture

from utilities.git import get_repo_root
from utilities.pytest import node_id_to_path

if TYPE_CHECKING:
    from pytest import FixtureRequest

    from utilities.types import PathLike


_PATH_TESTS = Path("src", "tests")


class OrjsonRegressionFixture:
    """Implementation of `orjson_regression` fixture."""

    def __init__(
        self,
        *,
        path_tests: PathLike | None = None,
        node_id_path: PathLike | None = None,
        request: FixtureRequest,
    ) -> None:
        super().__init__()
        path_tests_use = _get_path_tests() if path_tests is None else Path(path_tests)
        if node_id_path is not None:
            path_use = path_tests_use.joinpath(node_id_path)
        self._file_regression = FileRegressionFixture(
            datadir=path_use, original_datadir=path_use, request=request
        )

    def check(
        self,
        obj: Any,
        /,
        *,
        request_and_suffix: tuple[FixtureRequest, str] | None = None,
    ) -> None:
        """Serialize the object and compare it to a previously saved baseline."""
        from utilities.orjson import serialize

        data = serialize(obj)
        if request_and_suffix is None:
            basename = None
        else:
            request, suffix = request_and_suffix
            basename = f"{request.node.name}_{suffix}"
        self._file_regression.check(
            data, extension=".json", basename=basename, binary=True
        )


@fixture
def orjson_regression_fixture(*, request: FixtureRequest) -> OrjsonRegressionFixture:
    """Fixture to provide an instance of ObjectRegressionFixture using path_regression."""
    path_tests = _get_path_tests()
    node_id_path = node_id_to_path(request.node.nodeid, ".json", head=_PATH_TESTS)
    return OrjsonRegressionFixture(
        path_tests=path_tests, node_id_path=node_id_path, request=request
    )


def _get_path_tests() -> Path:
    """Get the path to the tests folder."""
    return get_repo_root().joinpath(_PATH_TESTS)
