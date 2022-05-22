from pathlib import Path

from pytest import mark
from pytest import param
from pytest import raises
from typeguard import typechecked

from dycw_utilities.pathlib import PathLike


@typechecked
def _uses_pathlike(path: PathLike, /) -> PathLike:
    return path


class TestPathLike:
    @mark.parametrize("path", [param(Path.home()), param("~")])
    def test_main(self, path: PathLike) -> None:
        _ = _uses_pathlike(path)

    def test_error(self) -> None:
        with raises(
            TypeError,
            match='type of argument "path" must be one .*; got NoneType instead',
        ):
            _ = _uses_pathlike(None)  # type: ignore
