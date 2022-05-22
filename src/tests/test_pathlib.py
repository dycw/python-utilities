from pathlib import Path

from pytest import raises
from typeguard import typechecked

from dycw_utilities.pathlib import PathLike


class TestPathLike:
    def test_main(self) -> None:
        @typechecked
        def func(path: PathLike, /) -> PathLike:
            return path

        _ = func(Path.home())
        _ = func("~")
        with raises(
            TypeError,
            match='type of argument "path" must be one .*; got NoneType instead',
        ):
            _ = func(None)  # type: ignore
