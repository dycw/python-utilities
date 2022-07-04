from pathlib import Path

from pytest import mark
from pytest import param
from pytest import raises
from typeguard import typechecked

from utilities.pathlib import PathLike
from utilities.pathlib import ensure_suffix


class TestEnsureSuffix:
    @mark.parametrize(
        ["path", "expected"],
        [
            param("hello.txt", "hello.txt"),
            param("hello.1.txt", "hello.1.txt"),
            param("hello.1.2.txt", "hello.1.2.txt"),
            param("hello.jpg", "hello.jpg.txt"),
            param("hello.1.jpg", "hello.1.jpg.txt"),
            param("hello.1.2.jpg", "hello.1.2.jpg.txt"),
            param("hello.txt.jpg", "hello.txt.jpg.txt"),
            param("hello.txt.1.jpg", "hello.txt.1.jpg.txt"),
            param("hello.txt.1.2.jpg", "hello.txt.1.2.jpg.txt"),
        ],
    )
    def test_main(self, path: Path, expected: Path) -> None:
        result = ensure_suffix(path, ".txt")
        assert result == Path(expected)


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
