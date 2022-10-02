from pathlib import Path

from hypothesis import given
from hypothesis.strategies import sets

from utilities.hypothesis import text_ascii
from utilities.hypothesis.tempfile import temp_dirs
from utilities.hypothesis.tempfile import temp_paths
from utilities.tempfile import TemporaryDirectory


class TestTempDirs:
    @given(temp_dir=temp_dirs())
    def test_main(self, temp_dir: TemporaryDirectory) -> None:
        _test_temp_path(temp_dir.name)

    @given(
        temp_dir=temp_dirs(), contents=sets(text_ascii(min_size=1), max_size=10)
    )
    def test_writing_files(
        self, temp_dir: TemporaryDirectory, contents: set[str]
    ) -> None:
        _test_writing_to_temp_path(temp_dir.name, contents)


class TestTempPaths:
    @given(temp_path=temp_paths())
    def test_main(self, temp_path: Path) -> None:
        _test_temp_path(temp_path)

    @given(
        temp_path=temp_paths(),
        contents=sets(text_ascii(min_size=1), max_size=10),
    )
    def test_writing_files(self, temp_path: Path, contents: set[str]) -> None:
        _test_writing_to_temp_path(temp_path, contents)


def _test_temp_path(path: Path, /) -> None:
    assert path.is_dir()
    assert len(set(path.iterdir())) == 0


def _test_writing_to_temp_path(path: Path, contents: set[str], /) -> None:
    assert len(set(path.iterdir())) == 0
    for content in contents:
        path.joinpath(content).touch()
    assert len(set(path.iterdir())) == len(contents)
