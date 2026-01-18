from __future__ import annotations

from os import mkfifo
from pathlib import Path
from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import binary, dictionaries, integers, lists, text
from pytest import raises

from utilities.core import (
    TemporaryDirectory,
    TemporaryFile,
    _FileOrDirMissingError,
    _FileOrDirTypeError,
    always_iterable,
    file_or_dir,
    yield_temp_dir_at,
    yield_temp_file_at,
)

if TYPE_CHECKING:
    from collections.abc import Iterator


class TestAlwaysIterable:
    @given(x=binary())
    def test_bytes(self, *, x: bytes) -> None:
        assert list(always_iterable(x)) == [x]

    @given(x=dictionaries(text(), integers()))
    def test_dict(self, *, x: dict[str, int]) -> None:
        assert list(always_iterable(x)) == list(x)

    @given(x=integers())
    def test_integer(self, *, x: int) -> None:
        assert list(always_iterable(x)) == [x]

    @given(x=lists(binary()))
    def test_list_of_bytes(self, *, x: list[bytes]) -> None:
        assert list(always_iterable(x)) == x

    @given(x=text())
    def test_string(self, *, x: str) -> None:
        assert list(always_iterable(x)) == [x]

    @given(x=lists(integers()))
    def test_list_of_integers(self, *, x: list[int]) -> None:
        assert list(always_iterable(x)) == x

    @given(x=lists(text()))
    def test_list_of_strings(self, *, x: list[str]) -> None:
        assert list(always_iterable(x)) == x

    def test_generator(self) -> None:
        def yield_ints() -> Iterator[int]:
            yield 0
            yield 1

        assert list(always_iterable(yield_ints())) == [0, 1]


class TestFileOrDir:
    def test_file(self, *, tmp_path: Path) -> None:
        path = tmp_path / "file.txt"
        path.touch()
        result = file_or_dir(path)
        assert result == "file"

    def test_dir(self, *, tmp_path: Path) -> None:
        path = tmp_path / "dir"
        path.mkdir()
        result = file_or_dir(path)
        assert result == "dir"

    def test_empty(self, *, tmp_path: Path) -> None:
        path = tmp_path / "non-existent"
        result = file_or_dir(path)
        assert result is None

    def test_error_missing(self, *, tmp_path: Path) -> None:
        path = tmp_path / "non-existent"
        with raises(_FileOrDirMissingError, match=r"Path does not exist: '.*'"):
            _ = file_or_dir(path, exists=True)

    def test_error_type(self, *, tmp_path: Path) -> None:
        path = tmp_path / "fifo"
        mkfifo(path)
        with raises(
            _FileOrDirTypeError, match=r"Path is neither a file nor a directory: '.*'"
        ):
            _ = file_or_dir(path)


class TestTemporaryDirectory:
    def test_main(self) -> None:
        temp_dir = TemporaryDirectory()
        path = temp_dir.path
        assert isinstance(path, Path)
        assert path.is_dir()
        assert set(path.iterdir()) == set()

    def test_context_manager(self) -> None:
        with TemporaryDirectory() as temp:
            assert isinstance(temp, Path)
            assert temp.is_dir()
            assert set(temp.iterdir()) == set()
        assert not temp.exists()

    def test_suffix(self) -> None:
        with TemporaryDirectory(suffix="suffix") as temp:
            assert temp.name.endswith("suffix")

    def test_prefix(self) -> None:
        with TemporaryDirectory(prefix="prefix") as temp:
            assert temp.name.startswith("prefix")

    def test_dir(self, *, tmp_path: Path) -> None:
        with TemporaryDirectory(dir=tmp_path) as temp:
            relative = temp.relative_to(tmp_path)
        assert len(relative.parts) == 1


class TestTemporaryFile:
    def test_main(self) -> None:
        with TemporaryFile() as temp:
            assert isinstance(temp, Path)
            assert temp.is_file()
            _ = temp.write_text("text")
            assert temp.read_text() == "text"
        assert not temp.exists()

    def test_dir(self, *, tmp_path: Path) -> None:
        with TemporaryFile(dir=tmp_path) as temp:
            relative = temp.relative_to(tmp_path)
        assert len(relative.parts) == 1

    def test_suffix(self) -> None:
        with TemporaryFile(suffix="suffix") as temp:
            assert temp.name.endswith("suffix")

    def test_dir_and_suffix(self, *, tmp_path: Path) -> None:
        with TemporaryFile(dir=tmp_path, suffix="suffix") as temp:
            assert temp.name.endswith("suffix")

    def test_prefix(self) -> None:
        with TemporaryFile(prefix="prefix") as temp:
            assert temp.name.startswith("prefix")

    def test_dir_and_prefix(self, *, tmp_path: Path) -> None:
        with TemporaryFile(dir=tmp_path, prefix="prefix") as temp:
            assert temp.name.startswith("prefix")

    def test_name(self) -> None:
        with TemporaryFile(name="name") as temp:
            assert temp.name == "name"

    def test_dir_and_name(self, *, tmp_path: Path) -> None:
        with TemporaryFile(dir=tmp_path, name="name") as temp:
            assert temp.name == "name"

    def test_data(self) -> None:
        data = b"data"
        with TemporaryFile(data=data) as temp:
            current = temp.read_bytes()
            assert current == data

    def test_text(self) -> None:
        text = "text"
        with TemporaryFile(text=text) as temp:
            current = temp.read_text()
            assert current == text


class TestYieldTempAt:
    def test_dir(self, *, temp_path_not_exist: Path) -> None:
        with yield_temp_dir_at(temp_path_not_exist) as temp:
            assert temp.is_dir()
            assert temp.parent == temp_path_not_exist.parent
            assert temp.name.startswith(temp_path_not_exist.name)

    def test_file(self, *, temp_path_not_exist: Path) -> None:
        with yield_temp_file_at(temp_path_not_exist) as temp:
            assert temp.is_file()
            assert temp.parent == temp_path_not_exist.parent
            assert temp.name.startswith(temp_path_not_exist.name)
