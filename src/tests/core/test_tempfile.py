from __future__ import annotations

from pathlib import Path

from pytest import mark, param
from pytest_lazy_fixtures import lf

from utilities.constants import EFFECTIVE_GROUP_NAME, EFFECTIVE_USER_NAME, TEMP_DIR
from utilities.core import (
    Permissions,
    TemporaryDirectory,
    TemporaryFile,
    get_file_group,
    get_file_owner,
    yield_adjacent_temp_dir,
    yield_adjacent_temp_file,
)


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

    @mark.parametrize(
        "dir_", [param(lf("tmp_path")), param(lf("temp_path_nested_not_exist"))]
    )
    def test_dir(self, *, dir_: Path) -> None:
        with TemporaryDirectory(dir=dir_) as temp:
            relative = temp.relative_to(dir_)
        assert len(relative.parts) == 1


class TestTemporaryFile:
    def test_main(self) -> None:
        with TemporaryFile() as temp:
            assert isinstance(temp, Path)
            assert temp.is_file()
            relative = temp.relative_to(TEMP_DIR)
            _ = temp.write_text("text")
            assert temp.read_text() == "text"
        assert len(relative.parts) == 1
        assert not temp.exists()

    @mark.parametrize(
        "dir_", [param(lf("tmp_path")), param(lf("temp_path_nested_not_exist"))]
    )
    def test_dir(self, *, dir_: Path) -> None:
        with TemporaryFile(dir=dir_) as temp:
            relative = temp.relative_to(dir_)
        assert len(relative.parts) == 1

    def test_suffix(self) -> None:
        with TemporaryFile(suffix="suffix") as temp:
            assert temp.name.endswith("suffix")

    def test_prefix(self) -> None:
        with TemporaryFile(prefix="prefix") as temp:
            assert temp.name.startswith("prefix")

    def test_name(self) -> None:
        with TemporaryFile(name="name") as temp:
            assert temp.name == "name"

    def test_delete(self, *, tmp_path: Path) -> None:
        with TemporaryFile(dir=tmp_path, delete=False) as temp:
            assert temp.is_file()
        assert temp.is_file()

    def test_data(self) -> None:
        with TemporaryFile(data=b"data") as temp:
            assert temp.read_bytes() == b"data"

    def test_text(self) -> None:
        with TemporaryFile(text="text") as temp:
            assert temp.read_text() == "text\n"

    def test_perms(self) -> None:
        perms = Permissions.from_text("u=rw,g=r,o=r")
        with TemporaryFile(perms=perms) as temp:
            assert Permissions.from_path(temp) == perms

    def test_owner(self) -> None:
        with TemporaryFile(owner=EFFECTIVE_USER_NAME) as temp:
            assert get_file_owner(temp) == EFFECTIVE_USER_NAME

    def test_group(self) -> None:
        with TemporaryFile(group=EFFECTIVE_GROUP_NAME) as temp:
            assert get_file_group(temp) == EFFECTIVE_GROUP_NAME


class TestYieldAdjacentTempDir:
    def test_main(self, *, tmp_path: Path) -> None:
        with yield_adjacent_temp_dir(tmp_path) as temp:
            self._run_test(tmp_path, temp)

    def test_deep(self, *, tmp_path: Path) -> None:
        path = tmp_path / "a/b/c"
        with yield_adjacent_temp_dir(path) as temp:
            self._run_test(path, temp)

    def _run_test(self, path: Path, temp: Path, /) -> None:
        assert temp.is_dir()
        assert temp.parent == path.parent
        assert temp.name.startswith(path.name)


class TestYieldAdjacentTempFile:
    def test_main(self, *, tmp_path: Path) -> None:
        with yield_adjacent_temp_file(tmp_path) as temp:
            self._run_test(tmp_path, temp)

    def test_deep(self, *, tmp_path: Path) -> None:
        path = tmp_path / "a/b/c/file.txt"
        with yield_adjacent_temp_file(path) as temp:
            self._run_test(path, temp)

    def _run_test(self, path: Path, temp: Path, /) -> None:
        assert temp.is_file()
        assert temp.parent == path.parent
        assert temp.name.startswith(path.name)
