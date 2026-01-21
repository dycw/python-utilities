from __future__ import annotations

from os import getenv
from typing import TYPE_CHECKING, assert_never

from pytest import mark, param, raises

from utilities.core import (
    GetEnvError,
    _CopyOrMoveDestinationExistsError,
    _CopyOrMoveSourceNotFoundError,
    copy,
    get_env,
    move,
    unique_str,
    yield_temp_environ,
)

if TYPE_CHECKING:
    from pathlib import Path

    from utilities.types import CopyOrMove


class TestCopyOrMove:
    @mark.parametrize("mode", [param("copy"), param("move")])
    @mark.parametrize(
        ("dest_exists", "overwrite"),
        [param(False, False), param(False, True), param(True, True)],
    )
    def test_file_to_file(
        self, *, tmp_path: Path, mode: CopyOrMove, dest_exists: bool, overwrite: bool
    ) -> None:
        src = self._setup_src_file(tmp_path)
        dest = tmp_path / "dest.txt"
        if dest_exists:
            _ = dest.write_text("dest")
        match mode:
            case "copy":
                copy(src, dest, overwrite=overwrite)
                assert src.is_file()
                assert src.read_text() == "src"
            case "move":
                move(src, dest, overwrite=overwrite)
                assert not src.exists()
            case never:
                assert_never(never)
        assert dest.is_file()
        assert dest.read_text() == "src"

    @mark.parametrize("mode", [param("copy"), param("move")])
    def test_file_to_dir(self, *, tmp_path: Path, mode: CopyOrMove) -> None:
        src = self._setup_src_file(tmp_path)
        dest = tmp_path / "dest"
        dest.mkdir()
        match mode:
            case "copy":
                copy(src, dest, overwrite=True)
                assert src.is_file()
                assert src.read_text() == "src"
            case "move":
                move(src, dest, overwrite=True)
                assert not src.exists()
            case never:
                assert_never(never)
        assert dest.is_file()
        assert dest.read_text() == "src"

    @mark.parametrize("mode", [param("copy"), param("move")])
    @mark.parametrize(
        ("dest_exists", "overwrite"),
        [param(False, False), param(False, True), param(True, True)],
    )
    def test_dir_to_dir(
        self, *, tmp_path: Path, mode: CopyOrMove, dest_exists: bool, overwrite: bool
    ) -> None:
        src = tmp_path / "src"
        src.mkdir()
        _ = (src / "src1.txt").write_text("src1")
        _ = (src / "src2.txt").write_text("src2")
        dest = tmp_path / "dest"
        if dest_exists:
            dest.mkdir()
            _ = (dest / "dest1.txt").write_text("dest1")
            _ = (dest / "dest2.txt").write_text("dest2")
            _ = (dest / "dest3.txt").write_text("dest3")
        match mode:
            case "copy":
                copy(src, dest, overwrite=overwrite)
                assert src.is_dir()
                assert {f.name for f in src.iterdir()} == {"src1.txt", "src2.txt"}
            case "move":
                move(src, dest, overwrite=overwrite)
                assert not src.exists()
            case never:
                assert_never(never)
        assert dest.is_dir()
        assert {f.name for f in dest.iterdir()} == {"src1.txt", "src2.txt"}
        assert (dest / "src1.txt").read_text() == "src1"
        assert (dest / "src2.txt").read_text() == "src2"

    @mark.parametrize("mode", [param("copy"), param("move")])
    def test_dir_to_file(self, *, tmp_path: Path, mode: CopyOrMove) -> None:
        src = tmp_path / "src"
        src.mkdir()
        _ = (src / "src1.txt").write_text("src1")
        _ = (src / "src2.txt").write_text("src2")
        dest = tmp_path / "dest"
        _ = dest.write_text("dest")
        match mode:
            case "copy":
                copy(src, dest, overwrite=True)
                assert src.is_dir()
                assert {f.name for f in src.iterdir()} == {"src1.txt", "src2.txt"}
            case "move":
                move(src, dest, overwrite=True)
                assert not src.exists()
            case never:
                assert_never(never)
        assert dest.is_dir()
        assert {f.name for f in dest.iterdir()} == {"src1.txt", "src2.txt"}
        assert (dest / "src1.txt").read_text() == "src1"
        assert (dest / "src2.txt").read_text() == "src2"

    def test_error_source_not_found(
        self, *, tmp_path: Path, temp_path_not_exist: Path
    ) -> None:
        with raises(
            _CopyOrMoveSourceNotFoundError, match=r"Source '.*' does not exist"
        ):
            move(temp_path_not_exist, tmp_path)

    def test_error_file_exists(self, *, temp_file: Path) -> None:
        with raises(
            _CopyOrMoveDestinationExistsError,
            match=r"Cannot move source '.*' since destination '.*' already exists",
        ):
            move(temp_file, temp_file)

    def _setup_src_file(self, tmp_path: Path, /) -> Path:
        src = tmp_path / "src.txt"
        _ = src.write_text("src")
        return src

    def _setup_dest_file(self, tmp_path: Path, /, *, exists: bool = False) -> Path:
        dest = tmp_path / "dest.txt"
        if exists:
            _ = dest.write_text("dest")
        src = tmp_path / "src.txt"
        _ = src.write_text("src")
        return src


class TestGetEnv:
    def test_main(self) -> None:
        key, value = self._generate()
        with yield_temp_environ({key: value}):
            assert get_env(key) == value

    def test_case_insensitive(self) -> None:
        key, value = self._generate()
        with yield_temp_environ({key.lower(): value}):
            assert get_env(key.upper()) == value

    def test_default(self) -> None:
        key, value = self._generate()
        assert get_env(key, default=value) == value

    def test_nullable(self) -> None:
        key, _ = self._generate()
        assert get_env(key, nullable=True) is None

    def test_error_case_insensitive(self) -> None:
        key1, value = self._generate()
        key2, _ = self._generate()
        with (
            yield_temp_environ({key1: value}),
            raises(GetEnvError, match=r"No environment variable '.*' \(modulo case\)"),
        ):
            _ = get_env(key2)

    def test_error_case_sensitive(self) -> None:
        key, value = self._generate()
        with (
            yield_temp_environ({key.lower(): value}),
            raises(GetEnvError, match=r"No environment variable '.*'"),
        ):
            _ = get_env(key.upper(), case_sensitive=True)

    def _generate(self) -> tuple[str, str]:
        key = f"_TEST_OS_{unique_str()}"
        value = unique_str()
        return key, value


class TestYieldTempEnviron:
    def test_set(self) -> None:
        key, value = self._generate()
        assert getenv(key) is None
        with yield_temp_environ({key: value}):
            assert getenv(key) == value
        assert getenv(key) is None

    def test_override(self) -> None:
        key, value1 = self._generate()
        with yield_temp_environ({key: value1}):
            assert getenv(key) == value1
            _, value2 = self._generate()
            with yield_temp_environ({key: value2}):
                assert getenv(key) == value2
            assert getenv(key) == value1

    def test_unset(self) -> None:
        key, value = self._generate()
        with yield_temp_environ({key: value}):
            assert getenv(key) == value
            with yield_temp_environ({key: None}):
                assert getenv(key) is None
            assert getenv(key) == value

    def _generate(self) -> tuple[str, str]:
        key = f"_TEST_OS_{unique_str()}"
        value = unique_str()
        return key, value
