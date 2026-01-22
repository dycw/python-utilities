from __future__ import annotations

from itertools import pairwise
from os import getenv
from typing import TYPE_CHECKING, assert_never

from pytest import fixture, mark, param, raises

from utilities.constants import EFFECTIVE_GROUP_NAME, EFFECTIVE_USER_NAME
from utilities.core import (
    GetEnvError,
    Permissions,
    PermissionsLike,
    _CopyOrMoveDestinationExistsError,
    _CopyOrMoveSourceNotFoundError,
    chmod,
    copy,
    get_env,
    get_file_group,
    get_file_owner,
    move,
    move_many,
    unique_str,
    yield_temp_environ,
)
from utilities.types import CopyOrMove
from utilities.typing import get_literal_elements

if TYPE_CHECKING:
    from pathlib import Path

    from _pytest.fixtures import SubRequest


@fixture(params=get_literal_elements(CopyOrMove))
def mode(*, request: SubRequest) -> CopyOrMove:
    return request.param


class TestChMod:
    def test_main(self, *, temp_file: Path) -> None:
        perms = Permissions.from_text("u=rw,g=r,o=r")
        chmod(temp_file, perms)
        result = Permissions.from_path(temp_file)
        assert result == perms


class TestCopyOrMove:
    @mark.parametrize(
        ("dest_exists", "overwrite"),
        [param(False, False), param(False, True), param(True, True)],
    )
    def test_file_to_file(
        self, *, tmp_path: Path, mode: CopyOrMove, dest_exists: bool, overwrite: bool
    ) -> None:
        src = self._setup_src_file(tmp_path)
        dest = self._setup_dest_file(tmp_path, exists=dest_exists)
        self._run_test_file(mode, src, dest, overwrite=overwrite)

    def test_file_to_dir(self, *, tmp_path: Path, mode: CopyOrMove) -> None:
        src = self._setup_src_file(tmp_path)
        dest = self._setup_dest_dir(tmp_path, exists=True)
        self._run_test_file(mode, src, dest, overwrite=True)

    @mark.parametrize(
        ("dest_exists", "overwrite"),
        [param(False, False), param(False, True), param(True, True)],
    )
    def test_dir_to_dir(
        self, *, tmp_path: Path, mode: CopyOrMove, dest_exists: bool, overwrite: bool
    ) -> None:
        src = self._setup_src_dir(tmp_path)
        dest = self._setup_dest_dir(tmp_path, exists=dest_exists)
        self._run_test_dir(mode, src, dest, overwrite=overwrite)

    def test_dir_to_file(self, *, tmp_path: Path, mode: CopyOrMove) -> None:
        src = self._setup_src_dir(tmp_path)
        dest = self._setup_dest_file(tmp_path, exists=True)
        self._run_test_dir(mode, src, dest, overwrite=True)

    def test_perms(self, *, tmp_path: Path, mode: CopyOrMove) -> None:
        src = self._setup_src_file(tmp_path)
        dest = self._setup_dest_file(tmp_path)
        perms = Permissions.from_text("u=rw,g=r,o=r")
        self._run_test_file(mode, src, dest, perms=perms)
        result = Permissions.from_path(dest)
        assert result == perms

    def test_user(self, *, tmp_path: Path, mode: CopyOrMove) -> None:
        src = self._setup_src_file(tmp_path)
        dest = self._setup_dest_file(tmp_path)
        self._run_test_file(mode, src, dest, owner=EFFECTIVE_USER_NAME)
        result = get_file_owner(dest)
        assert result == EFFECTIVE_USER_NAME

    def test_group(self, *, tmp_path: Path, mode: CopyOrMove) -> None:
        src = self._setup_src_file(tmp_path)
        dest = self._setup_dest_file(tmp_path)
        self._run_test_file(mode, src, dest, group=EFFECTIVE_GROUP_NAME)
        result = get_file_group(dest)
        assert result == EFFECTIVE_GROUP_NAME

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

    def _setup_src_dir(self, tmp_path: Path, /) -> Path:
        src = tmp_path / "src"
        src.mkdir()
        _ = (src / "src1.txt").write_text("src1")
        _ = (src / "src2.txt").write_text("src2")
        return src

    def _setup_dest_file(self, tmp_path: Path, /, *, exists: bool = False) -> Path:
        dest = tmp_path / "dest.txt"
        if exists:
            _ = dest.write_text("dest")
        return dest

    def _setup_dest_dir(self, tmp_path: Path, /, *, exists: bool = False) -> Path:
        dest = tmp_path / "dest"
        if exists:
            dest.mkdir()
            _ = (dest / "dest1.txt").write_text("dest1")
            _ = (dest / "dest2.txt").write_text("dest2")
            _ = (dest / "dest3.txt").write_text("dest3")
        return dest

    def _run_test_file(
        self,
        mode: CopyOrMove,
        src: Path,
        dest: Path,
        /,
        *,
        overwrite: bool = False,
        perms: PermissionsLike | None = None,
        owner: str | int | None = None,
        group: str | int | None = None,
    ) -> None:
        match mode:
            case "copy":
                copy(
                    src,
                    dest,
                    overwrite=overwrite,
                    perms=perms,
                    owner=owner,
                    group=group,
                )
                assert src.is_file()
                assert src.read_text() == "src"
            case "move":
                move(
                    src,
                    dest,
                    overwrite=overwrite,
                    perms=perms,
                    owner=owner,
                    group=group,
                )
                assert not src.exists()
            case never:
                assert_never(never)
        assert dest.is_file()
        assert dest.read_text() == "src"

    def _run_test_dir(
        self, mode: CopyOrMove, src: Path, dest: Path, /, *, overwrite: bool = False
    ) -> None:
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


class TestMoveMany:
    def test_many(self, *, tmp_path: Path) -> None:
        n = 5
        files = [tmp_path.joinpath(f"file{i}") for i in range(n + 2)]
        for i, file in enumerate(files[:-1]):
            _ = file.write_text(str(i))
        move_many(*pairwise(files), overwrite=True)
        for i, file in enumerate(files[1:], start=1):
            assert file.read_text() == str(i - 1)


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
