from __future__ import annotations

from typing import TYPE_CHECKING

from pytest import mark, param, raises

from utilities.constants import EFFECTIVE_GROUP_NAME, EFFECTIVE_USER_NAME
from utilities.core import (
    Permissions,
    YieldWritePathError,
    get_file_group,
    get_file_owner,
    yield_gzip,
    yield_write_path,
)

if TYPE_CHECKING:
    from pathlib import Path


class TestYieldWritePath:
    def test_main(self, *, temp_path_not_exist: Path) -> None:
        with yield_write_path(temp_path_not_exist) as temp:
            _ = temp.write_text("text")
            assert not temp_path_not_exist.exists()
        assert temp_path_not_exist.is_file()
        assert temp_path_not_exist.read_text() == "text"

    def test_compress(self, *, temp_path_not_exist: Path) -> None:
        with yield_write_path(temp_path_not_exist, compress=True) as temp:
            _ = temp.write_bytes(b"data")
        assert temp_path_not_exist.is_file()
        with yield_gzip(temp_path_not_exist) as temp:
            assert temp.read_bytes() == b"data"

    def test_overwrite(self, *, temp_file: Path) -> None:
        _ = temp_file.write_text("init")
        with yield_write_path(temp_file, overwrite=True) as temp:
            _ = temp.write_text("post")
        assert temp_file.read_text() == "post"

    def test_perms(self, *, temp_path_not_exist: Path) -> None:
        perms = Permissions.from_text("u=rw,g=r,o=r")
        with yield_write_path(temp_path_not_exist, perms=perms) as temp:
            temp.touch()
        assert Permissions.from_path(temp_path_not_exist) == perms

    def test_owner(self, *, temp_path_not_exist: Path) -> None:
        with yield_write_path(temp_path_not_exist, owner=EFFECTIVE_USER_NAME) as temp:
            temp.touch()
        assert get_file_owner(temp_path_not_exist) == EFFECTIVE_USER_NAME

    def test_group(self, *, temp_path_not_exist: Path) -> None:
        with yield_write_path(temp_path_not_exist, group=EFFECTIVE_GROUP_NAME) as temp:
            temp.touch()
        assert get_file_group(temp_path_not_exist) == EFFECTIVE_GROUP_NAME

    @mark.parametrize("compress", [param(False), param(True)])
    def test_error(self, *, temp_file: Path, compress: bool) -> None:
        with (
            raises(
                YieldWritePathError,
                match=r"Cannot write to '.*' since it already exists",
            ),
            yield_write_path(temp_file, compress=compress),
        ):
            ...
