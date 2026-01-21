from __future__ import annotations

from typing import TYPE_CHECKING

from pytest import mark, param, raises

from utilities.core import (
    YieldWritePathError,
    write_bytes,
    write_text,
    yield_write_path,
)

if TYPE_CHECKING:
    from pathlib import Path


class TestWriteBytes:
    def test_main(self, *, tmp_path: Path) -> None:
        path = tmp_path / "file"
        write_bytes(path, b"data")
        assert path.is_file()
        assert path.read_bytes() == b"data"


class TestWriteText:
    @mark.parametrize("text", [param("text"), param("text\n")])
    def test_main(self, *, tmp_path: Path, text: str) -> None:
        path = tmp_path / "file.txt"
        write_text(path, text)
        assert path.is_file()
        assert path.read_text() == "text\n"


class TestYieldWritePath:
    def test_main(self, *, tmp_path: Path) -> None:
        path = tmp_path / "file.txt"
        with yield_write_path(path) as temp:
            _ = temp.write_text("text")
            assert not path.exists()
        assert path.is_file()
        assert path.read_text() == "text"

    def test_overwrite(self, *, tmp_path: Path) -> None:
        path = tmp_path / "file.txt"
        _ = path.write_text("init")
        with yield_write_path(path, overwrite=True) as temp:
            _ = temp.write_text("post")
        assert path.read_text() == "post"

    def test_error(self, *, tmp_path: Path) -> None:
        path = tmp_path / "file.txt"
        path.touch()
        with (
            raises(
                YieldWritePathError,
                match=r"Cannot write to '.*' since it already exists",
            ),
            yield_write_path(path),
        ):
            ...
