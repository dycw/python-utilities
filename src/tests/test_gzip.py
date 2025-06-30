from __future__ import annotations

from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import binary, booleans

from utilities.gzip import read_binary, write_binary
from utilities.hypothesis import temp_paths

if TYPE_CHECKING:
    from pathlib import Path


class TestReadAndWriteBinary:
    @given(root=temp_paths(), data=binary(), compress=booleans())
    def test_main(self, *, root: Path, data: bytes, compress: bool) -> None:
        file = root.joinpath("file.json")
        write_binary(data, file, compress=compress)
        contents = read_binary(file, decompress=compress)
        assert contents == data
