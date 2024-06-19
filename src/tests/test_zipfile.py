from __future__ import annotations

from pathlib import Path
from string import ascii_letters
from typing import TYPE_CHECKING
from zipfile import ZipFile

from hypothesis import given
from hypothesis.strategies import sampled_from, sets

from utilities.hypothesis import temp_paths
from utilities.pathvalidate import valid_path
from utilities.platform import maybe_yield_lower_case
from utilities.zipfile import yield_zip_file_contents

if TYPE_CHECKING:
    from collections.abc import Set as AbstractSet


class TestYieldZipFileContents:
    @given(
        temp_path=temp_paths(),
        contents=sets(sampled_from(ascii_letters), min_size=1, max_size=10),
    )
    def test_main(self, temp_path: Path, contents: AbstractSet[str]) -> None:
        contents = set(maybe_yield_lower_case(contents))
        assert temp_path.exists()
        assert not list(temp_path.iterdir())
        path_zip = valid_path(temp_path, "zipfile")
        with ZipFile(path_zip, mode="w") as zf:
            for con in contents:
                zf.writestr(con, con)
        assert path_zip.exists()
        with yield_zip_file_contents(path_zip) as paths:
            assert isinstance(paths, list)
            assert len(paths) == len(contents)
            for path in paths:
                assert isinstance(path, Path)
