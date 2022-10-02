from pathlib import Path
from string import ascii_letters
from zipfile import ZipFile

from hypothesis import given
from hypothesis.strategies import sampled_from
from hypothesis.strategies import sets

from utilities.hypothesis.tempfile import temp_paths
from utilities.zipfile import yield_zip_file_contents


class TestYieldZipFileContents:
    @given(
        temp_path=temp_paths(),
        contents=sets(sampled_from(ascii_letters), min_size=1, max_size=10),
    )
    def test_main(self, temp_path: Path, contents: set[str]) -> None:
        assert temp_path.exists()
        assert not list(temp_path.iterdir())
        path_zip = temp_path.joinpath("zipfile")
        with ZipFile(path_zip, mode="w") as zf:
            for con in contents:
                zf.writestr(con, con)
        assert path_zip.exists()
        with yield_zip_file_contents(path_zip) as paths:
            assert isinstance(paths, list)
            assert len(paths) == len(contents)
            for path in paths:
                assert isinstance(path, Path)
