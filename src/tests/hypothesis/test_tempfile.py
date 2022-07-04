from string import ascii_letters

from hypothesis import given
from hypothesis.strategies import sampled_from
from hypothesis.strategies import sets

from utilities.hypothesis.tempfile import temp_dirs
from utilities.tempfile import TemporaryDirectory


class TestTempDirs:
    @given(temp_dir=temp_dirs())
    def test_main(self, temp_dir: TemporaryDirectory) -> None:
        path = temp_dir.name
        assert path.is_dir()
        assert len(set(path.iterdir())) == 0

    @given(
        temp_dir=temp_dirs(),
        contents=sets(sampled_from(ascii_letters), max_size=10),
    )
    def test_writing_files(
        self, temp_dir: TemporaryDirectory, contents: set[str]
    ) -> None:
        path = temp_dir.name
        assert len(set(path.iterdir())) == 0
        for content in contents:
            path.joinpath(content).touch()
        assert len(set(path.iterdir())) == len(contents)
