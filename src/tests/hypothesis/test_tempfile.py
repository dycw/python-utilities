from hypothesis import given

from dycw_utilities.hypothesis.tempfile import temp_dirs
from dycw_utilities.tempfile import TemporaryDirectory


@given(temp_dir=temp_dirs())
def test_temp_dirs(temp_dir: TemporaryDirectory) -> None:
    assert isinstance(temp_dir, TemporaryDirectory)
    path = temp_dir.name
    assert path.is_dir()
    assert set(path.iterdir()) == set()
