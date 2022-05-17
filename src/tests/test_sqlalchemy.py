from hypothesis import given
from sqlalchemy.engine import Engine

from dycw_utilities.hypothesis.tempfile import temp_dirs
from dycw_utilities.sqlalchemy import create_engine
from dycw_utilities.tempfile import TemporaryDirectory


class TestCreateEngine:
    @given(temp_dir=temp_dirs())
    def test_main(self, temp_dir: TemporaryDirectory) -> None:
        engine = create_engine("sqlite", database=temp_dir.name.as_posix())
        assert isinstance(engine, Engine)
