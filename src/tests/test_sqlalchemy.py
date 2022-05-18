from hypothesis import given
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql.schema import Table

from dycw_utilities.hypothesis.tempfile import temp_dirs
from dycw_utilities.sqlalchemy import create_engine
from dycw_utilities.sqlalchemy import get_columns
from dycw_utilities.sqlalchemy import get_table
from dycw_utilities.tempfile import TemporaryDirectory


class TestCreateEngine:
    @given(temp_dir=temp_dirs())
    def test_main(self, temp_dir: TemporaryDirectory) -> None:
        engine = create_engine("sqlite", database=temp_dir.name.as_posix())
        assert isinstance(engine, Engine)


class TestGetColumns:
    def test_main(self) -> None:
        Base = declarative_base()

        class Example(Base):  # type: ignore
            __tablename__ = "example"

            id = Column(Integer, primary_key=True)

        columns = get_columns(Example)
        assert isinstance(columns, list)
        assert len(columns) == 1
        assert isinstance(columns[0], Column)


class TestGetTable:
    def test_main(self) -> None:
        Base = declarative_base()

        class Example(Base):  # type: ignore
            __tablename__ = "example"

            id = Column(Integer, primary_key=True)

        table = get_table(Example)
        assert isinstance(table, Table)
        assert table.name == "example"
