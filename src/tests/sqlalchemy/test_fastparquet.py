from pathlib import Path
from typing import Optional

from hypothesis import given
from hypothesis.strategies import DataObject
from hypothesis.strategies import data
from hypothesis.strategies import integers
from hypothesis.strategies import none
from hypothesis_sqlalchemy.sample import table_records_lists
from sqlalchemy import Column
from sqlalchemy import Engine
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy.orm import declarative_base

from utilities.fastparquet import get_dtypes
from utilities.hypothesis import temp_paths
from utilities.hypothesis.sqlalchemy import sqlite_engines
from utilities.pandas import Int64
from utilities.sqlalchemy import ensure_table_created
from utilities.sqlalchemy import get_table
from utilities.sqlalchemy.fastparquet import select_to_parquet
from utilities.sqlalchemy.pandas import insert_items


class TestSelectToParquet:
    @given(
        data=data(),
        engine=sqlite_engines(),
        root=temp_paths(),
        stream=integers(1, 10) | none(),
    )
    def test_streamed_dataframe(
        self,
        data: DataObject,
        engine: Engine,
        root: Path,
        stream: Optional[int],
    ) -> None:
        class Example(declarative_base()):  # does not work with a core table
            __tablename__ = "example"
            Id = Column(Integer, primary_key=True)

        rows = data.draw(table_records_lists(get_table(Example), min_size=1))
        ensure_table_created(Example, engine)
        insert_items([(rows, Example)], engine)
        sel = select(Example.Id)
        select_to_parquet(
            sel,
            engine,
            path := root.joinpath("df.parq"),
            stream=stream,
        )
        dtypes = get_dtypes(path)
        assert dtypes == {"Id": Int64}
