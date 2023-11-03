from __future__ import annotations

from pathlib import Path

from hypothesis import given
from hypothesis.strategies import integers, none, sets
from sqlalchemy import Column, Engine, Integer, select
from sqlalchemy.orm import declarative_base

from utilities.fastparquet import get_dtypes
from utilities.hypothesis import sqlite_engines, temp_paths
from utilities.pandas import Int64
from utilities.sqlalchemy import ensure_tables_created, insert_items, select_to_parquet


class TestSelectToParquet:
    @given(
        engine=sqlite_engines(),
        ids=sets(integers(min_value=0, max_value=10), min_size=1, max_size=10),
        root=temp_paths(),
        stream=integers(1, 10) | none(),
    )
    def test_streamed_dataframe(
        self, *, engine: Engine, ids: set[int], root: Path, stream: int | None
    ) -> None:
        class Example(declarative_base()):  # does not work with a core table
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        ensure_tables_created(engine, Example)
        insert_items(engine, ([(id_,) for id_ in ids], Example))
        sel = select(Example.id_)
        select_to_parquet(sel, engine, path := root.joinpath("df.parq"), stream=stream)
        dtypes = get_dtypes(path)
        assert dtypes == {"id_": Int64}
