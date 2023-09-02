from __future__ import annotations

from typing import Any

from fastparquet import write
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.sql import Select

from utilities.atomicwrites import writer
from utilities.fastparquet import Compression, write_parquet
from utilities.pathlib import PathLike
from utilities.sqlalchemy import yield_connection
from utilities.sqlalchemy.pandas import select_to_dataframe


def select_to_parquet(
    sel: Select[Any],
    engine_or_conn: Engine | Connection,
    path: PathLike,
    /,
    *,
    stream: int | None = None,
    snake: bool = False,
    overwrite: bool = False,
    compression: Compression | None = "gzip",
) -> None:
    """Read a table from a database into a Parquet file.

    Optionally stream it in chunks.
    """
    if stream is None:
        df = select_to_dataframe(sel, engine_or_conn, snake=snake)
        return write_parquet(
            df, path, overwrite=overwrite, compression=compression
        )
    with writer(path, overwrite=overwrite) as temp, yield_connection(
        engine_or_conn
    ) as conn:
        temp_str = temp.as_posix()
        dfs = select_to_dataframe(sel, conn, snake=snake, stream=stream)
        for i, df in enumerate(dfs):
            write(temp_str, df, compression=compression, append=i >= 1)
    return None
