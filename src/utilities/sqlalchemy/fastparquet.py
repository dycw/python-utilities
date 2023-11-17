from __future__ import annotations

from typing import Any

from fastparquet import write
from sqlalchemy import Connection, Engine, Select

from utilities.atomicwrites import writer
from utilities.fastparquet import Compression, write_parquet
from utilities.pathlib import PathLike

from .pandas import select_to_pandas_dataframe  # noqa: TID252
from .sqlalchemy import yield_connection  # noqa: TID252


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
        df = select_to_pandas_dataframe(sel, engine_or_conn, snake=snake)
        return write_parquet(df, path, overwrite=overwrite, compression=compression)
    with writer(path, overwrite=overwrite) as temp, yield_connection(
        engine_or_conn
    ) as conn:
        dfs = select_to_pandas_dataframe(sel, conn, snake=snake, stream=stream)
        for i, df in enumerate(dfs):
            write(str(temp), df, compression=compression, append=i >= 1)
    return None


__all__ = ["select_to_parquet"]
