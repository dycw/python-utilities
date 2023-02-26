from typing import Optional
from typing import Union

from beartype import beartype
from fastparquet import write
from sqlalchemy.engine import Connection
from sqlalchemy.engine import Engine
from sqlalchemy.sql import Select

from utilities.atomicwrites import writer
from utilities.fastparquet import Compression
from utilities.fastparquet import write_parquet
from utilities.pathlib import PathLike
from utilities.sqlalchemy import yield_connection
from utilities.sqlalchemy.pandas import select_to_dataframe


@beartype
def select_to_parquet(
    sel: Select,
    engine_or_conn: Union[Engine, Connection],
    path: PathLike,
    /,
    *,
    stream: Optional[int] = None,
    snake: bool = False,
    overwrite: bool = False,
    compression: Optional[Compression] = "gzip",
) -> None:
    """Read a table from a database into a Parquet file.

    Optionally stream it in chunks.
    """
    if stream is None:
        df = select_to_dataframe(sel, engine_or_conn, snake=snake)
        return write_parquet(
            df,
            path,
            overwrite=overwrite,
            compression=compression,
        )
    with writer(path, overwrite=overwrite) as temp, yield_connection(
        engine_or_conn,
    ) as conn:
        temp_str = temp.as_posix()
        dfs = select_to_dataframe(sel, conn, snake=snake, stream=stream)
        for i, df in enumerate(dfs):
            write(temp_str, df, compression=compression, append=i >= 1)
    return None
