from typing import Optional

from sqlalchemy import create_engine as _create_engine
from sqlalchemy.engine import URL
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool
from sqlalchemy.pool import Pool


def create_engine(
    drivername: str,
    /,
    *,
    username: Optional[str] = None,
    password: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    poolclass: Optional[type[Pool]] = NullPool,
) -> Engine:
    """Create a SQLAlchemy engine."""

    url = URL.create(
        drivername,
        username=username,
        password=password,
        host=host,
        port=port,
        database=database,
    )
    return _create_engine(url, future=True, poolclass=poolclass)
