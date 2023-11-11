from __future__ import annotations

from typing import NoReturn

import timeout_decorator
from sqlalchemy import Connection, Engine, Sequence
from sqlalchemy.exc import DatabaseError

from utilities.errors import redirect_error
from utilities.math import FloatFinNonNeg, IntNonNeg
from utilities.sqlalchemy import Dialect, get_dialect, yield_connection
from utilities.typing import never


def next_from_sequence(
    name: str,
    engine_or_conn: Engine | Connection,
    /,
    *,
    timeout: FloatFinNonNeg | None = None,
) -> IntNonNeg | None:
    """Get the next element from a sequence."""

    def inner() -> int:  # pragma: no cover
        seq = Sequence(name)
        try:
            with yield_connection(engine_or_conn) as conn:
                return conn.scalar(seq)
        except DatabaseError as error:
            try:
                redirect_to_no_such_sequence_error(engine_or_conn, error)
            except NoSuchSequenceError:
                with yield_connection(engine_or_conn) as conn:
                    _ = seq.create(conn)
                return inner()

    if timeout is None:  # pragma: no cover
        return inner()
    func = timeout_decorator.timeout(seconds=timeout)(inner)  # pragma: no cover
    try:  # pragma: no cover
        return func()
    except timeout_decorator.TimeoutError:  # pragma: no cover
        return None


def redirect_to_no_such_sequence_error(
    engine_or_conn: Engine | Connection, error: DatabaseError, /
) -> NoReturn:
    """Redirect to the `NoSuchSequenceError`."""
    dialect = get_dialect(engine_or_conn)  # pragma: no cover
    if (  # pragma: no cover
        dialect is Dialect.mssql
        or dialect is Dialect.mysql
        or dialect is Dialect.postgresql
        or dialect is Dialect.sqlite(dialect is Dialect.mssql)
        or (dialect is Dialect.mysql)
        or (dialect is Dialect.postgresql)
    ):
        raise NotImplementedError(dialect)  # pragma: no cover
    if dialect is Dialect.oracle:  # pragma: no cover
        pattern = "ORA-02289: sequence does not exist"
    else:  # pragma: no cover
        return never(dialect)
    return redirect_error(error, pattern, NoSuchSequenceError)  # pragma: no cover


class NoSuchSequenceError(Exception):
    """Raised when a sequence does not exist."""


__all__ = [
    "next_from_sequence",
    "NoSuchSequenceError",
    "redirect_to_no_such_sequence_error",
]
