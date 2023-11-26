from __future__ import annotations

from typing import NoReturn

import timeout_decorator
from sqlalchemy import Connection, Engine, Sequence
from sqlalchemy.exc import DatabaseError
from typing_extensions import assert_never

from utilities._sqlalchemy.common import Dialect, get_dialect, yield_connection
from utilities.errors import redirect_error
from utilities.math import FloatFinNonNeg, IntNonNeg


def next_from_sequence(
    name: str,
    engine_or_conn: Engine | Connection,
    /,
    *,
    timeout: FloatFinNonNeg | None = None,
) -> IntNonNeg | None:
    """Get the next element from a sequence."""

    def inner() -> int:
        seq = Sequence(name)
        try:
            with yield_connection(engine_or_conn) as conn:  # pragma: no cover
                return conn.scalar(seq)
        except DatabaseError as error:
            try:  # pragma: no cover
                redirect_to_next_from_sequence_error(
                    engine_or_conn, error
                )  # pragma: no cover
            except NextFromSequenceError:  # pragma: no cover
                with yield_connection(engine_or_conn) as conn:  # pragma: no cover
                    _ = seq.create(conn)  # pragma: no cover
                return inner()  # pragma: no cover

    if timeout is None:
        return inner()
    func = timeout_decorator.timeout(seconds=timeout)(inner)  # pragma: no cover
    try:  # pragma: no cover
        return func()  # pragma: no cover
    except timeout_decorator.TimeoutError:  # pragma: no cover
        return None  # pragma: no cover


def redirect_to_next_from_sequence_error(
    engine_or_conn: Engine | Connection, error: DatabaseError, /
) -> NoReturn:
    """Redirect to the `NextFromSequenceError`."""
    match dialect := get_dialect(engine_or_conn):
        case (  # pragma: no cover
            Dialect.mssql
            | Dialect.mysql
            | Dialect.postgresql
        ):
            raise NotImplementedError(dialect)  # pragma: no cover
        case Dialect.oracle:  # pragma: no cover
            pattern = "ORA-02289: sequence does not exist"
        case Dialect.sqlite:
            msg = f"{engine_or_conn=}, {error=}"
            raise NextFromSequenceError(msg) from None
        case _:  # pragma: no cover  # type: ignore
            assert_never(dialect)
    return redirect_error(
        error, pattern, NextFromSequenceError(*error.args)
    )  # pragma: no cover


class NextFromSequenceError(Exception):
    ...


__all__ = [
    "next_from_sequence",
    "NextFromSequenceError",
    "redirect_to_next_from_sequence_error",
]
