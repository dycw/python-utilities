from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

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
            with yield_connection(
                engine_or_conn
            ) as conn, redirect_next_from_sequence_error(conn):  # pragma: no cover
                return conn.scalar(seq)
        except NextFromSequenceError:
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


@contextmanager
def redirect_next_from_sequence_error(
    engine_or_conn: Engine | Connection
) -> Iterator[None]:
    """Redirect to the `NextFromSequenceError`."""
    match dialect := get_dialect(engine_or_conn):
        case (  # pragma: no cover
            Dialect.mssql
            | Dialect.mysql
            | Dialect.postgresql
        ):
            raise NotImplementedError(dialect)  # pragma: no cover
        case Dialect.oracle:  # pragma: no cover
            match = "ORA-02289: sequence does not exist"
        case Dialect.sqlite:
            msg = f"{engine_or_conn=}"
            raise NotImplementedError(msg)
        case _:  # pragma: no cover  # type: ignore
            assert_never(dialect)
    with redirect_error(
        DatabaseError, NextFromSequenceError, match=match
    ):  # pragma: no cover
        yield


class NextFromSequenceError(Exception):
    ...


__all__ = [
    "NextFromSequenceError",
    "next_from_sequence",
    "redirect_next_from_sequence_error",
]
