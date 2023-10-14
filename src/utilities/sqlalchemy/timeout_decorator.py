from __future__ import annotations

from typing import NoReturn

import timeout_decorator
from sqlalchemy import Sequence
from sqlalchemy.engine import Connection
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DatabaseError

from utilities.errors import redirect_error
from utilities.math import FloatFinNonNeg
from utilities.math import IntNonNeg
from utilities.typing import never

from .sqlalchemy import get_dialect  # noqa: TID252
from .sqlalchemy import yield_connection  # noqa: TID252


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
        dialect == "mssql"  # noqa: PLR1714
        or dialect == "mysql"
        or dialect == "postgresql"
        or dialect == "sqlite"
    ):
        raise NotImplementedError(dialect)  # pragma: no cover
    if dialect == "oracle":  # pragma: no cover
        pattern = "ORA-02289: sequence does not exist"
    else:  # pragma: no cover
        return never(dialect)
    return redirect_error(
        error, pattern, NoSuchSequenceError
    )  # pragma: no cover


class NoSuchSequenceError(Exception):
    """Raised when a sequence does not exist."""
