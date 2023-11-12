from __future__ import annotations

from hypothesis import given
from pytest import raises
from sqlalchemy import Engine
from sqlalchemy.exc import DatabaseError

from utilities.hypothesis import sqlite_engines
from utilities.hypothesis.hypothesis import text_ascii
from utilities.sqlalchemy import (
    SQLiteDoesNotSupportSequencesError,
    next_from_sequence,
    redirect_to_no_such_sequence_error,
)


class TestNextFromSequence:
    @given(name=text_ascii(min_size=1), engine=sqlite_engines())
    def test_main(self, *, name: str, engine: Engine) -> None:
        with raises(NotImplementedError):
            _ = next_from_sequence(name, engine)


class TestRedirectToNoSuchSequenceError:
    @given(engine=sqlite_engines())
    def test_main(self, *, engine: Engine) -> None:
        error = DatabaseError(None, None, ValueError("base"))
        with raises(SQLiteDoesNotSupportSequencesError):
            _ = redirect_to_no_such_sequence_error(engine, error)
