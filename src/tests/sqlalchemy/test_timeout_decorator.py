from __future__ import annotations

from hypothesis import given
from pytest import raises
from sqlalchemy import Engine
from sqlalchemy.exc import DatabaseError

from utilities._sqlalchemy.timeout_decorator import (
    NextFromSequenceError,
    next_from_sequence,
    redirect_to_next_from_sequence_error,
)
from utilities.hypothesis import sqlite_engines, text_ascii


class TestNextFromSequence:
    @given(name=text_ascii(min_size=1), engine=sqlite_engines())
    def test_main(self, *, name: str, engine: Engine) -> None:
        with raises(NotImplementedError):
            _ = next_from_sequence(name, engine)


class TestRedirectToNoSuchSequenceError:
    @given(engine=sqlite_engines())
    def test_main(self, *, engine: Engine) -> None:
        error = DatabaseError(None, None, ValueError("base"))
        with raises(NextFromSequenceError):
            _ = redirect_to_next_from_sequence_error(engine, error)
