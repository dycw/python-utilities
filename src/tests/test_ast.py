from __future__ import annotations

from ast import Assign, Try, parse

from utilities.ast import _yield_from_assign, _yield_from_try, yield_dunder_all
from utilities.iterables import one
from utilities.text import strip_and_dedent


class TestYieldDunderAll:
    def test_main(self) -> None:
        text = strip_and_dedent(
            """
            __all__ = ["A", "B"]

            try:
                from utilities import C, D
            except ModuleNotFoundError:
                pass
            else:
                __all__ += ["C", "D"]
            """
        )
        result = list(yield_dunder_all(text))
        expected = [["A", "B"], ["C", "D"]]
        assert result == expected


class TestYieldFromAssign:
    def test_main(self) -> None:
        text = strip_and_dedent(
            """
            __all__ = ["A", "B"]
            """
        )
        module = parse(text)
        assign = one(stmt for stmt in module.body if isinstance(stmt, Assign))
        result = list(_yield_from_assign(assign))
        expected = [["A", "B"]]
        assert result == expected

    def test_error(self) -> None:
        text = strip_and_dedent(
            """
            a = b = ["A", "B"]
            """
        )
        module = parse(text)
        assign = one(stmt for stmt in module.body if isinstance(stmt, Assign))
        result = list(_yield_from_assign(assign))
        assert result == []


class TestYieldFromTry:
    def test_main(self) -> None:
        text = strip_and_dedent(
            """
            try:
                from utilities import A, B
            except ModuleNotFoundError:
                pass
            else:
                __all__ += ["A", "B"]
            """
        )
        module = parse(text)
        try_ = one(stmt for stmt in module.body if isinstance(stmt, Try))
        result = list(_yield_from_try(try_))
        expected = [["A", "B"]]
        assert result == expected

    def test_error(self) -> None:
        text = strip_and_dedent(
            """
            try:
                from utilities import A, B
            except ModuleNotFoundError:
                pass
            else:
                a = b = ["A", "B"]
            """
        )
        module = parse(text)
        try_ = one(stmt for stmt in module.body if isinstance(stmt, Try))
        result = list(_yield_from_try(try_))
        assert result == []
