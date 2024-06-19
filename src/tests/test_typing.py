from __future__ import annotations

from typing import Literal

from pytest import raises

from utilities.typing import GetLiteralArgsError, get_literal_args


class TestGetLiteralArgs:
    def test_main(self) -> None:
        literal = Literal["a", "b", "c"]
        result: tuple[Literal["a", "b", "c"], ...] = get_literal_args(literal)
        expected = ("a", "b", "c")
        assert result == expected

    def test_error(self) -> None:
        union = int | str
        with raises(
            GetLiteralArgsError,
            match="Object .* must be a Literal; got origin .* instead",
        ):
            _ = get_literal_args(union)
