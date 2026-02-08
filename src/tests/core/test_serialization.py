from __future__ import annotations

from pytest import mark, param, raises

from utilities.core import ParseBoolError, parse_bool


class TestParseBool:
    @mark.parametrize(
        ("text", "expected"),
        [
            param("1", True),
            param("True", True),
            param("Y", True),
            param("Yes", True),
            param("On", True),
            param("0", False),
            param("False", False),
            param("N", False),
            param("No", False),
            param("Off", False),
        ],
    )
    def test_main(self, *, text: str, expected: bool) -> None:
        assert parse_bool(text) is expected
        assert parse_bool(text.lower()) is expected
        assert parse_bool(text.upper()) is expected

    @mark.parametrize(
        "text",
        [
            param("00"),
            param("11"),
            param("ffalsee"),
            param("invalid"),
            param("nn"),
            param("nnoo"),
            param("oofff"),
            param("oonn"),
            param("ttruee"),
            param("yy"),
            param("yyess"),
        ],
    )
    def test_error(self, *, text: str) -> None:
        with raises(ParseBoolError, match=r"Unable to parse boolean value; got '.*'"):
            _ = parse_bool(text)
