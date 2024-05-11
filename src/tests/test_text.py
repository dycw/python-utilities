from __future__ import annotations

from pytest import mark, param, raises

from utilities.sentinel import sentinel
from utilities.text import EnsureStrError, ensure_str, strip_and_dedent


class TestEnsureStr:
    @mark.parametrize(
        ("obj", "nullable"), [param("", False), param("", True), param(None, True)]
    )
    def test_main(self, *, obj: bool | None, nullable: bool) -> None:
        _ = ensure_str(obj, nullable=nullable)

    @mark.parametrize(
        ("nullable", "match"),
        [
            param(False, "Object .* must be a string"),
            param(True, "Object .* must be a string or None"),
        ],
    )
    def test_error(self, *, nullable: bool, match: str) -> None:
        with raises(EnsureStrError, match=f"{match}; got .* instead"):
            _ = ensure_str(sentinel, nullable=nullable)


class TestStripAndDedent:
    def test_main(self) -> None:
        text = """
               This is line 1.
               This is line 2.
               """
        result = strip_and_dedent(text)
        expected = "This is line 1.\nThis is line 2."
        assert result == expected
