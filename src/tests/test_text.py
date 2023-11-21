from __future__ import annotations

from hypothesis import given
from pytest import mark, param, raises

from utilities.hypothesis import text_ascii
from utilities.text import (
    NotAStringError,
    SnakeCaseContainsDuplicatesError,
    ensure_str,
    snake_case,
    snake_case_mappings,
    strip_and_dedent,
)


class TestEnsureStr:
    def test_str(self) -> None:
        assert isinstance(ensure_str(""), str)

    def test_not_str(self) -> None:
        with raises(NotAStringError):
            _ = ensure_str(None)


class TestSnakeCase:
    @mark.parametrize(
        ("text", "expected"),
        [
            # inflection
            param("Product", "product"),
            param("SpecialGuest", "special_guest"),
            param("ApplicationController", "application_controller"),
            param("Area51Controller", "area51_controller"),
            param("HTMLTidy", "html_tidy"),
            param("HTMLTidyGenerator", "html_tidy_generator"),
            param("FreeBSD", "free_bsd"),
            param("HTML", "html"),
            # custom
            param("text", "text"),
            param("Text", "text"),
            param("text123", "text123"),
            param("Text123", "text123"),
            param("OneTwo", "one_two"),
            param("One Two", "one_two"),
            param("One  Two", "one_two"),
            param("One   Two", "one_two"),
            param("One_Two", "one_two"),
            param("One__Two", "one_two"),
            param("One___Two", "one_two"),
            param("NoHTML", "no_html"),
            param("HTMLVersion", "html_version"),
        ],
    )
    def test_main(self, *, text: str, expected: str) -> None:
        result = snake_case(text)
        assert result == expected


class TestSnakeCaseMappings:
    @given(text=text_ascii())
    def test_success(self, *, text: str) -> None:
        result = snake_case_mappings([text])
        expected = {text: snake_case(text)}
        assert result == expected

    @given(text=text_ascii())
    def test_inverse(self, *, text: str) -> None:
        result = snake_case_mappings([text], inverse=True)
        expected = {snake_case(text): text}
        assert result == expected

    @given(text=text_ascii(min_size=1))
    def test_error(self, *, text: str) -> None:
        with raises(SnakeCaseContainsDuplicatesError):
            _ = snake_case_mappings([text.lower(), text.upper()])


class TestStripAndDedent:
    def test_main(self) -> None:
        text = """
               This is line 1.
               This is line 2.
               """
        result = strip_and_dedent(text)
        expected = "This is line 1.\nThis is line 2."
        assert result == expected
