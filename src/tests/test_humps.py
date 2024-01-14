from __future__ import annotations

from hypothesis import given
from pytest import mark, param, raises

from utilities.humps import SnakeCaseMappingsError, snake_case, snake_case_mappings
from utilities.hypothesis import text_ascii


class TestSnakeCase:
    @mark.parametrize(
        ("text", "expected"),
        [
            param("Product", "product"),
            param("SpecialGuest", "special_guest"),
            param("ApplicationController", "application_controller"),
            param("Area51Controller", "area51_controller"),
            param("HTMLTidy", "html_tidy"),
            param("HTMLTidyGenerator", "html_tidy_generator"),
            param("FreeBSD", "free_bsd"),
            param("HTML", "html"),
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
    def test_main(self, *, text: str) -> None:
        result = snake_case_mappings([text])
        expected = {text: snake_case(text)}
        assert result == expected

    @given(text=text_ascii(min_size=1))
    def test_error_keys(self, *, text: str) -> None:
        with raises(
            SnakeCaseMappingsError,
            match="Strings .* must not contain duplicates; got .*",
        ):
            _ = snake_case_mappings([text, text])

    @given(text=text_ascii(min_size=1))
    def test_error_values(self, *, text: str) -> None:
        with raises(
            SnakeCaseMappingsError,
            match="Snake-cased strings .* must not contain duplicates; got .*",
        ):
            _ = snake_case_mappings([text.lower(), text.upper()])
