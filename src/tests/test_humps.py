from __future__ import annotations

import pytest
from hypothesis import given

from utilities.humps import SnakeCaseMappingsError, snake_case, snake_case_mappings
from utilities.hypothesis import text_ascii


class TestSnakeCase:
    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            pytest.param("Product", "product"),
            pytest.param("SpecialGuest", "special_guest"),
            pytest.param("ApplicationController", "application_controller"),
            pytest.param("Area51Controller", "area51_controller"),
            pytest.param("HTMLTidy", "html_tidy"),
            pytest.param("HTMLTidyGenerator", "html_tidy_generator"),
            pytest.param("FreeBSD", "free_bsd"),
            pytest.param("HTML", "html"),
            pytest.param("text", "text"),
            pytest.param("Text", "text"),
            pytest.param("text123", "text123"),
            pytest.param("Text123", "text123"),
            pytest.param("OneTwo", "one_two"),
            pytest.param("One Two", "one_two"),
            pytest.param("One  Two", "one_two"),
            pytest.param("One   Two", "one_two"),
            pytest.param("One_Two", "one_two"),
            pytest.param("One__Two", "one_two"),
            pytest.param("One___Two", "one_two"),
            pytest.param("NoHTML", "no_html"),
            pytest.param("HTMLVersion", "html_version"),
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
        with pytest.raises(
            SnakeCaseMappingsError,
            match="Strings .* must not contain duplicates; got .*",
        ):
            _ = snake_case_mappings([text, text])

    @given(text=text_ascii(min_size=1))
    def test_error_values(self, *, text: str) -> None:
        with pytest.raises(
            SnakeCaseMappingsError,
            match="Snake-cased strings .* must not contain duplicates; got .*",
        ):
            _ = snake_case_mappings([text.lower(), text.upper()])
