from __future__ import annotations

from pytest import mark, param

from utilities.humps import snake_case


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
