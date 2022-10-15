from pytest import mark
from pytest import param

from utilities.inflection import snake_case


class TestSnakeCase:
    @mark.parametrize(
        ["text", "expected"],
        [
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
            param("HTML", "html"),
            param("NoHTML", "no_html"),
            param("HTMLVersion", "html_version"),
        ],
    )
    def test_main(self, text: str, expected: str) -> None:
        result = snake_case(text)
        assert result == expected
