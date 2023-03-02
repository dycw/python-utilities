from pytest import mark, param, raises

from utilities.text import ensure_str, snake_case, strip_and_dedent


class TestEnsureStr:
    def test_str(self) -> None:
        assert isinstance(ensure_str(""), str)

    def test_not_str(self) -> None:
        with raises(TypeError, match="x=None"):
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
    def test_main(self, text: str, expected: str) -> None:
        result = snake_case(text)
        assert result == expected


class TestStripAndDedent:
    def test_main(self) -> None:
        text = """
               This is line 1.
               This is line 2.
               """
        result = strip_and_dedent(text)
        expected = "This is line 1.\nThis is line 2."
        assert result == expected
