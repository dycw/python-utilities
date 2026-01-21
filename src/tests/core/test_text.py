from __future__ import annotations

from pytest import mark, param

from utilities.core import kebab_case, pascal_case, snake_case, unique_str
from utilities.text import strip_and_dedent


class TestPascalSnakeAndKebabCase:
    @mark.parametrize(
        ("text", "exp_pascal", "exp_snake", "exp_kebab"),
        [
            param("API", "API", "api", "api"),
            param("APIResponse", "APIResponse", "api_response", "api-response"),
            param(
                "ApplicationController",
                "ApplicationController",
                "application_controller",
                "application-controller",
            ),
            param(
                "Area51Controller",
                "Area51Controller",
                "area51_controller",
                "area51-controller",
            ),
            param("FreeBSD", "FreeBSD", "free_bsd", "free-bsd"),
            param("HTML", "HTML", "html", "html"),
            param("HTMLTidy", "HTMLTidy", "html_tidy", "html-tidy"),
            param(
                "HTMLTidyGenerator",
                "HTMLTidyGenerator",
                "html_tidy_generator",
                "html-tidy-generator",
            ),
            param("HTMLVersion", "HTMLVersion", "html_version", "html-version"),
            param("NoHTML", "NoHTML", "no_html", "no-html"),
            param("One   Two", "OneTwo", "one_two", "one-two"),
            param("One  Two", "OneTwo", "one_two", "one-two"),
            param("One Two", "OneTwo", "one_two", "one-two"),
            param("OneTwo", "OneTwo", "one_two", "one-two"),
            param("One_Two", "OneTwo", "one_two", "one-two"),
            param("One__Two", "OneTwo", "one_two", "one-two"),
            param("One___Two", "OneTwo", "one_two", "one-two"),
            param("Product", "Product", "product", "product"),
            param("SpecialGuest", "SpecialGuest", "special_guest", "special-guest"),
            param("Text", "Text", "text", "text"),
            param("Text123", "Text123", "text123", "text123"),
            param(
                "Text123Text456", "Text123Text456", "text123_text456", "text123-text456"
            ),
            param("_APIResponse_", "APIResponse", "_api_response_", "-api-response-"),
            param("_API_", "API", "_api_", "-api-"),
            param("__APIResponse__", "APIResponse", "_api_response_", "-api-response-"),
            param("__API__", "API", "_api_", "-api-"),
            param(
                "__impliedVolatility_",
                "ImpliedVolatility",
                "_implied_volatility_",
                "-implied-volatility-",
            ),
            param("_itemID", "ItemID", "_item_id", "-item-id"),
            param("_lastPrice__", "LastPrice", "_last_price_", "-last-price-"),
            param("_symbol", "Symbol", "_symbol", "-symbol"),
            param("aB", "AB", "a_b", "a-b"),
            param("changePct", "ChangePct", "change_pct", "change-pct"),
            param("changePct_", "ChangePct", "change_pct_", "change-pct-"),
            param(
                "impliedVolatility",
                "ImpliedVolatility",
                "implied_volatility",
                "implied-volatility",
            ),
            param("lastPrice", "LastPrice", "last_price", "last-price"),
            param("memMB", "MemMB", "mem_mb", "mem-mb"),
            param("sizeX", "SizeX", "size_x", "size-x"),
            param("symbol", "Symbol", "symbol", "symbol"),
            param("testNTest", "TestNTest", "test_n_test", "test-n-test"),
            param("text", "Text", "text", "text"),
            param("text123", "Text123", "text123", "text123"),
        ],
    )
    def test_main(
        self, *, text: str, exp_pascal: str, exp_snake: str, exp_kebab: str
    ) -> None:
        assert pascal_case(text) == exp_pascal
        assert snake_case(text) == exp_snake
        assert kebab_case(text) == exp_kebab


class TestStripAndDedent:
    def test_main(self) -> None:
        result = strip_and_dedent(
            """
            This is line 1.
            This is line 2.
            """
        )
        expected = "This is line 1.\nThis is line 2.\n"
        assert result == expected


class TestUniqueStrs:
    def test_main(self) -> None:
        first, second = [unique_str() for _ in range(2)]
        assert first != second
