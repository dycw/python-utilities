from __future__ import annotations

from hypothesis import given
from hypothesis.strategies import sampled_from

from utilities.humps import snake_case


class TestSnakeCase:
    @given(
        case=sampled_from([
            ("API", "api"),
            ("APIResponse", "api_response"),
            ("ApplicationController", "application_controller"),
            ("Area51Controller", "area51_controller"),
            ("FreeBSD", "free_bsd"),
            ("HTML", "html"),
            ("HTMLTidy", "html_tidy"),
            ("HTMLTidyGenerator", "html_tidy_generator"),
            ("HTMLVersion", "html_version"),
            ("NoHTML", "no_html"),
            ("One   Two", "one_two"),
            ("One  Two", "one_two"),
            ("One Two", "one_two"),
            ("OneTwo", "one_two"),
            ("One_Two", "one_two"),
            ("One__Two", "one_two"),
            ("One___Two", "one_two"),
            ("Product", "product"),
            ("SpecialGuest", "special_guest"),
            ("Text", "text"),
            ("Text123", "text123"),
            ("_APIResponse_", "_api_response_"),
            ("_API_", "_api_"),
            ("__APIResponse__", "_api_response_"),
            ("__API__", "_api_"),
            ("__impliedVolatility_", "_implied_volatility_"),
            ("_itemID", "_item_id"),
            ("_lastPrice__", "_last_price_"),
            ("_symbol", "_symbol"),
            ("aB", "a_b"),
            ("changePct", "change_pct"),
            ("changePct_", "change_pct_"),
            ("impliedVolatility", "implied_volatility"),
            ("lastPrice", "last_price"),
            ("memMB", "mem_mb"),
            ("sizeX", "size_x"),
            ("symbol", "symbol"),
            ("testNTest", "test_n_test"),
            ("text", "text"),
            ("text123", "text123"),
        ])
    )
    def test_main(self, *, case: tuple[str, str]) -> None:
        text, expected = case
        result = snake_case(text)
        assert result == expected
