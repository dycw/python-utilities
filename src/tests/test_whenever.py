from __future__ import annotations


class TestParseTimedelta:
    @given(timedelta=timedeltas())
    def test_main(self, *, timedelta: dt.timedelta) -> None:
        result = parse_timedelta(str(timedelta))
        assert result == timedelta

    def test_error(self) -> None:
        with raises(
            ParseTimedeltaError, match="Unable to parse timedelta; got 'error'"
        ):
            _ = parse_timedelta("error")
