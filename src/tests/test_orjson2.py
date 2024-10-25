from __future__ import annotations

from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import dictionaries, lists

from utilities.hypothesis import text_ascii, zoned_datetimes
from utilities.orjson2 import deserialize2, serialize2

if TYPE_CHECKING:
    import datetime as dt


class TestSerialize2:
    @given(datetime=zoned_datetimes())
    def test_main(self, *, datetime: dt.datetime) -> None:
        result = deserialize2(serialize2(datetime))
        assert result == datetime

    @given(datetimes=lists(zoned_datetimes()))
    def test_list_of_dates(self, *, datetimes: list[dt.datetime]) -> None:
        result = deserialize2(serialize2(datetimes))
        assert result == datetimes

    @given(datetimes=dictionaries(text_ascii(), zoned_datetimes()))
    def test_dict_of_dates(self, *, datetimes: dict[str, dt.datetime]) -> None:
        result = deserialize2(serialize2(datetimes))
        assert result == datetimes

    @given(datetimes=lists(dictionaries(text_ascii(), zoned_datetimes())))
    def test_list_of_dict_of_dates(
        self, *, datetimes: list[dict[str, dt.datetime]]
    ) -> None:
        result = deserialize2(serialize2(datetimes))
        assert result == datetimes

    @given(datetimes=dictionaries(text_ascii(), lists(zoned_datetimes())))
    def test_dict_of_list_of_dates(
        self, *, datetimes: dict[str, list[dt.datetime]]
    ) -> None:
        result = deserialize2(serialize2(datetimes))
        assert result == datetimes
