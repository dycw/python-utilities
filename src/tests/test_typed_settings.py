from collections.abc import Callable
from dataclasses import dataclass
from operator import eq
from pathlib import Path
from typing import TypeVar

from hypothesis import given
from hypothesis.strategies import DataObject, SearchStrategy, data, tuples
from pytest import mark, param
from typed_settings import FileLoader, TomlFormat, load_settings
from whenever import Date, DateDelta, PlainDateTime, Time, TimeDelta, ZonedDateTime

from utilities.hypothesis import (
    date_deltas_whenever,
    dates_whenever,
    plain_datetimes_whenever,
    temp_paths,
    text_ascii,
    time_deltas_whenever,
    times_whenever,
    zoned_datetimes_whenever,
)
from utilities.typed_settings import ExtendedTSConverter

app_names = text_ascii(min_size=1).map(str.lower)


_T = TypeVar("_T")


class TestExtendedTSConverter:
    @given(data=data(), root=temp_paths(), appname=text_ascii(min_size=1))
    @mark.parametrize(
        ("test_cls", "strategy", "serialize"),
        [
            param(Date, dates_whenever(), Date.format_common_iso),
            param(
                DateDelta,
                date_deltas_whenever(parsable=True),
                DateDelta.format_common_iso,
            ),
            param(
                PlainDateTime,
                plain_datetimes_whenever(),
                PlainDateTime.format_common_iso,
            ),
            param(Time, times_whenever(), Time.format_common_iso),
            param(TimeDelta, time_deltas_whenever(), TimeDelta.format_common_iso),
            param(
                ZonedDateTime,
                zoned_datetimes_whenever(),
                ZonedDateTime.format_common_iso,
            ),
        ],
    )
    def test_main(
        self,
        *,
        data: DataObject,
        root: Path,
        appname: str,
        test_cls: type[_T],
        strategy: SearchStrategy[_T],
        serialize: Callable[[_T], str],
    ) -> None:
        default, value = data.draw(tuples(strategy, strategy))
        self._run_test(test_cls, default, root, appname, serialize, value, eq)

    def _run_test(
        self,
        test_cls: type[_T],
        default: _T,
        root: Path,
        appname: str,
        serialize: Callable[[_T], str],
        value: _T,
        equal: Callable[[_T, _T], bool],
        /,
    ) -> None:
        @dataclass(frozen=True, kw_only=True, slots=True)
        class Settings:
            value: test_cls = default  # pyright: ignore[reportInvalidTypeForm]

        settings_default = load_settings(
            Settings, loaders=[], converter=ExtendedTSConverter()
        )
        assert settings_default.value == default
        _ = hash(settings_default)
        file = Path(root, "file.toml")
        with file.open(mode="w") as fh:
            _ = fh.write(f'[{appname}]\nvalue = "{serialize(value)}"')
        settings_loaded = load_settings(
            Settings,
            loaders=[FileLoader(formats={"*.toml": TomlFormat(appname)}, files=[file])],
            converter=ExtendedTSConverter(),
        )
        assert equal(settings_loaded.value, value)
