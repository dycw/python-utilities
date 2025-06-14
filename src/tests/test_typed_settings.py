from collections.abc import Callable
from dataclasses import dataclass
from ipaddress import IPv4Address, IPv6Address
from pathlib import Path
from typing import TypeVar

import typed_settings
from hypothesis import given
from hypothesis.strategies import DataObject, SearchStrategy, data, ip_addresses, tuples
from pytest import mark, param, raises
from typed_settings import FileLoader, TomlFormat
from whenever import (
    Date,
    DateDelta,
    DateTimeDelta,
    PlainDateTime,
    Time,
    TimeDelta,
    ZonedDateTime,
)

from utilities.hypothesis import (
    date_deltas,
    date_time_deltas,
    dates,
    plain_datetimes,
    temp_paths,
    text_ascii,
    time_deltas,
    times,
    zoned_datetimes,
)
from utilities.typed_settings import (
    ExtendedTSConverter,
    LoadSettingsError,
    load_settings,
)

app_names = text_ascii(min_size=1).map(str.lower)


_T = TypeVar("_T")


class TestExtendedTSConverter:
    @given(data=data(), root=temp_paths(), app_name=text_ascii(min_size=1))
    @mark.parametrize(
        ("test_cls", "strategy", "serialize"),
        [
            param(Date, dates(), Date.format_common_iso),
            param(DateDelta, date_deltas(parsable=True), DateDelta.format_common_iso),
            param(
                DateTimeDelta,
                date_time_deltas(parsable=True),
                DateTimeDelta.format_common_iso,
            ),
            param(IPv4Address, ip_addresses(v=4), IPv4Address),
            param(IPv6Address, ip_addresses(v=6), IPv6Address),
            param(PlainDateTime, plain_datetimes(), PlainDateTime.format_common_iso),
            param(Time, times(), Time.format_common_iso),
            param(TimeDelta, time_deltas(), TimeDelta.format_common_iso),
            param(ZonedDateTime, zoned_datetimes(), ZonedDateTime.format_common_iso),
        ],
    )
    def test_main(
        self,
        *,
        data: DataObject,
        root: Path,
        app_name: str,
        test_cls: type[_T],
        strategy: SearchStrategy[_T],
        serialize: Callable[[_T], str],
    ) -> None:
        default, value = data.draw(tuples(strategy, strategy))

        @dataclass(frozen=True, kw_only=True, slots=True)
        class Settings:
            value: test_cls = default  # pyright: ignore[reportInvalidTypeForm]

        settings_default = typed_settings.load_settings(
            Settings, loaders=[], converter=ExtendedTSConverter()
        )
        assert settings_default.value == default
        _ = hash(settings_default)
        file = Path(root, "file.toml")
        with file.open(mode="w") as fh:
            _ = fh.write(f'[{app_name}]\nvalue = "{serialize(value)}"')
        settings_loaded = typed_settings.load_settings(
            Settings,
            loaders=[
                FileLoader(formats={"*.toml": TomlFormat(app_name)}, files=[file])
            ],
            converter=ExtendedTSConverter(),
        )
        assert settings_loaded.value == value


class TestLoadSettings:
    def test_main(self) -> None:
        pass

    @mark.parametrize("app_name", [param("app_"), param("app1"), param("app__name")])
    def test_error(self, *, app_name: str) -> None:
        @dataclass(frozen=True, kw_only=True, slots=True)
        class Settings: ...

        with raises(LoadSettingsError, match="Invalid app name; got '.+'"):
            _ = load_settings(Settings, app_name)
