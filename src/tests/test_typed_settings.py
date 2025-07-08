from collections.abc import Callable
from dataclasses import dataclass
from ipaddress import IPv4Address, IPv6Address
from pathlib import Path
from typing import Any, ClassVar, assert_never

import typed_settings
from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    SearchStrategy,
    booleans,
    data,
    ip_addresses,
    tuples,
)
from pytest import mark, param, raises
from typed_settings import EnvLoader, FileLoader, TomlFormat
from whenever import (
    Date,
    DateDelta,
    DateTimeDelta,
    MonthDay,
    PlainDateTime,
    Time,
    TimeDelta,
    YearMonth,
    ZonedDateTime,
)

from utilities.hypothesis import (
    date_deltas,
    date_time_deltas,
    dates,
    month_days,
    paths,
    plain_datetimes,
    temp_paths,
    text_ascii,
    time_deltas,
    times,
    year_months,
    zoned_datetimes,
)
from utilities.os import temp_environ
from utilities.text import strip_and_dedent
from utilities.typed_settings import (
    ExtendedTSConverter,
    LoadSettingsError,
    load_settings,
)

app_names = text_ascii(min_size=1).map(str.lower)


@dataclass(kw_only=True, slots=True)
class _Case[T]:
    cls: type[T]
    strategy: SearchStrategy[T]
    serialize: Callable[[T], str]


class TestExtendedTSConverter:
    cases: ClassVar[list[_Case]] = [
        _Case(cls=Date, strategy=dates(), serialize=Date.format_common_iso),
        _Case(
            cls=DateDelta,
            strategy=date_deltas(parsable=True),
            serialize=DateDelta.format_common_iso,
        ),
        _Case(
            cls=DateTimeDelta,
            strategy=date_time_deltas(parsable=True),
            serialize=DateTimeDelta.format_common_iso,
        ),
        _Case(cls=IPv4Address, strategy=ip_addresses(v=4), serialize=str),
        _Case(cls=IPv6Address, strategy=ip_addresses(v=6), serialize=str),
        _Case(
            cls=MonthDay, strategy=month_days(), serialize=MonthDay.format_common_iso
        ),
        _Case(
            cls=PlainDateTime,
            strategy=plain_datetimes(),
            serialize=PlainDateTime.format_common_iso,
        ),
        _Case(cls=Time, strategy=times(), serialize=Time.format_common_iso),
        _Case(
            cls=TimeDelta, strategy=time_deltas(), serialize=TimeDelta.format_common_iso
        ),
        _Case(
            cls=YearMonth, strategy=year_months(), serialize=YearMonth.format_common_iso
        ),
        _Case(
            cls=ZonedDateTime,
            strategy=zoned_datetimes(),
            serialize=ZonedDateTime.format_common_iso,
        ),
    ]

    @given(data=data())
    @mark.parametrize(("cls", "strategy"), [param(c.cls, c.strategy) for c in cases])
    def test_default(
        self, *, data: DataObject, cls: type[Any], strategy: SearchStrategy[Any]
    ) -> None:
        default = data.draw(strategy)

        @dataclass(frozen=True, kw_only=True, slots=True)
        class Settings:
            value: cls = default  # pyright: ignore[reportInvalidTypeForm]

        loaded = typed_settings.load_settings(
            Settings, loaders=[], converter=ExtendedTSConverter()
        )
        assert loaded.value == default

    @given(data=data(), root=temp_paths(), app_name=app_names)
    @mark.parametrize(
        ("cls", "strategy", "serialize"),
        [param(c.cls, c.strategy, c.serialize) for c in cases],
    )
    def test_loaded(
        self,
        *,
        data: DataObject,
        root: Path,
        app_name: str,
        cls: type[Any],
        strategy: SearchStrategy[Any],
        serialize: Callable[[Any], str],
    ) -> None:
        default, value = data.draw(tuples(strategy, strategy))

        @dataclass(frozen=True, kw_only=True, slots=True)
        class Settings:
            value: cls = default  # pyright: ignore[reportInvalidTypeForm]

        file = Path(root, "file.toml")
        _ = file.write_text(
            strip_and_dedent(f"""
                [{app_name}]
                value = '{serialize(value)}'
            """)
        )
        loaded = typed_settings.load_settings(
            Settings,
            loaders=[
                FileLoader(formats={"*.toml": TomlFormat(app_name)}, files=[file])
            ],
            converter=ExtendedTSConverter(),
        )
        assert loaded.value == value

    @given(
        root=temp_paths(),
        app_name=app_names,
        env_name=text_ascii(min_size=1).map(lambda text: f"TEST_{text}".upper()),
        env_value=text_ascii(min_size=1),
    )
    def test_path_env_var(
        self, *, root: str, app_name: str, env_name: str, env_value: str
    ) -> None:
        @dataclass(frozen=True, kw_only=True, slots=True)
        class Settings:
            value: Path

        file = Path(root, "file.toml")
        _ = file.write_text(
            strip_and_dedent(f"""
                [{app_name}]
                value = '${env_name}'
            """)
        )
        with temp_environ({env_name: env_value}):
            settings = typed_settings.load_settings(
                Settings,
                loaders=[
                    FileLoader(formats={"*.toml": TomlFormat(app_name)}, files=[file])
                ],
                converter=ExtendedTSConverter(resolve_paths=False),
            )
        expected = Path(env_value)
        assert settings.value == expected

    @given(root=temp_paths(), app_name=app_names, path=paths(), resolve=booleans())
    def test_path_resolution(
        self, *, root: str, app_name: str, path: Path, resolve: bool
    ) -> None:
        @dataclass(frozen=True, kw_only=True, slots=True)
        class Settings:
            value: Path

        file = Path(root, "file.toml")
        _ = file.write_text(
            strip_and_dedent(f"""
                [{app_name}]
                value = '{path!s}'
            """)
        )
        settings = typed_settings.load_settings(
            Settings,
            loaders=[
                FileLoader(formats={"*.toml": TomlFormat(app_name)}, files=[file])
            ],
            converter=ExtendedTSConverter(resolve_paths=resolve),
        )
        match resolve:
            case True:
                expected = Path.cwd().joinpath(path)
            case False:
                expected = Path(path)
            case _ as never:
                assert_never(never)
        assert settings.value == expected


class TestLoadSettings:
    @given(root=temp_paths(), datetime=zoned_datetimes())
    def test_main(self, *, root: Path, datetime: ZonedDateTime) -> None:
        @dataclass(frozen=True, kw_only=True, slots=True)
        class Settings:
            datetime: ZonedDateTime

        file = Path(root, "file.toml")
        _ = file.write_text("")
        _ = file.write_text(
            strip_and_dedent(f"""
                [app_name]
                datetime = '{datetime.format_common_iso()}'
            """)
        )
        settings = load_settings(
            Settings, "app_name", filenames="file.toml", start_dir=root
        )
        assert settings.datetime == datetime

    @given(
        prefix=app_names.map(lambda text: f"TEST_{text}".upper()),
        datetime=zoned_datetimes(),
    )
    def test_loaders(self, *, prefix: str, datetime: ZonedDateTime) -> None:
        key = f"{prefix}__DATETIME"

        @dataclass(frozen=True, kw_only=True, slots=True)
        class Settings:
            datetime: ZonedDateTime

        with temp_environ({key: datetime.format_common_iso()}):
            settings = load_settings(
                Settings, "app_name", loaders=[EnvLoader(prefix=f"{prefix}__")]
            )
        assert settings.datetime == datetime

    @mark.parametrize("app_name", [param("app_"), param("app1"), param("app__name")])
    def test_error(self, *, app_name: str) -> None:
        @dataclass(frozen=True, kw_only=True, slots=True)
        class Settings: ...

        with raises(LoadSettingsError, match="Invalid app name; got '.+'"):
            _ = load_settings(Settings, app_name)
