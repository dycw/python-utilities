from collections.abc import Callable
from dataclasses import dataclass
from ipaddress import IPv4Address, IPv6Address
from pathlib import Path
from typing import Any, ClassVar, Self, assert_never, override
from uuid import UUID

import typed_settings
from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    SearchStrategy,
    booleans,
    data,
    integers,
    ip_addresses,
    sampled_from,
    tuples,
    uuids,
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
    plain_date_times,
    temp_paths,
    text_ascii,
    time_deltas,
    times,
    year_months,
    zoned_date_times,
)
from utilities.os import temp_environ
from utilities.re import extract_group
from utilities.sentinel import Sentinel, sentinel
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
        _Case(cls=Date, strategy=dates(), serialize=str),
        _Case(cls=DateDelta, strategy=date_deltas(parsable=True), serialize=str),
        _Case(
            cls=DateTimeDelta, strategy=date_time_deltas(parsable=True), serialize=str
        ),
        _Case(cls=IPv4Address, strategy=ip_addresses(v=4), serialize=str),
        _Case(cls=IPv6Address, strategy=ip_addresses(v=6), serialize=str),
        _Case(cls=MonthDay, strategy=month_days(), serialize=str),
        _Case(cls=PlainDateTime, strategy=plain_date_times(), serialize=str),
        _Case(cls=Time, strategy=times(), serialize=str),
        _Case(cls=TimeDelta, strategy=time_deltas(), serialize=str),
        _Case(cls=UUID, strategy=uuids(), serialize=str),
        _Case(cls=YearMonth, strategy=year_months(), serialize=str),
        _Case(cls=ZonedDateTime, strategy=zoned_date_times(), serialize=str),
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
            case never:
                assert_never(never)
        assert settings.value == expected


class TestLoadSettings:
    @given(root=temp_paths(), date_time=zoned_date_times(), app_name=app_names)
    def test_main(self, *, root: Path, date_time: ZonedDateTime, app_name: str) -> None:
        @dataclass(frozen=True, kw_only=True, slots=True)
        class Settings:
            date_time: ZonedDateTime

        file = root.joinpath("settings.toml")
        _ = file.write_text(
            strip_and_dedent(f"""
                [{app_name}]
                date_time = {str(date_time)!r}
            """)
        )
        settings = load_settings(Settings, app_name, start_dir=root)
        assert settings.date_time == date_time

    @given(
        prefix=app_names.map(lambda text: f"TEST_{text}".upper()),
        date_time=zoned_date_times(),
        app_name=app_names,
    )
    def test_loaders(
        self, *, prefix: str, date_time: ZonedDateTime, app_name: str
    ) -> None:
        key = f"{prefix}__DATE_TIME"

        @dataclass(frozen=True, kw_only=True, slots=True)
        class Settings:
            date_time: ZonedDateTime

        with temp_environ({key: str(date_time)}):
            settings = load_settings(
                Settings, app_name, loaders=[EnvLoader(prefix=f"{prefix}__")]
            )
        assert settings.date_time == date_time

    @given(root=temp_paths(), app_name=app_names)
    def test_converter_simple(self, *, root: Path, app_name: str) -> None:
        @dataclass(frozen=True, kw_only=True, slots=True)
        class Settings:
            sentinel: Sentinel

        file = root.joinpath("settings.toml")
        _ = file.write_text(
            strip_and_dedent(f"""
                [{app_name}]
                sentinel = 'sentinel'
            """)
        )

        def convert(text: str, /) -> Sentinel:
            if text == "sentinel":
                return sentinel
            raise ValueError

        settings = load_settings(
            Settings, app_name, start_dir=root, converters=[(Sentinel, convert)]
        )
        assert settings.sentinel is sentinel

    @given(data=data(), root=temp_paths(), n=integers(), app_name=app_names)
    def test_converter_dataclass(
        self, *, data: DataObject, root: Path, n: int, app_name: str
    ) -> None:
        @dataclass(repr=False, frozen=True, kw_only=True, slots=True)
        class Left:
            x: int

            @override
            def __str__(self) -> str:
                return f"left{self.x}"

            @classmethod
            def parse(cls, text: str, /) -> Self:
                x = extract_group(r"^left(.+?)$", text)
                return cls(x=int(x))

        @dataclass(frozen=True, kw_only=True, slots=True)
        class Right:
            y: int

            @override
            def __str__(self) -> str:
                return f"right{self.y}"

            @classmethod
            def parse(cls, text: str, /) -> Self:
                y = extract_group(r"^right(.+?)$", text)
                return cls(y=int(y))

        value = data.draw(sampled_from([Left(x=n), Right(y=n)]))

        @dataclass(frozen=True, kw_only=True, slots=True)
        class Settings:
            inner: Left | Right

        file = root.joinpath("settings.toml")
        _ = file.write_text(
            strip_and_dedent(f"""
                [{app_name}]
                inner = {str(value)!r}
            """)
        )
        settings = load_settings(
            Settings,
            app_name,
            start_dir=root,
            converters=[(Left, Left.parse), (Right, Right.parse)],
        )
        assert settings.inner == value

    @mark.parametrize("app_name", [param("app_"), param("app1"), param("app__name")])
    def test_error(self, *, app_name: str) -> None:
        @dataclass(frozen=True, kw_only=True, slots=True)
        class Settings: ...

        with raises(LoadSettingsError, match="Invalid app name; got '.+'"):
            _ = load_settings(Settings, app_name)
