import datetime as dt
from collections.abc import Callable
from dataclasses import dataclass
from operator import eq
from pathlib import Path
from typing import Any, TypeVar

from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    SearchStrategy,
    data,
    dates,
    datetimes,
    just,
    timedeltas,
    times,
    tuples,
)
from pytest import mark, param, raises
from typed_settings.exceptions import InvalidSettingsError

from utilities.datetime import (
    UTC,
    serialize_date,
    serialize_datetime,
    serialize_time,
    serialize_timedelta,
)
from utilities.hypothesis import temp_paths, text_ascii
from utilities.typed_settings import AppNameContainsUnderscoreError, load_settings
from utilities.typed_settings.typed_settings import _get_loaders

app_names = text_ascii(min_size=1).map(str.lower)


class TestGetLoaders:
    def test_success(self) -> None:
        _ = _get_loaders()

    def test_error(self) -> None:
        with raises(AppNameContainsUnderscoreError):
            _ = _get_loaders(appname="app_name")


_T = TypeVar("_T")


class TestLoadSettings:
    @given(data=data(), root=temp_paths(), appname=app_names)
    @mark.parametrize(
        ("test_cls", "strategy", "serialize"),
        [
            param(dt.date, dates(), serialize_date),
            param(dt.datetime, datetimes(timezones=just(UTC)), serialize_datetime),
            param(dt.time, times(), serialize_time),
            param(dt.timedelta, timedeltas(), serialize_timedelta),
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
        self.run_test(test_cls, default, root, appname, serialize, value, eq)

    @staticmethod
    def run_test(
        test_cls: type[_T],
        default: _T,
        root: Path,
        appname: str,
        serialize: Callable[[_T], str],
        value: _T,
        equal: Callable[[_T, _T], bool],
        /,
    ) -> None:
        @dataclass(frozen=True)
        class Settings:
            value: test_cls = default

        settings_default = load_settings(Settings)
        assert settings_default.value == default
        _ = hash(settings_default)
        file = root.joinpath("file.toml")
        with file.open(mode="w") as fh:
            _ = fh.write(f'[{appname}]\nvalue = "{serialize(value)}"')
        settings_loaded = load_settings(Settings, appname=appname, config_files=[file])
        assert equal(settings_loaded.value, value)

    @given(appname=app_names)
    @mark.parametrize("cls", [param(dt.date), param(dt.time), param(dt.timedelta)])
    def test_errors(self, *, appname: str, cls: Any) -> None:
        @dataclass(frozen=True)
        class Settings:
            value: cls = None

        with raises(InvalidSettingsError):
            _ = load_settings(Settings, appname=appname)
