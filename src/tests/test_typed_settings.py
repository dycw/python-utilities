import datetime as dt
import enum
from collections.abc import Callable
from dataclasses import dataclass
from enum import auto
from operator import attrgetter, eq
from pathlib import Path
from typing import Any, TypeVar

from click import command, echo
from click.testing import CliRunner
from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    SearchStrategy,
    data,
    dates,
    datetimes,
    integers,
    just,
    sampled_from,
    timedeltas,
    times,
    tuples,
)
from pytest import mark, param, raises
from sqlalchemy import Engine
from typed_settings.exceptions import InvalidSettingsError

from tests.conftest import FLAKY
from utilities.datetime import (
    serialize_date,
    serialize_datetime,
    serialize_time,
    serialize_timedelta,
)
from utilities.hypothesis import sqlite_engines, temp_paths, text_ascii
from utilities.pathlib import ensure_path
from utilities.pytest import skipif_windows
from utilities.sqlalchemy import serialize_engine
from utilities.typed_settings import (
    _get_loaders,
    _GetLoadersError,
    click_field,
    click_options,
    load_settings,
)
from utilities.zoneinfo import UTC

app_names = text_ascii(min_size=1).map(str.lower)


_T = TypeVar("_T")


class TestClickField:
    @given(default=integers(), appname=app_names, value=integers())
    def test_main(self, *, default: int, appname: str, value: int) -> None:
        @dataclass(frozen=True)
        class Config:
            num: int = click_field(default=default, param_decls=("-n", "--num"))

        @command()
        @click_options(Config, appname=appname)
        def cli(config: Config, /) -> None:
            echo(f"num = {config.num}")

        runner = CliRunner()
        result = runner.invoke(cli)
        assert result.exit_code == 0
        assert result.stdout == f"num = {default}\n"

        result = runner.invoke(cli, f"-n{value}")
        assert result.exit_code == 0
        assert result.stdout == f"num = {value}\n"

        result = runner.invoke(cli, f"--num={value}")
        assert result.exit_code == 0
        assert result.stdout == f"num = {value}\n"


class TestClickOptions:
    @given(data=data(), appname=app_names, root=temp_paths())
    @mark.parametrize(
        ("test_cls", "strategy", "serialize"),
        [
            param(dt.date, dates(), serialize_date),
            param(dt.datetime, datetimes(timezones=just(UTC)), serialize_datetime),
            param(dt.time, times(), serialize_time),
            param(dt.timedelta, timedeltas(), serialize_timedelta),
            param(Engine, sqlite_engines(), serialize_engine, marks=skipif_windows),
        ],
    )
    def test_main(
        self,
        *,
        data: DataObject,
        appname: str,
        root: Path,
        test_cls: type[_T],
        strategy: SearchStrategy[_T],
        serialize: Callable[[_T], str],
    ) -> None:
        default, value, cfg = data.draw(tuples(strategy, strategy, strategy))
        self._run_test(test_cls, default, appname, serialize, root, value, cfg)

    @FLAKY
    @given(data=data(), appname=app_names, root=temp_paths())
    def test_enum(self, *, data: DataObject, appname: str, root: Path) -> None:
        class Truth(enum.Enum):
            true = auto()
            false = auto()

        strategy = sampled_from(Truth)
        default, value, cfg = data.draw(tuples(strategy, strategy, strategy))
        self._run_test(Truth, default, appname, attrgetter("name"), root, value, cfg)

    def _run_test(
        self,
        test_cls: type[_T],
        default: _T,
        appname: str,
        serialize: Callable[[_T], str],
        root: Path,
        value: _T,
        cfg: _T,
        /,
    ) -> None:
        @dataclass(frozen=True)
        class Config:
            value: test_cls = default

        @command()
        @click_options(Config, appname=appname)
        def cli1(config: Config, /) -> None:
            echo(f"value = {serialize(config.value)}")

        runner = CliRunner()
        result = runner.invoke(cli1)
        assert result.exit_code == 0
        assert result.stdout == f"value = {serialize(default)}\n"

        val_str = serialize(value)
        result = runner.invoke(cli1, f'--value="{val_str}"')
        assert result.exit_code == 0
        assert result.stdout == f"value = {val_str}\n"

        file = ensure_path(root, "file.toml")
        cfg_str = serialize(cfg)
        with file.open(mode="w") as fh:
            _ = fh.write(f'[{appname}]\nvalue = "{cfg_str}"')

        @command()
        @click_options(Config, appname=appname, config_files=[file])
        def cli2(config: Config, /) -> None:
            echo(f"value = {serialize(config.value)}")

        result = runner.invoke(cli2)
        assert result.exit_code == 0
        assert result.stdout == f"value = {cfg_str}\n"

        result = runner.invoke(cli1, f'--value="{val_str}"')
        assert result.exit_code == 0
        assert result.stdout == f"value = {val_str}\n"


class TestGetLoaders:
    def test_success(self) -> None:
        _ = _get_loaders()

    def test_error(self) -> None:
        with raises(_GetLoadersError, match="App name .* must not contain underscores"):
            _ = _get_loaders(appname="app_name")


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
        self._run_test(test_cls, default, root, appname, serialize, value, eq)

    @given(
        default=sqlite_engines(),
        appname=app_names,
        root=temp_paths(),
        value=sqlite_engines(),
    )
    @skipif_windows  # writing \\ to file
    def test_engines(
        self, *, default: Engine, root: Path, appname: str, value: Engine
    ) -> None:
        def equal(x: Engine, y: Engine, /) -> bool:
            return x.url == y.url

        self._run_test(Engine, default, root, appname, serialize_engine, value, equal)

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
        @dataclass(frozen=True)
        class Settings:
            value: test_cls = default

        settings_default = load_settings(Settings)
        assert settings_default.value == default
        _ = hash(settings_default)
        file = ensure_path(root, "file.toml")
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
