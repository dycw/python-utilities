import datetime as dt
from collections.abc import Callable
from pathlib import Path
from subprocess import check_call
from typing import Any, cast

from click import command, echo
from click.testing import CliRunner
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
from pytest import MonkeyPatch, mark, param, raises
from typed_settings import settings
from typed_settings.exceptions import InvalidValueError

from utilities.datetime import (
    UTC,
    serialize_date,
    serialize_datetime,
    serialize_time,
    serialize_timedelta,
)
from utilities.hypothesis import temp_paths, text_ascii
from utilities.typed_settings import (
    AppNameContainsUnderscoreError,
    _get_loaders,
    click_options,
    get_repo_root_config,
    load_settings,
)

app_names = text_ascii(min_size=1).map(str.lower)


class TestGetRepoRootConfig:
    def test_exists(self, monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
        monkeypatch.chdir(tmp_path)
        _ = check_call(["git", "init"])
        Path("config.toml").touch()
        expected = tmp_path.joinpath("config.toml")
        assert get_repo_root_config(cwd=tmp_path) == expected

    def test_does_not_exist(self, tmp_path: Path) -> None:
        assert get_repo_root_config(cwd=tmp_path) is None


class TestGetLoaders:
    def test_success(self) -> None:
        _ = _get_loaders()

    def test_error(self) -> None:
        with raises(AppNameContainsUnderscoreError):
            _ = _get_loaders(appname="app_name")


class TestLoadSettings:
    @given(data=data(), appname=app_names, root=temp_paths())
    @mark.parametrize(
        ("cls", "strategy"),
        [
            param(dt.date, dates()),
            param(dt.datetime, datetimes(timezones=just(UTC))),
            param(dt.time, times()),
            param(dt.timedelta, timedeltas()),
        ],
    )
    def test_main(
        self,
        data: DataObject,
        appname: str,
        root: Path,
        cls: Any,
        strategy: SearchStrategy[Any],
    ) -> None:
        default, value = data.draw(tuples(strategy, strategy))

        @settings(frozen=True)
        class Settings:
            value: cls = default

        settings_default = load_settings(Settings)
        assert settings_default.value == default
        file = root.joinpath("file.toml")
        with file.open(mode="w") as fh:
            _ = fh.write(f'[{appname}]\nvalue = "{value}"')
        settings_loaded = load_settings(Settings, appname=appname, config_files=[file])
        assert settings_loaded.value == value

    @given(appname=app_names)
    @mark.parametrize("cls", [param(dt.date), param(dt.time), param(dt.timedelta)])
    def test_errors(self, appname: str, cls: Any) -> None:
        @settings(frozen=True)
        class Settings:
            value: cls = cast(Any, None)

        with raises(InvalidValueError):
            _ = load_settings(Settings, appname=appname)


class TestClickOptions:
    @given(data=data(), appname=app_names, root=temp_paths())
    @mark.parametrize(
        ("cls", "strategy", "serialize"),
        [
            param(dt.date, dates(), serialize_date),
            param(dt.datetime, datetimes(timezones=just(UTC)), serialize_datetime),
            param(dt.time, times(), serialize_time),
            param(dt.timedelta, timedeltas(), serialize_timedelta),
        ],
    )
    def test_main(
        self,
        data: DataObject,
        appname: str,
        root: Path,
        cls: Any,
        strategy: SearchStrategy[Any],
        serialize: Callable[[Any], str],
    ) -> None:
        default, val, cfg = data.draw(tuples(strategy, strategy, strategy))
        val_str, cfg_str = map(serialize, [val, cfg])
        runner = CliRunner()

        @settings(frozen=True)
        class Settings:
            value: cls = default

        @command()
        @click_options(Settings, appname=appname)
        def cli1(settings: Settings, /) -> None:
            echo(f"value = {serialize(settings.value)}")

        result = runner.invoke(cli1)
        assert result.exit_code == 0
        assert result.stdout == f"value = {serialize(default)}\n"

        result = runner.invoke(cli1, f'--value="{val_str}"')
        assert result.exit_code == 0
        assert result.stdout == f"value = {val_str}\n"

        file = root.joinpath("file.toml")
        with file.open(mode="w") as fh:
            _ = fh.write(f'[{appname}]\nvalue = "{cfg_str}"')

        @command()
        @click_options(Settings, appname=appname, config_files=[file])
        def cli2(settings: Settings, /) -> None:
            echo(f"value = {serialize(settings.value)}")

        result = runner.invoke(cli2)
        assert result.exit_code == 0
        assert result.stdout == f"value = {cfg_str}\n"

        result = runner.invoke(cli1, f'--value="{val_str}"')
        assert result.exit_code == 0
        assert result.stdout == f"value = {val_str}\n"
