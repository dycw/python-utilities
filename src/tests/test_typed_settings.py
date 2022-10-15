import datetime as dt
from collections.abc import Callable
from pathlib import Path
from typing import Any
from typing import cast

from click import command
from click import echo
from click.testing import CliRunner
from hypothesis import given
from hypothesis.strategies import DataObject
from hypothesis.strategies import SearchStrategy
from hypothesis.strategies import data
from hypothesis.strategies import dates
from hypothesis.strategies import datetimes
from hypothesis.strategies import timedeltas
from hypothesis.strategies import times
from hypothesis.strategies import tuples
from pytest import mark
from pytest import param
from pytest import raises
from typed_settings import settings
from typed_settings.exceptions import InvalidValueError

from utilities.datetime import serialize_date
from utilities.datetime import serialize_datetime
from utilities.datetime import serialize_time
from utilities.datetime import serialize_timedelta
from utilities.hypothesis import temp_paths
from utilities.typed_settings import click_options
from utilities.typed_settings import load_settings


class TestLoadSettings:
    @given(data=data(), root=temp_paths())
    @mark.parametrize(
        ["cls", "strategy"],
        [
            param(dt.date, dates()),
            param(dt.datetime, datetimes()),
            param(dt.time, times()),
            param(dt.timedelta, timedeltas()),
        ],
    )
    def test_main(
        self,
        data: DataObject,
        root: Path,
        cls: Any,
        strategy: SearchStrategy[Any],
    ) -> None:
        default, value = data.draw(tuples(strategy, strategy))

        @settings(frozen=True)
        class Settings:
            value: cls = default

        settings_default = load_settings(Settings, "appname")
        assert settings_default.value == default
        with open(file := root.joinpath("file.toml"), mode="w") as fh:
            _ = fh.write(f'[appname]\nvalue = "{value}"')
        settings_loaded = load_settings(
            Settings, "appname", config_files=[file]
        )
        assert settings_loaded.value == value

    @mark.parametrize(
        "cls", [param(dt.date), param(dt.time), param(dt.timedelta)]
    )
    def test_errors(self, cls: Any) -> None:
        @settings(frozen=True)
        class Settings:
            value: cls = cast(Any, None)

        with raises(InvalidValueError):
            _ = load_settings(Settings, "appname")


class TestClickOptions:
    @given(data=data(), root=temp_paths())
    @mark.parametrize(
        ["cls", "strategy", "serialize"],
        [
            param(dt.date, dates(), serialize_date),
            param(dt.datetime, datetimes(), serialize_datetime),
            param(dt.time, times(), serialize_time),
            param(dt.timedelta, timedeltas(), serialize_timedelta),
        ],
    )
    def test_main(
        self,
        data: DataObject,
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
        @click_options(Settings, "appname")
        def cli1(settings: Settings, /) -> None:
            echo(f"value = {serialize(settings.value)}")

        result = runner.invoke(cli1)
        assert result.exit_code == 0
        assert result.stdout == f"value = {serialize(default)}\n"

        result = runner.invoke(cli1, f'--value="{val_str}"')
        assert result.exit_code == 0
        assert result.stdout == f"value = {val_str}\n"

        with open(file := root.joinpath("file.toml"), mode="w") as fh:
            _ = fh.write(f'[appname]\nvalue = "{cfg_str}"')

        @command()
        @click_options(Settings, "appname", config_files=[file])
        def cli2(settings: Settings, /) -> None:
            echo(f"value = {serialize(settings.value)}")

        result = runner.invoke(cli2)
        assert result.exit_code == 0
        assert result.stdout == f"value = {cfg_str}\n"

        result = runner.invoke(cli1, f'--value="{val_str}"')
        assert result.exit_code == 0
        assert result.stdout == f"value = {val_str}\n"
