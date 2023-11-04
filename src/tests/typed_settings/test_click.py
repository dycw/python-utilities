import datetime as dt
import enum
from collections.abc import Callable
from dataclasses import dataclass
from enum import auto
from operator import attrgetter
from pathlib import Path
from typing import TypeVar

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
from pytest import mark, param

from tests.typed_settings.test_typed_settings import app_names
from utilities.datetime import (
    UTC,
    serialize_date,
    serialize_datetime,
    serialize_time,
    serialize_timedelta,
)
from utilities.hypothesis import temp_paths
from utilities.typed_settings import click_field, click_options

_T = TypeVar("_T")


class TestClickOptions:
    @given(data=data(), appname=app_names, root=temp_paths())
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
        appname: str,
        root: Path,
        test_cls: type[_T],
        strategy: SearchStrategy[_T],
        serialize: Callable[[_T], str],
    ) -> None:
        default, value, cfg = data.draw(tuples(strategy, strategy, strategy))
        self.run_test(test_cls, default, appname, serialize, root, value, cfg)

    @given(data=data(), appname=app_names, root=temp_paths())
    def test_enum(self, *, data: DataObject, appname: str, root: Path) -> None:
        class Truth(enum.Enum):
            true = auto()
            false = auto()

        strategy = sampled_from(Truth)
        default, value, cfg = data.draw(tuples(strategy, strategy, strategy))
        self.run_test(Truth, default, appname, attrgetter("name"), root, value, cfg)

    @staticmethod
    def run_test(
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

        file = root.joinpath("file.toml")
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
