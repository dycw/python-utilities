from __future__ import annotations

import datetime as dt
import enum
from collections.abc import Callable
from enum import auto
from typing import Any

from click import ParamType, argument, command, echo, option
from click.testing import CliRunner
from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    SearchStrategy,
    data,
    dates,
    datetimes,
    just,
    sampled_from,
    timedeltas,
    times,
)
from pytest import mark, param

import utilities.click
from utilities.click import Date, DateTime, Time, Timedelta, log_level_option
from utilities.datetime import (
    UTC,
    serialize_date,
    serialize_datetime,
    serialize_time,
    serialize_timedelta,
)
from utilities.logging import LogLevel


class TestParameters:
    cases = (
        param(Date(), dt.date, dates(), serialize_date),
        param(
            DateTime(), dt.datetime, datetimes(timezones=just(UTC)), serialize_datetime
        ),
        param(Time(), dt.time, times(), serialize_time),
        param(Timedelta(), dt.timedelta, timedeltas(), serialize_timedelta),
    )

    @given(data=data())
    @mark.parametrize(("param", "cls", "strategy", "serialize"), cases)
    def test_argument(
        self,
        *,
        data: DataObject,
        param: ParamType,
        cls: Any,
        strategy: SearchStrategy[Any],
        serialize: Callable[[Any], str],
    ) -> None:
        runner = CliRunner()

        @command()
        @argument("value", type=param)
        def cli(*, value: cls) -> None:
            echo(f"value = {serialize(value)}")

        value_str = serialize(data.draw(strategy))
        result = CliRunner().invoke(cli, [value_str])
        assert result.exit_code == 0
        assert result.stdout == f"value = {value_str}\n"

        result = runner.invoke(cli, ["error"])
        assert result.exit_code == 2

    @given(data=data())
    @mark.parametrize(("param", "cls", "strategy", "serialize"), cases)
    def test_option(
        self,
        *,
        data: DataObject,
        param: ParamType,
        cls: Any,
        strategy: SearchStrategy[Any],
        serialize: Callable[[Any], str],
    ) -> None:
        value = data.draw(strategy)

        @command()
        @option("--value", type=param, default=value)
        def cli(*, value: cls) -> None:
            echo(f"value = {serialize(value)}")

        result = CliRunner().invoke(cli)
        assert result.exit_code == 0
        assert result.stdout == f"value = {serialize(value)}\n"


class _Truth(enum.Enum):
    true = auto()
    false = auto()


class TestEnum:
    @given(truth=sampled_from(_Truth))
    def test_command(self, *, truth: _Truth) -> None:
        @command()
        @argument("truth", type=utilities.click.Enum(_Truth))
        def cli(*, truth: _Truth) -> None:
            echo(f"truth = {truth}")

        result = CliRunner().invoke(cli, [truth.name])
        assert result.exit_code == 0
        assert result.stdout == f"truth = {truth}\n"

        result = CliRunner().invoke(cli, ["not_an_element"])
        assert result.exit_code == 2

    @given(data=data(), truth=sampled_from(_Truth))
    def test_case_insensitive(self, *, data: DataObject, truth: _Truth) -> None:
        @command()
        @argument("truth", type=utilities.click.Enum(_Truth, case_sensitive=False))
        def cli(*, truth: _Truth) -> None:
            echo(f"truth = {truth}")

        name = truth.name
        as_str = data.draw(sampled_from([name, name.lower()]))
        result = CliRunner().invoke(cli, [as_str])
        assert result.exit_code == 0
        assert result.stdout == f"truth = {truth}\n"

    @given(truth=sampled_from(_Truth))
    def test_option(self, *, truth: _Truth) -> None:
        @command()
        @option("--truth", type=utilities.click.Enum(_Truth), default=truth)
        def cli(*, truth: _Truth) -> None:
            echo(f"truth = {truth}")

        result = CliRunner().invoke(cli)
        assert result.exit_code == 0
        assert result.stdout == f"truth = {truth}\n"


class TestLogLevelOption:
    @given(log_level=sampled_from(LogLevel))
    def test_main(self, *, log_level: LogLevel) -> None:
        @command()
        @log_level_option
        def cli(*, log_level: LogLevel) -> None:
            echo(f"log_level = {log_level}")

        result = CliRunner().invoke(cli, ["--log-level", log_level.name])
        assert result.exit_code == 0
        assert result.stdout == f"log_level = {log_level}\n"
