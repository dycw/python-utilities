import datetime as dt
import enum
from collections.abc import Callable
from enum import auto
from typing import Any

from click import ParamType, argument, command, echo
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
from utilities.click import (
    Date,
    DateTime,
    Time,
    Timedelta,
    log_level_option,
)
from utilities.datetime import (
    UTC,
    serialize_date,
    serialize_datetime,
    serialize_time,
    serialize_timedelta,
)
from utilities.logging import LogLevel


class TestParameters:
    @given(data=data())
    @mark.parametrize(
        ("param", "cls", "strategy", "serialize"),
        [
            param(Date(), dt.date, dates(), serialize_date),
            param(
                DateTime(),
                dt.datetime,
                datetimes(timezones=just(UTC)),
                serialize_datetime,
            ),
            param(Time(), dt.time, times(), serialize_time),
            param(Timedelta(), dt.timedelta, timedeltas(), serialize_timedelta),
        ],
    )
    def test_success(
        self,
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
        result = runner.invoke(cli, [value_str])
        assert result.exit_code == 0
        assert result.stdout == f"value = {value_str}\n"

        result = runner.invoke(cli, ["error"])
        assert result.exit_code == 2


class TestEnum:
    class Truth(enum.Enum):
        true = auto()
        false = auto()

    @command()
    @argument("truth", type=utilities.click.Enum(Truth))
    def cli(*, truth: Truth) -> None:
        echo(f"truth = {truth}")

    @given(truth=sampled_from(Truth))
    def test_success(self, truth: Truth) -> None:
        result = CliRunner().invoke(self.cli, [truth.name])
        assert result.exit_code == 0
        assert result.stdout == f"truth = {truth}\n"

    def test_failure(self) -> None:
        result = CliRunner().invoke(self.cli, ["not_an_element"])
        assert result.exit_code == 2

    @given(data=data(), truth=sampled_from(Truth))
    def test_success_insensitive(self, data: DataObject, truth: Truth) -> None:
        Truth = self.Truth  # noqa: N806

        @command()
        @argument(
            "truth",
            type=utilities.click.Enum(Truth, case_sensitive=False),
        )
        def cli(*, truth: Truth) -> None:
            echo(f"truth = {truth}")

        name = truth.name
        as_str = data.draw(sampled_from([name, name.lower()]))
        result = CliRunner().invoke(cli, [as_str])
        assert result.exit_code == 0
        assert result.stdout == f"truth = {truth}\n"


class TestLogLevelOption:
    @given(log_level=sampled_from(LogLevel))
    def test_main(self, log_level: LogLevel) -> None:
        @command()
        @log_level_option
        def cli(*, log_level: LogLevel) -> None:
            echo(f"log_level = {log_level}")

        result = CliRunner().invoke(cli, ["--log-level", log_level.name])
        assert result.exit_code == 0
        assert result.stdout == f"log_level = {log_level}\n"
