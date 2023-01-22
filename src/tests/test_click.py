import datetime as dt
from collections.abc import Callable
from enum import Enum as _Enum
from enum import auto
from typing import Any

from click import ParamType
from click import argument
from click import command
from click import echo
from click.testing import CliRunner
from hypothesis import given
from hypothesis.strategies import DataObject
from hypothesis.strategies import SearchStrategy
from hypothesis.strategies import data
from hypothesis.strategies import dates
from hypothesis.strategies import datetimes
from hypothesis.strategies import just
from hypothesis.strategies import sampled_from
from hypothesis.strategies import timedeltas
from hypothesis.strategies import times
from pytest import mark
from pytest import param

from utilities.click import Date
from utilities.click import DateTime
from utilities.click import Enum
from utilities.click import Time
from utilities.click import Timedelta
from utilities.click import log_level_option
from utilities.datetime import UTC
from utilities.datetime import serialize_date
from utilities.datetime import serialize_datetime
from utilities.datetime import serialize_time
from utilities.datetime import serialize_timedelta
from utilities.logging import LogLevel


def runners() -> SearchStrategy[CliRunner]:
    return just(CliRunner())


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


class Truth(_Enum):
    true = auto()
    false = auto()


@command()
@argument("truth", type=Enum(Truth))
def uses_enum(*, truth: Truth) -> None:
    echo(f"truth = {truth}")


class TestEnum:
    @given(data=data(), runner=runners(), truth=sampled_from(Truth))
    def test_success(
        self,
        data: DataObject,
        runner: CliRunner,
        truth: Truth,
    ) -> None:
        name = truth.name
        as_str = data.draw(sampled_from([name, name.lower()]))
        result = runner.invoke(uses_enum, [as_str])
        assert result.exit_code == 0
        assert result.stdout == f"truth = {truth}\n"

    @given(runner=runners())
    def test_failure(self, runner: CliRunner) -> None:
        result = runner.invoke(uses_enum, ["not_an_element"])
        assert result.exit_code == 2


@command()
@log_level_option
def uses_log_level(*, log_level: LogLevel) -> None:
    echo(f"log_level = {log_level}")


class TestLogLevelOption:
    @given(runner=runners(), log_level=sampled_from(LogLevel))
    def test_main(self, runner: CliRunner, log_level: LogLevel) -> None:
        result = runner.invoke(uses_log_level, [f"--log-level={log_level}"])
        assert result.exit_code == 0
        assert result.stdout == f"log_level = {log_level}\n"
