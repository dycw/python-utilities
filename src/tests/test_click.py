import datetime as dt
from collections.abc import Callable
from enum import Enum as _Enum
from enum import auto

from click import argument
from click import command
from click import echo
from click.testing import CliRunner
from hypothesis import assume
from hypothesis import given
from hypothesis.strategies import DataObject
from hypothesis.strategies import SearchStrategy
from hypothesis.strategies import data
from hypothesis.strategies import dates
from hypothesis.strategies import datetimes
from hypothesis.strategies import just
from hypothesis.strategies import sampled_from
from pytest import mark
from pytest import param

from utilities.click import Date
from utilities.click import DateTime
from utilities.click import Enum
from utilities.click import log_level_option
from utilities.logging import LogLevel


def runners() -> SearchStrategy[CliRunner]:
    return just(CliRunner())


@command()
@argument("date", type=Date())
def uses_date(*, date: dt.date) -> None:
    echo(f"date = {date}")


def format_date_1(date: dt.date, /) -> str:
    return date.isoformat()


def format_date_2(date: dt.date, /) -> str:
    return date.strftime("%4Y%m%d")


class TestDate:
    @given(runner=runners(), date=dates())
    @mark.parametrize("format", [param(format_date_1), param(format_date_2)])
    def test_success(
        self, runner: CliRunner, date: dt.date, format: Callable[[dt.date], str]
    ) -> None:
        result = runner.invoke(uses_date, [format(date)])
        assert result.exit_code == 0
        assert result.stdout == f"date = {date:%4Y-%m-%d}\n"

    @given(runner=runners(), date=dates())
    def test_failure(self, runner: CliRunner, date: dt.date) -> None:
        result = runner.invoke(uses_date, [date.strftime("%4Y/%m/%d")])
        assert result.exit_code == 2


@command()
@argument("datetime", type=DateTime())
def uses_datetime(*, datetime: dt.datetime) -> None:
    echo(f"datetime = {datetime}")


def format_datetime_1(date: dt.datetime, /) -> str:
    return date.isoformat()


def format_datetime_2(date: dt.datetime, /) -> str:
    return date.strftime("%4Y%m%d%H%M%S")


class TestDateTime:
    @given(runner=runners(), date=datetimes())
    @mark.parametrize(
        "format", [param(format_datetime_1), param(format_datetime_2)]
    )
    def test_success(
        self,
        runner: CliRunner,
        date: dt.datetime,
        format: Callable[[dt.datetime], str],
    ) -> None:
        _ = assume(date.microsecond == 0)
        result = runner.invoke(uses_datetime, [format(date)])
        assert result.exit_code == 0
        assert result.stdout == f"datetime = {date}\n"

    @given(runner=runners(), date=dates())
    def test_failure(self, runner: CliRunner, date: dt.date) -> None:
        result = runner.invoke(
            uses_datetime, [date.strftime("%4Y/%m/%d %H:%M:%S")]
        )
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
        self, data: DataObject, runner: CliRunner, truth: Truth
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
