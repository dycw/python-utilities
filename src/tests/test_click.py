import datetime as dt
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
from hypothesis.strategies import just
from hypothesis.strategies import sampled_from
from hypothesis.strategies._internal.datetime import datetimes

from dycw_utilities.click import Date
from dycw_utilities.click import DateTime
from dycw_utilities.click import Enum


def runners() -> SearchStrategy[CliRunner]:
    return just(CliRunner())


@command()
@argument("date", type=Date())
def uses_date(date: dt.date) -> None:
    echo(f"date = {date}")


class TestDate:
    @given(data=data(), runner=runners(), date=dates())
    def test_success(
        self, data: DataObject, runner: CliRunner, date: dt.date
    ) -> None:
        as_str = data.draw(
            sampled_from([date.isoformat(), date.strftime("%4Y%m%d")])
        )
        result = runner.invoke(uses_date, [as_str])
        assert result.exit_code == 0
        assert result.stdout == f"date = {date:%4Y-%m-%d}\n"

    @given(runner=runners(), date=dates())
    def test_failure(self, runner: CliRunner, date: dt.date) -> None:
        result = runner.invoke(uses_date, [date.strftime("%4Y/%m/%d")])
        assert result.exit_code == 2


@command()
@argument("datetime", type=DateTime())
def uses_datetime(datetime: dt.datetime) -> None:
    echo(f"datetime = {datetime}")


class TestDateTime:
    @given(data=data(), runner=runners(), date=datetimes())
    def test_success(
        self, data: DataObject, runner: CliRunner, date: dt.datetime
    ) -> None:
        _ = assume(date.microsecond == 0)
        as_str = data.draw(
            sampled_from([date.isoformat(), date.strftime("%4Y%m%d%H%M%S")])
        )
        result = runner.invoke(uses_datetime, [as_str])
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
def uses_enum(truth: Truth) -> None:
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
