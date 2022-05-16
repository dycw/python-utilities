import datetime as dt

from click import argument
from click import command
from click import echo
from click.testing import CliRunner
from hypothesis import given
from hypothesis.strategies import DataObject
from hypothesis.strategies import SearchStrategy
from hypothesis.strategies import data
from hypothesis.strategies import dates
from hypothesis.strategies import just
from hypothesis.strategies import sampled_from

from dycw_utilities.click import Date


@command()
@argument("date", type=Date())
def func(date: dt.date) -> None:
    echo(f"date = {date}")


def runners() -> SearchStrategy[CliRunner]:
    return just(CliRunner())


class TestDate:
    @given(data=data(), runner=runners(), date=dates())
    def test_date(
        self, data: DataObject, runner: CliRunner, date: dt.date
    ) -> None:

        as_str = data.draw(
            sampled_from([date.isoformat(), date.strftime("%4Y%m%d")])
        )
        result = runner.invoke(func, [as_str])
        assert result.exit_code == 0
        assert result.stdout == f"date = {date:%4Y-%m-%d}\n"

    @given(runner=runners(), date=dates())
    def test_failure(self, runner: CliRunner, date: dt.date) -> None:
        @command()
        @argument("d", type=Date())
        def func(d: dt.date) -> None:
            assert d == date

        result = runner.invoke(func, [date.strftime("%4Y/%m/%d")])
        assert result.exit_code == 2
