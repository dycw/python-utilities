from __future__ import annotations

import datetime as dt
import enum
from enum import auto
from re import search
from string import ascii_lowercase
from typing import TYPE_CHECKING, Any

import sqlalchemy
from click import ParamType, argument, command, echo, option
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
    lists,
    none,
    sampled_from,
    times,
)
from pytest import mark, param

import utilities.click
import utilities.datetime
import utilities.types
from utilities.click import (
    Date,
    DirPath,
    ExistingDirPath,
    ExistingFilePath,
    FilePath,
    ListChoices,
    ListDates,
    ListInts,
    ListMonths,
    ListStrs,
    LocalDateTime,
    Time,
    Timedelta,
    ZonedDateTime,
    local_scheduler_option_default_central,
    local_scheduler_option_default_local,
    log_level_option,
    workers_option,
)
from utilities.datetime import serialize_month
from utilities.hypothesis import (
    durations,
    months,
    sqlite_engines,
    text_ascii,
    timedeltas_2w,
)
from utilities.logging import LogLevel
from utilities.sqlalchemy import serialize_engine
from utilities.text import join_strs
from utilities.whenever import (
    serialize_date,
    serialize_duration,
    serialize_local_datetime,
    serialize_time,
    serialize_timedelta,
    serialize_zoned_datetime,
)
from utilities.zoneinfo import UTC

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from pathlib import Path

    from utilities.types import SequenceStrs


class _Truth(enum.Enum):
    true = auto()
    false = auto()


class TestEnum:
    def test_repr(self) -> None:
        param = utilities.click.Enum(_Truth)
        expected = f"Enum({_Truth})"
        assert repr(param) == expected

    @given(truth=sampled_from(_Truth))
    def test_command(self, *, truth: _Truth) -> None:
        @command()
        @argument("truth", type=utilities.click.Enum(_Truth))
        def cli(*, truth: _Truth) -> None:
            echo(f"truth = {truth}")

        result = CliRunner().invoke(cli, [truth.name])
        assert result.exit_code == 0
        assert result.stdout == f"truth = {truth}\n"

        result = CliRunner().invoke(cli, ["invalid"])
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


class TestFileAndDirPaths:
    def test_existing_dir_path(self, *, tmp_path: Path) -> None:
        @command()
        @argument("path", type=ExistingDirPath)
        def cli(*, path: Path) -> None:
            from pathlib import Path

            assert isinstance(path, Path)

        result = CliRunner().invoke(cli, [str(tmp_path)])
        assert result.exit_code == 0

        file_path = tmp_path.joinpath("file.txt")
        file_path.touch()
        result = CliRunner().invoke(cli, [str(file_path)])
        assert result.exit_code == 2
        assert search("is a file", result.stdout)

        non_existent = tmp_path.joinpath("non-existent")
        result = CliRunner().invoke(cli, [str(non_existent)])
        assert result.exit_code == 2
        assert search("does not exist", result.stdout)

    def test_existing_file_path(self, *, tmp_path: Path) -> None:
        @command()
        @argument("path", type=ExistingFilePath)
        def cli(*, path: Path) -> None:
            from pathlib import Path

            assert isinstance(path, Path)

        result = CliRunner().invoke(cli, [str(tmp_path)])
        assert result.exit_code == 2
        assert search("is a directory", result.stdout)

        file_path = tmp_path.joinpath("file.txt")
        file_path.touch()
        result = CliRunner().invoke(cli, [str(file_path)])
        assert result.exit_code == 0

        non_existent = tmp_path.joinpath("non-existent")
        result = CliRunner().invoke(cli, [str(non_existent)])
        assert result.exit_code == 2
        assert search("does not exist", result.stdout)

    def test_dir_path(self, *, tmp_path: Path) -> None:
        @command()
        @argument("path", type=DirPath)
        def cli(*, path: Path) -> None:
            from pathlib import Path

            assert isinstance(path, Path)

        result = CliRunner().invoke(cli, [str(tmp_path)])
        assert result.exit_code == 0

        file_path = tmp_path.joinpath("file.txt")
        file_path.touch()
        result = CliRunner().invoke(cli, [str(file_path)])
        assert result.exit_code == 2
        assert search("is a file", result.stdout)

        non_existent = tmp_path.joinpath("non-existent")
        result = CliRunner().invoke(cli, [str(non_existent)])
        assert result.exit_code == 0

    def test_file_path(self, *, tmp_path: Path) -> None:
        @command()
        @argument("path", type=FilePath)
        def cli(*, path: Path) -> None:
            from pathlib import Path

            assert isinstance(path, Path)

        result = CliRunner().invoke(cli, [str(tmp_path)])
        assert result.exit_code == 2
        assert search("is a directory", result.stdout)

        file_path = tmp_path.joinpath("file.txt")
        file_path.touch()
        result = CliRunner().invoke(cli, [str(file_path)])
        assert result.exit_code == 0

        non_existent = tmp_path.joinpath("non-existent")
        result = CliRunner().invoke(cli, [str(non_existent)])
        assert result.exit_code == 0


class TestListChoices:
    def test_repr(self) -> None:
        param = ListChoices(ascii_lowercase)
        expected = f"ListChoices({list(ascii_lowercase)})"
        assert repr(param) == expected

    @given(choices=lists(sampled_from(ascii_lowercase), min_size=1, unique=True))
    def test_command(self, *, choices: list[str]) -> None:
        @command()
        @argument("choices", type=ListChoices(ascii_lowercase))
        def cli(*, choices: list[str]) -> None:
            echo(f"choices = {choices}")

        joined = join_strs(choices)
        result = CliRunner().invoke(cli, [joined])
        assert result.exit_code == 0
        assert result.stdout == f"choices = {choices}\n"

        result = CliRunner().invoke(cli, ["invalid"])
        assert result.exit_code == 2

    @given(
        data=data(),
        choices=lists(sampled_from(ascii_lowercase), min_size=1, unique=True),
    )
    def test_case_insensitive(self, *, data: DataObject, choices: list[str]) -> None:
        @command()
        @argument("choices", type=ListChoices(ascii_lowercase, case_sensitive=False))
        def cli(*, choices: list[str]) -> None:
            echo(f"choices = {choices}")

        variable = [data.draw(sampled_from([c, c.upper()])) for c in choices]
        joined = join_strs(variable)
        result = CliRunner().invoke(cli, [joined])
        assert result.exit_code == 0
        assert result.stdout == f"choices = {choices}\n"

    @given(choices=lists(sampled_from(ascii_lowercase), min_size=1, unique=True))
    def test_option(self, *, choices: list[str]) -> None:
        @command()
        @option("--choices", type=ListChoices(ascii_lowercase), default=choices)
        def cli(*, choices: list[str]) -> None:
            echo(f"choices = {choices}")

        result = CliRunner().invoke(cli)
        assert result.exit_code == 0
        assert result.stdout == f"choices = {choices}\n"


class TestLocalSchedulerOption:
    @mark.parametrize(
        ("args", "expected"),
        [param([], True), param(["-ls"], True), param(["-nls"], False)],
    )
    def test_default_local(self, *, args: SequenceStrs, expected: bool) -> None:
        @command()
        @local_scheduler_option_default_local
        def cli(*, local_scheduler: bool) -> None:
            echo(f"local_scheduler = {local_scheduler}")

        result = CliRunner().invoke(cli, args)
        assert result.exit_code == 0
        assert result.stdout == f"local_scheduler = {expected}\n"

    @mark.parametrize(
        ("args", "expected"),
        [param([], False), param(["-ls"], True), param(["-nls"], False)],
    )
    def test_default_central(self, *, args: SequenceStrs, expected: bool) -> None:
        @command()
        @local_scheduler_option_default_central
        def cli(*, local_scheduler: bool) -> None:
            echo(f"local_scheduler = {local_scheduler}")

        result = CliRunner().invoke(cli, args)
        assert result.exit_code == 0
        assert result.stdout == f"local_scheduler = {expected}\n"


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


def _serialize_iterable_dates(values: Iterable[dt.date], /) -> str:
    return join_strs(map(serialize_date, values))


def _serialize_iterable_ints(values: Iterable[int], /) -> str:
    return join_strs(map(str, values))


def _serialize_iterable_months(values: Iterable[utilities.datetime.Month], /) -> str:
    return join_strs(map(serialize_month, values))


def _serialize_iterable_strs(values: Iterable[str], /) -> str:
    return join_strs(values)


class TestParameters:
    cases = (
        param(Date(), dt.date, dates(), serialize_date, True),
        param(
            utilities.click.Duration(),
            utilities.types.Duration,
            durations(two_way=True),
            serialize_duration,
            True,
        ),
        param(
            utilities.click.Engine(),
            sqlalchemy.Engine,
            sqlite_engines(),
            serialize_engine,
            True,
        ),
        param(
            ListDates(), list[dt.date], lists(dates()), _serialize_iterable_dates, True
        ),
        param(
            ListInts(),
            list[int],
            lists(integers(0, 10)),
            _serialize_iterable_ints,
            True,
        ),
        param(
            ListMonths(),
            list[utilities.datetime.Month],
            lists(months()),
            _serialize_iterable_months,
            True,
        ),
        param(
            ListStrs(),
            list[str],
            lists(text_ascii(), min_size=5),
            _serialize_iterable_strs,
            False,
        ),
        param(
            LocalDateTime(), dt.datetime, datetimes(), serialize_local_datetime, True
        ),
        param(
            utilities.click.Month(),
            utilities.datetime.Month,
            months(),
            serialize_month,
            True,
        ),
        param(Time(), dt.time, times(), serialize_time, True),
        param(
            Timedelta(),
            dt.timedelta,
            timedeltas_2w(min_value=dt.timedelta(0)),
            serialize_timedelta,
            True,
        ),
        param(
            ZonedDateTime(),
            dt.datetime,
            datetimes(timezones=just(UTC)),
            serialize_zoned_datetime,
            True,
        ),
    )

    @given(data=data())
    @mark.parametrize(("param", "cls", "strategy", "serialize", "failable"), cases)
    def test_argument(
        self,
        *,
        data: DataObject,
        param: ParamType,
        cls: Any,
        strategy: SearchStrategy[Any],
        serialize: Callable[[Any], str],
        failable: bool,
    ) -> None:
        runner = CliRunner()

        @command()
        @argument("value", type=param)
        def cli(*, value: cls) -> None:
            echo(f"value = {serialize(value)}")

        result = CliRunner().invoke(cli, ["--help"])
        assert result.exit_code == 0

        value_str = serialize(data.draw(strategy))
        result = CliRunner().invoke(cli, [value_str])
        assert result.exit_code == 0
        assert result.stdout == f"value = {value_str}\n"

        result = runner.invoke(cli, ["error"])
        expected = 2 if failable else 0
        assert result.exit_code == expected

    @given(data=data())
    @mark.parametrize(("param", "cls", "strategy", "serialize", "failable"), cases)
    def test_option(
        self,
        *,
        data: DataObject,
        param: ParamType,
        cls: Any,
        strategy: SearchStrategy[Any],
        serialize: Callable[[Any], str],
        failable: bool,
    ) -> None:
        value = data.draw(strategy)

        @command()
        @option("--value", type=param, default=value)
        def cli(*, value: cls) -> None:
            echo(f"value = {serialize(value)}")

        result = CliRunner().invoke(cli, ["--help"])
        assert result.exit_code == 0

        result = CliRunner().invoke(cli)
        assert result.exit_code == 0
        assert result.stdout == f"value = {serialize(value)}\n"

        _ = failable


class TestWorkersOption:
    @given(workers=integers() | none())
    def test_main(self, workers: int | None) -> None:
        @command()
        @workers_option
        def cli(*, workers: int | None) -> None:
            echo(f"workers = {workers}")

        args = [] if workers is None else ["--workers", str(workers)]
        result = CliRunner().invoke(cli, args)
        assert result.exit_code == 0
        assert result.stdout == f"workers = {workers}\n"
