from __future__ import annotations

import datetime as dt
import enum
from enum import auto
from re import search
from typing import TYPE_CHECKING, Any

import pytest
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
    none,
    sampled_from,
    timedeltas,
    times,
)

import utilities.click
from utilities.click import (
    Date,
    DateTime,
    DirPath,
    ExistingDirPath,
    ExistingFilePath,
    FilePath,
    Time,
    Timedelta,
    local_scheduler_option_default_central,
    local_scheduler_option_default_local,
    log_level_option,
    workers_option,
)
from utilities.datetime import (
    UTC,
    serialize_date,
    serialize_datetime,
    serialize_time,
    serialize_timedelta,
)
from utilities.hypothesis import sqlite_engines
from utilities.logging import LogLevel
from utilities.sqlalchemy import serialize_engine

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from utilities.types import SequenceStrs


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


class TestLocalSchedulerOption:
    @pytest.mark.parametrize(
        ("args", "expected"),
        [
            pytest.param([], True),
            pytest.param(["-ls"], True),
            pytest.param(["-nls"], False),
        ],
    )
    def test_default_local(self, *, args: SequenceStrs, expected: bool) -> None:
        @command()
        @local_scheduler_option_default_local
        def cli(*, local_scheduler: bool) -> None:
            echo(f"local_scheduler = {local_scheduler}")

        result = CliRunner().invoke(cli, args)
        assert result.exit_code == 0
        assert result.stdout == f"local_scheduler = {expected}\n"

    @pytest.mark.parametrize(
        ("args", "expected"),
        [
            pytest.param([], False),
            pytest.param(["-ls"], True),
            pytest.param(["-nls"], False),
        ],
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


class TestParameters:
    cases = (
        pytest.param(Date(), dt.date, dates(), serialize_date),
        pytest.param(
            DateTime(), dt.datetime, datetimes(timezones=just(UTC)), serialize_datetime
        ),
        pytest.param(
            utilities.click.Engine(),
            sqlalchemy.Engine,
            sqlite_engines(),
            serialize_engine,
        ),
        pytest.param(Time(), dt.time, times(), serialize_time),
        pytest.param(Timedelta(), dt.timedelta, timedeltas(), serialize_timedelta),
    )

    @given(data=data())
    @pytest.mark.parametrize(("param", "cls", "strategy", "serialize"), cases)
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
    @pytest.mark.parametrize(("param", "cls", "strategy", "serialize"), cases)
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
