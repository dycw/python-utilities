from __future__ import annotations

import datetime as dt
import enum
from enum import auto
from operator import attrgetter
from re import search
from typing import TYPE_CHECKING, Any, TypeVar
from uuid import UUID

from click import ParamType, argument, command, echo, option
from click.testing import CliRunner
from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    SearchStrategy,
    booleans,
    data,
    dates,
    datetimes,
    floats,
    frozensets,
    integers,
    just,
    lists,
    sampled_from,
    times,
    uuids,
)
from pytest import mark, param

import utilities.click
import utilities.datetime
import utilities.types
from utilities.click import (
    Date,
    DirPath,
    Enum,
    ExistingDirPath,
    ExistingFilePath,
    FilePath,
    FrozenSetBools,
    FrozenSetDates,
    FrozenSetEnums,
    FrozenSetFloats,
    FrozenSetInts,
    FrozenSetMonths,
    FrozenSetStrs,
    FrozenSetUUIDs,
    ListBools,
    ListDates,
    ListEnums,
    ListFloats,
    ListInts,
    ListMonths,
    ListStrs,
    ListUUIDs,
    LocalDateTime,
    Time,
    Timedelta,
    ZonedDateTime,
)
from utilities.datetime import serialize_month
from utilities.hypothesis import durations, months, text_ascii, timedeltas_2w
from utilities.text import join_strs, strip_and_dedent
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


_T = TypeVar("_T")


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


class _Truth(enum.Enum):
    true = auto()
    false = auto()


def _lift_serializer(serializer: Callable[[_T], str]) -> Callable[[Iterable[_T]], str]:
    def wrapped(values: Iterable[_T], /) -> str:
        return join_strs(map(serializer, values))

    return wrapped


class TestParameters:
    cases = (
        param(Date(), "DATE", dt.date, dates(), serialize_date, True),
        param(
            Enum(_Truth),
            "ENUM[_Truth]",
            _Truth,
            sampled_from(_Truth),
            attrgetter("name"),
            True,
        ),
        param(
            utilities.click.Duration(),
            "DURATION",
            utilities.types.Duration,
            durations(min_number=0, min_timedelta=dt.timedelta(0), two_way=True),
            serialize_duration,
            True,
        ),
        param(
            FrozenSetBools(),
            "FROZENSET[BOOL]",
            frozenset[bool],
            frozensets(booleans(), max_size=1),
            _lift_serializer(str),
            True,
        ),
        param(
            FrozenSetDates(),
            "FROZENSET[DATE]",
            frozenset[dt.date],
            frozensets(dates(), max_size=1),
            _lift_serializer(serialize_date),
            True,
        ),
        param(
            FrozenSetEnums(_Truth),
            "FROZENSET[ENUM[_Truth]]",
            frozenset[_Truth],
            frozensets(sampled_from(_Truth), max_size=1),
            _lift_serializer(attrgetter("name")),
            True,
        ),
        param(
            FrozenSetFloats(),
            "FROZENSET[FLOAT]",
            frozenset[float],
            frozensets(floats(0, 10), max_size=1),
            _lift_serializer(str),
            True,
        ),
        param(
            FrozenSetInts(),
            "FROZENSET[INT]",
            frozenset[int],
            frozensets(integers(0, 10), max_size=1),
            _lift_serializer(str),
            True,
        ),
        param(
            FrozenSetMonths(),
            "FROZENSET[MONTH]",
            frozenset[utilities.datetime.Month],
            frozensets(months(), max_size=1),
            _lift_serializer(serialize_month),
            True,
        ),
        param(
            FrozenSetStrs(),
            "FROZENSET[STRING]",
            frozenset[str],
            frozensets(text_ascii(), max_size=1),
            _lift_serializer(str),
            False,
        ),
        param(
            FrozenSetUUIDs(),
            "FROZENSET[UUID]",
            frozenset[UUID],
            frozensets(uuids(), max_size=1),
            _lift_serializer(str),
            True,
        ),
        param(
            ListBools(),
            "LIST[BOOL]",
            list[bool],
            lists(booleans()),
            _lift_serializer(str),
            True,
        ),
        param(
            ListDates(),
            "LIST[DATE]",
            list[dt.date],
            lists(dates()),
            _lift_serializer(serialize_date),
            True,
        ),
        param(
            ListEnums(_Truth),
            "LIST[ENUM[_Truth]]",
            list[_Truth],
            lists(sampled_from(_Truth)),
            _lift_serializer(attrgetter("name")),
            True,
        ),
        param(
            ListFloats(),
            "LIST[FLOAT]",
            list[float],
            lists(floats(0, 10)),
            _lift_serializer(str),
            True,
        ),
        param(
            ListInts(),
            "LIST[INT]",
            list[int],
            lists(integers(0, 10)),
            _lift_serializer(str),
            True,
        ),
        param(
            ListMonths(),
            "LIST[MONTH]",
            list[utilities.datetime.Month],
            lists(months()),
            _lift_serializer(serialize_month),
            True,
        ),
        param(
            ListStrs(),
            "LIST[STRING]",
            list[str],
            lists(text_ascii()),
            _lift_serializer(str),
            False,
        ),
        param(
            ListUUIDs(),
            "LIST[UUID]",
            list[UUID],
            lists(uuids()),
            _lift_serializer(str),
            True,
        ),
        param(
            LocalDateTime(),
            "LOCAL DATETIME",
            dt.datetime,
            datetimes(),
            serialize_local_datetime,
            True,
        ),
        param(
            utilities.click.Month(),
            "MONTH",
            utilities.datetime.Month,
            months(),
            serialize_month,
            True,
        ),
        param(Time(), "TIME", dt.time, times(), serialize_time, True),
        param(
            Timedelta(),
            "TIMEDELTA",
            dt.timedelta,
            timedeltas_2w(min_value=dt.timedelta(0)),
            serialize_timedelta,
            True,
        ),
        param(
            ZonedDateTime(),
            "ZONED DATETIME",
            dt.datetime,
            datetimes(timezones=just(UTC)),
            serialize_zoned_datetime,
            True,
        ),
    )

    @given(data=data())
    @mark.parametrize(
        ("param", "exp_repr", "cls", "strategy", "serialize", "failable"), cases
    )
    def test_argument(
        self,
        *,
        data: DataObject,
        param: ParamType,
        exp_repr: str,
        cls: Any,
        strategy: SearchStrategy[Any],
        serialize: Callable[[Any], str],
        failable: bool,
    ) -> None:
        assert repr(param) == exp_repr

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
        expected = 2 if failable else 0
        assert result.exit_code == expected

    @given(data=data())
    @mark.parametrize(
        ("param", "exp_repr", "cls", "strategy", "serialize", "failable"), cases
    )
    def test_option(
        self,
        *,
        data: DataObject,
        param: ParamType,
        exp_repr: str,
        cls: Any,
        strategy: SearchStrategy[Any],
        serialize: Callable[[Any], str],
        failable: bool,
    ) -> None:
        assert repr(param) == exp_repr

        value = data.draw(strategy)

        @command()
        @option("--value", type=param, default=value)
        def cli(*, value: cls) -> None:
            echo(f"value = {serialize(value)}")

        result = CliRunner().invoke(cli)
        assert result.exit_code == 0
        assert result.stdout == f"value = {serialize(value)}\n"

        _ = failable

    @mark.parametrize(
        "param", [param(ListEnums(_Truth)), param(FrozenSetEnums(_Truth))], ids=str
    )
    def test_error_list_and_frozensets_parse(self, *, param: ParamType) -> None:
        @command()
        @option("--value", type=param, default=0)
        def cli(*, value: list[_Truth] | frozenset[_Truth]) -> None:
            echo(f"value = {value}")

        result = CliRunner().invoke(cli)
        assert result.exit_code == 2
        assert search(
            "Invalid value for '--value': Unable to parse 0 of type <class 'int'>",
            result.stdout,
        )


class TestCLIHelp:
    @mark.parametrize(
        ("param", "expected"),
        [
            param(
                str,
                """
    Usage: cli [OPTIONS]

    Options:
      --value TEXT
      --help        Show this message and exit.
""",
            ),
            param(
                ListStrs(),
                """
    Usage: cli [OPTIONS]

    Options:
      --value [LIST[TEXT] SEP=,]
      --help                      Show this message and exit.
""",
            ),
            param(
                Enum(_Truth),
                """
    Usage: cli [OPTIONS]

    Options:
      --value [true,false]
      --help                Show this message and exit.
""",
            ),
            param(
                ListEnums(_Truth),
                """
    Usage: cli [OPTIONS]

    Options:
      --value [LIST[true,false] SEP=,]
      --help                          Show this message and exit.
""",
            ),
        ],
    )
    def test_main(self, *, param: Any, expected: str) -> None:
        @command()
        @option("--value", type=param)
        def cli(*, value: Any) -> None:
            echo(f"value = {value}")

        result = CliRunner().invoke(cli, ["--help"])
        assert result.exit_code == 0
        expected = strip_and_dedent(expected, trailing=True)
        assert result.stdout == expected
