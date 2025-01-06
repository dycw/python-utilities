from __future__ import annotations

import enum
from enum import auto
from operator import attrgetter
from re import search
from typing import TYPE_CHECKING, Any, TypeVar

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
    FrozenSetChoices,
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
from utilities.datetime import ZERO_TIME, serialize_month
from utilities.hypothesis import (
    datetime_durations,
    months,
    pairs,
    text_ascii,
    timedeltas_2w,
)
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


class _ExampleEnum(enum.Enum):
    a = auto()
    b = auto()
    c = auto()


def _lift_serializer(
    serializer: Callable[[_T], str], /, *, sort: bool = False
) -> Callable[[Iterable[_T]], str]:
    def wrapped(values: Iterable[_T], /) -> str:
        return join_strs(map(serializer, values), sort=sort)

    return wrapped


class TestParameters:
    @given(data=data(), use_value=booleans())
    @mark.parametrize(
        ("param", "exp_repr", "strategy", "serialize", "failable"),
        [
            param(Date(), "DATE", dates(), serialize_date, True),
            param(
                Enum(_ExampleEnum),
                "ENUM[_ExampleEnum]",
                sampled_from(_ExampleEnum),
                attrgetter("name"),
                True,
            ),
            param(
                utilities.click.Duration(),
                "DURATION",
                datetime_durations(min_number=0, min_timedelta=ZERO_TIME, two_way=True),
                serialize_duration,
                True,
            ),
            param(
                FrozenSetBools(),
                "FROZENSET[BOOL]",
                frozensets(booleans()),
                _lift_serializer(str, sort=True),
                True,
            ),
            param(
                FrozenSetDates(),
                "FROZENSET[DATE]",
                frozensets(dates()),
                _lift_serializer(serialize_date, sort=True),
                True,
            ),
            param(
                FrozenSetChoices(["a", "b", "c"]),
                "FROZENSET[Choice(['a', 'b', 'c'])]",
                frozensets(sampled_from(["a", "b", "c"])),
                _lift_serializer(str, sort=True),
                True,
            ),
            param(
                FrozenSetEnums(_ExampleEnum),
                "FROZENSET[ENUM[_ExampleEnum]]",
                frozensets(sampled_from(_ExampleEnum)),
                _lift_serializer(attrgetter("name"), sort=True),
                True,
            ),
            param(
                FrozenSetFloats(),
                "FROZENSET[FLOAT]",
                frozensets(floats(0, 10)),
                _lift_serializer(str, sort=True),
                True,
            ),
            param(
                FrozenSetInts(),
                "FROZENSET[INT]",
                frozensets(integers(0, 10)),
                _lift_serializer(str, sort=True),
                True,
            ),
            param(
                FrozenSetMonths(),
                "FROZENSET[MONTH]",
                frozensets(months()),
                _lift_serializer(serialize_month, sort=True),
                True,
            ),
            param(
                FrozenSetStrs(),
                "FROZENSET[STRING]",
                frozensets(text_ascii()),
                _lift_serializer(str, sort=True),
                False,
            ),
            param(
                FrozenSetUUIDs(),
                "FROZENSET[UUID]",
                frozensets(uuids()),
                _lift_serializer(str, sort=True),
                True,
            ),
            param(
                ListBools(),
                "LIST[BOOL]",
                lists(booleans()),
                _lift_serializer(str),
                True,
            ),
            param(
                ListDates(),
                "LIST[DATE]",
                lists(dates()),
                _lift_serializer(serialize_date),
                True,
            ),
            param(
                ListEnums(_ExampleEnum),
                "LIST[ENUM[_ExampleEnum]]",
                lists(sampled_from(_ExampleEnum)),
                _lift_serializer(attrgetter("name")),
                True,
            ),
            param(
                ListFloats(),
                "LIST[FLOAT]",
                lists(floats(0, 10)),
                _lift_serializer(str),
                True,
            ),
            param(
                ListInts(),
                "LIST[INT]",
                lists(integers(0, 10)),
                _lift_serializer(str),
                True,
            ),
            param(
                ListMonths(),
                "LIST[MONTH]",
                lists(months()),
                _lift_serializer(serialize_month),
                True,
            ),
            param(
                ListStrs(),
                "LIST[STRING]",
                lists(text_ascii()),
                _lift_serializer(str),
                False,
            ),
            param(
                ListUUIDs(), "LIST[UUID]", lists(uuids()), _lift_serializer(str), True
            ),
            param(
                LocalDateTime(),
                "LOCAL DATETIME",
                datetimes(),
                serialize_local_datetime,
                True,
            ),
            param(utilities.click.Month(), "MONTH", months(), serialize_month, True),
            param(Time(), "TIME", times(), serialize_time, True),
            param(
                Timedelta(),
                "TIMEDELTA",
                timedeltas_2w(min_value=ZERO_TIME),
                serialize_timedelta,
                True,
            ),
            param(
                ZonedDateTime(),
                "ZONED DATETIME",
                datetimes(timezones=just(UTC)),
                serialize_zoned_datetime,
                True,
            ),
        ],
    )
    def test_main(
        self,
        *,
        data: DataObject,
        param: ParamType,
        exp_repr: str,
        strategy: SearchStrategy[Any],
        serialize: Callable[[Any], str],
        use_value: bool,
        failable: bool,
    ) -> None:
        assert repr(param) == exp_repr

        default, value = data.draw(pairs(strategy))

        @command()
        @option("--value", type=param, default=default)
        def cli(*, value: Any) -> None:
            echo(f"value = {serialize(value)}")

        args = [f"--value={serialize(value)}"] if use_value else None
        result = CliRunner().invoke(cli, args=args)
        assert result.exit_code == 0
        expected_str = serialize(value if use_value else default)
        assert result.stdout == f"value = {expected_str}\n"

        result = CliRunner().invoke(cli, ["--value=error"])
        expected = 2 if failable else 0
        assert result.exit_code == expected

    @mark.parametrize(
        "param",
        [param(ListEnums(_ExampleEnum)), param(FrozenSetEnums(_ExampleEnum))],
        ids=str,
    )
    def test_error_list_and_frozensets_parse(self, *, param: ParamType) -> None:
        @command()
        @option("--value", type=param, default=0)
        def cli(*, value: list[_ExampleEnum] | frozenset[_ExampleEnum]) -> None:
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
                Enum(_ExampleEnum),
                """
    Usage: cli [OPTIONS]

    Options:
      --value [a,b,c]
      --help           Show this message and exit.
""",
            ),
            param(
                FrozenSetEnums(_ExampleEnum),
                """
    Usage: cli [OPTIONS]

    Options:
      --value [FROZENSET[a,b,c] SEP=,]
      --help                          Show this message and exit.
""",
            ),
            param(
                FrozenSetStrs(),
                """
    Usage: cli [OPTIONS]

    Options:
      --value [FROZENSET[TEXT] SEP=,]
      --help                          Show this message and exit.
""",
            ),
            param(
                ListEnums(_ExampleEnum),
                """
    Usage: cli [OPTIONS]

    Options:
      --value [LIST[a,b,c] SEP=,]
      --help                       Show this message and exit.
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
