from __future__ import annotations

import enum
from dataclasses import dataclass
from enum import auto
from operator import attrgetter
from re import search
from typing import TYPE_CHECKING, Any, ClassVar

import whenever
from click import ParamType, argument, command, echo, option
from click.testing import CliRunner
from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    SearchStrategy,
    data,
    frozensets,
    integers,
    ip_addresses,
    lists,
    sampled_from,
)
from pytest import mark, param

from utilities.click import (
    CONTEXT_SETTINGS_HELP_OPTION_NAMES,
    Date,
    DateDelta,
    DateTimeDelta,
    DirPath,
    Enum,
    ExistingDirPath,
    ExistingFilePath,
    FilePath,
    FrozenSetChoices,
    FrozenSetEnums,
    FrozenSetInts,
    FrozenSetStrs,
    IPv4Address,
    IPv6Address,
    ListChoices,
    ListEnums,
    ListInts,
    ListStrs,
    MonthDay,
    PlainDateTime,
    Time,
    TimeDelta,
    YearMonth,
    ZonedDateTime,
)
from utilities.hypothesis import (
    date_deltas,
    date_time_deltas,
    dates,
    month_days,
    plain_datetimes,
    text_ascii,
    time_deltas,
    times,
    year_months,
    zoned_datetimes,
)
from utilities.text import join_strs, strip_and_dedent

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from pathlib import Path


class TestContextSettingsHelpOptionNames:
    @given(help_=sampled_from(["-h", "--help"]))
    def test_main(self, *, help_: str) -> None:
        @command(**CONTEXT_SETTINGS_HELP_OPTION_NAMES)
        def cli() -> None: ...

        result = CliRunner().invoke(cli, [help_])
        assert result.exit_code == 0


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
        assert search("is a file", result.stderr)

        non_existent = tmp_path.joinpath("non-existent")
        result = CliRunner().invoke(cli, [str(non_existent)])
        assert result.exit_code == 2
        assert search("does not exist", result.stderr)

    def test_existing_file_path(self, *, tmp_path: Path) -> None:
        @command()
        @argument("path", type=ExistingFilePath)
        def cli(*, path: Path) -> None:
            from pathlib import Path

            assert isinstance(path, Path)

        result = CliRunner().invoke(cli, [str(tmp_path)])
        assert result.exit_code == 2
        assert search("is a directory", result.stderr)

        file_path = tmp_path.joinpath("file.txt")
        file_path.touch()
        result = CliRunner().invoke(cli, [str(file_path)])
        assert result.exit_code == 0

        non_existent = tmp_path.joinpath("non-existent")
        result = CliRunner().invoke(cli, [str(non_existent)])
        assert result.exit_code == 2
        assert search("does not exist", result.stderr)

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
        assert search("is a file", result.stderr)

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
        assert search("is a directory", result.stderr)

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


def _lift_serializer[T](
    serializer: Callable[[T], str], /, *, sort: bool = False
) -> Callable[[Iterable[T]], str]:
    def wrapped(values: Iterable[T], /) -> str:
        return join_strs(map(serializer, values), sort=sort)

    return wrapped


@dataclass(kw_only=True, slots=True)
class _Case[T]:
    param: ParamType
    name: str
    repr: str | None = None
    strategy: SearchStrategy[T]
    serialize: Callable[[T], str]
    failable: bool = False


class TestParameters:
    cases: ClassVar[list[_Case]] = [
        _Case(
            param=Date(),
            name="date",
            strategy=dates(),
            serialize=whenever.Date.format_common_iso,
            failable=True,
        ),
        _Case(
            param=DateDelta(),
            name="date delta",
            strategy=date_deltas(parsable=True),
            serialize=whenever.DateDelta.format_common_iso,
            failable=True,
        ),
        _Case(
            param=DateTimeDelta(),
            name="date-time delta",
            strategy=date_time_deltas(parsable=True),
            serialize=whenever.DateTimeDelta.format_common_iso,
            failable=True,
        ),
        _Case(
            param=Enum(_ExampleEnum),
            name="enum[_ExampleEnum]",
            repr="ENUM[_ExampleEnum]",
            strategy=sampled_from(_ExampleEnum),
            serialize=attrgetter("name"),
            failable=True,
        ),
        _Case(
            param=FrozenSetInts(),
            name="frozenset[integer]",
            repr="FROZENSET[INT]",
            strategy=frozensets(integers()),
            serialize=_lift_serializer(str, sort=True),
            failable=True,
        ),
        _Case(
            param=FrozenSetChoices(["a", "b", "c"]),
            name="frozenset[choice]",
            repr="FROZENSET[Choice(['a', 'b', 'c'])]",
            strategy=frozensets(sampled_from(["a", "b", "c"])),
            serialize=_lift_serializer(str, sort=True),
            failable=True,
        ),
        _Case(
            param=FrozenSetEnums(_ExampleEnum),
            name="frozenset[enum[_ExampleEnum]]",
            repr="FROZENSET[ENUM[_ExampleEnum]]",
            strategy=frozensets(sampled_from(_ExampleEnum)),
            serialize=_lift_serializer(attrgetter("name"), sort=True),
            failable=True,
        ),
        _Case(
            param=FrozenSetStrs(),
            name="frozenset[text]",
            repr="FROZENSET[STRING]",
            strategy=frozensets(text_ascii()),
            serialize=_lift_serializer(str, sort=True),
        ),
        _Case(
            param=IPv4Address(),
            name="ipv4 address",
            strategy=ip_addresses(v=4),
            serialize=str,
            failable=True,
        ),
        _Case(
            param=IPv6Address(),
            name="ipv6 address",
            strategy=ip_addresses(v=6),
            serialize=str,
            failable=True,
        ),
        _Case(
            param=ListChoices(["a", "b", "c"]),
            name="list[choice]",
            repr="LIST[Choice(['a', 'b', 'c'])]",
            strategy=lists(sampled_from(["a", "b", "c"])),
            serialize=_lift_serializer(str),
            failable=True,
        ),
        _Case(
            param=ListInts(),
            name="list[integer]",
            repr="LIST[INT]",
            strategy=lists(integers()),
            serialize=_lift_serializer(str),
            failable=True,
        ),
        _Case(
            param=ListEnums(_ExampleEnum),
            name="list[enum[_ExampleEnum]]",
            repr="LIST[ENUM[_ExampleEnum]]",
            strategy=lists(sampled_from(_ExampleEnum)),
            serialize=_lift_serializer(attrgetter("name")),
            failable=True,
        ),
        _Case(
            param=MonthDay(),
            name="month-day",
            strategy=month_days(),
            serialize=whenever.MonthDay.format_common_iso,
            failable=True,
        ),
        _Case(
            param=PlainDateTime(),
            name="plain date-time",
            strategy=plain_datetimes(),
            serialize=whenever.PlainDateTime.format_common_iso,
            failable=True,
        ),
        _Case(
            param=Time(),
            name="time",
            strategy=times(),
            serialize=whenever.Time.format_common_iso,
            failable=True,
        ),
        _Case(
            param=TimeDelta(),
            name="time-delta",
            strategy=time_deltas(),
            serialize=whenever.TimeDelta.format_common_iso,
            failable=True,
        ),
        _Case(
            param=YearMonth(),
            name="year-month",
            strategy=year_months(),
            serialize=whenever.YearMonth.format_common_iso,
            failable=True,
        ),
        _Case(
            param=ZonedDateTime(),
            name="zoned date-time",
            strategy=zoned_datetimes(),
            serialize=whenever.ZonedDateTime.format_common_iso,
            failable=True,
        ),
    ]

    @given(data=data())
    @mark.parametrize(
        ("param", "strategy", "serialize"),
        [param(c.param, c.strategy, c.serialize) for c in cases],
    )
    def test_default(
        self,
        *,
        data: DataObject,
        param: ParamType,
        strategy: SearchStrategy[Any],
        serialize: Callable[[Any], str],
    ) -> None:
        default = data.draw(strategy)

        @command()
        @option("--value", type=param, default=default)
        def cli(*, value: Any) -> None:
            echo(f"value = {serialize(value)}")

        result = CliRunner().invoke(cli, args=[])
        assert result.exit_code == 0
        assert result.stdout == f"value = {serialize(default)}\n"

    @given(data=data())
    @mark.parametrize(
        ("param", "strategy", "serialize"),
        [param(c.param, c.strategy, c.serialize) for c in cases],
    )
    def test_cli_value(
        self,
        *,
        data: DataObject,
        param: ParamType,
        strategy: SearchStrategy[Any],
        serialize: Callable[[Any], str],
    ) -> None:
        @command()
        @option("--value", type=param)
        def cli(*, value: Any) -> None:
            echo(f"value = {serialize(value)}")

        value = data.draw(strategy)
        ser = serialize(value)
        result = CliRunner().invoke(cli, args=[f"--value={ser}"])
        assert result.exit_code == 0
        assert result.stdout == f"value = {ser}\n"

    @mark.parametrize(
        ("param", "serialize"),
        [param(c.param, c.serialize) for c in cases if c.failable],
    )
    def test_cli_fail(
        self, *, param: ParamType, serialize: Callable[[Any], str]
    ) -> None:
        @command()
        @option("--value", type=param)
        def cli(*, value: Any) -> None:
            echo(f"value = {serialize(value)}")

        result = CliRunner().invoke(cli, args=["--value=invalid"])
        assert result.exit_code == 2

    @mark.parametrize(("param", "name"), [param(c.param, c.name) for c in cases])
    def test_name(self, *, param: ParamType, name: str) -> None:
        assert param.name == name

    @mark.parametrize(
        ("param", "repr_", "name"), [param(c.param, c.repr, c.name) for c in cases]
    )
    def test_repr(self, *, param: ParamType, repr_: str | None, name: str) -> None:
        expected = name.upper() if repr_ is None else repr_
        assert repr(param) == expected

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
            "Invalid value for '--value': Object '0' of type 'int' must be a string",
            result.stderr,
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
