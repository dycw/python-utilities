from __future__ import annotations

import enum
from dataclasses import dataclass
from enum import auto
from operator import attrgetter
from re import search
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeVar

import whenever
from click import ParamType, argument, command, echo, option
from click.testing import CliRunner
from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    SearchStrategy,
    booleans,
    data,
    frozensets,
    ip_addresses,
    lists,
    sampled_from,
)
from pytest import mark, param

import utilities
import utilities.whenever
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
    Freq,
    FrozenSetChoices,
    FrozenSetEnums,
    FrozenSetStrs,
    IPv4Address,
    IPv6Address,
    ListEnums,
    ListStrs,
    Month,
    PlainDateTime,
    Time,
    TimeDelta,
    ZonedDateTime,
)
from utilities.hypothesis import (
    date_deltas,
    date_time_deltas,
    dates,
    freqs,
    months,
    pairs,
    plain_datetimes,
    text_ascii,
    time_deltas,
    times,
    zoned_datetimes,
)
from utilities.text import join_strs, strip_and_dedent

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from pathlib import Path

    from _pytest.mark import ParameterSet


_T = TypeVar("_T")


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


def _lift_serializer(
    serializer: Callable[[_T], str], /, *, sort: bool = False
) -> Callable[[Iterable[_T]], str]:
    def wrapped(values: Iterable[_T], /) -> str:
        return join_strs(map(serializer, values), sort=sort)

    return wrapped


@dataclass(kw_only=True, slots=True)
class _Case(Generic[_T]):
    param: ParamType
    name: str
    strategy: SearchStrategy[_T]
    serialize: Callable[[_T], str]
    failable: bool = False


class TestParameters:
    cases: ClassVar[list[_Case]] = [
        _Case(
            param=Date(),
            name="DATE",
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
            name="enum[_exampleenum]",
            strategy=sampled_from(_ExampleEnum),
            serialize=attrgetter("name"),
            failable=True,
        ),
        _Case(
            param=Freq(),
            name="freq",
            strategy=freqs(),
            serialize=utilities.whenever.Freq.serialize,
            failable=True,
        ),
        _Case(
            param=FrozenSetChoices(["a", "b", "c"]),
            name="frozenset[choice(['a', 'b', 'c'])]",
            strategy=frozensets(sampled_from(["a", "b", "c"])),
            serialize=_lift_serializer(str, sort=True),
            failable=True,
        ),
        _Case(
            param=FrozenSetEnums(_ExampleEnum),
            name="frozenset[enum[_exampleenum]]",
            strategy=frozensets(sampled_from(_ExampleEnum)),
            serialize=_lift_serializer(attrgetter("name"), sort=True),
            failable=True,
        ),
        _Case(
            param=FrozenSetStrs(),
            name="frozenset[string]",
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
            param=ListEnums(_ExampleEnum),
            name="list[enum[_exampleenum]]",
            strategy=lists(sampled_from(_ExampleEnum)),
            serialize=_lift_serializer(attrgetter("name")),
            failable=True,
        ),
        _Case(
            param=Month(),
            name="month",
            strategy=months(),
            serialize=utilities.whenever.Month.format_common_iso,
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
            param=ZonedDateTime(),
            name="zoned date-time",
            strategy=zoned_datetimes(),
            serialize=whenever.ZonedDateTime.format_common_iso,
            failable=True,
        ),
    ]
    cases_odl: ClassVar[list[ParameterSet]] = [
        param(Date(), "DATE", dates(), whenever.Date.format_common_iso, True),
        param(
            DateDelta(),
            "DATE DELTA",
            date_deltas(parsable=True),
            whenever.DateDelta.format_common_iso,
            True,
        ),
        param(
            DateTimeDelta(),
            "DATE-TIME DELTA",
            date_time_deltas(parsable=True),
            whenever.DateTimeDelta.format_common_iso,
            True,
        ),
        param(
            Enum(_ExampleEnum),
            "ENUM[_ExampleEnum]",
            sampled_from(_ExampleEnum),
            attrgetter("name"),
            True,
        ),
        param(Freq(), "FREQ", freqs(), utilities.whenever.Freq.serialize, True),
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
            FrozenSetStrs(),
            "FROZENSET[STRING]",
            frozensets(text_ascii()),
            _lift_serializer(str, sort=True),
            False,
        ),
        param(IPv4Address(), "IPV4 ADDRESS", ip_addresses(v=4), str, True),
        param(IPv6Address(), "IPV6 ADDRESS", ip_addresses(v=6), str, True),
        param(
            ListEnums(_ExampleEnum),
            "LIST[ENUM[_ExampleEnum]]",
            lists(sampled_from(_ExampleEnum)),
            _lift_serializer(attrgetter("name")),
            True,
        ),
        param(
            Month(), "MONTH", months(), utilities.whenever.Month.format_common_iso, True
        ),
        param(
            PlainDateTime(),
            "PLAIN DATE-TIME",
            plain_datetimes(),
            whenever.PlainDateTime.format_common_iso,
            True,
        ),
        param(Time(), "TIME", times(), whenever.Time.format_common_iso, True),
        param(
            TimeDelta(),
            "TIME-DELTA",
            time_deltas(),
            whenever.TimeDelta.format_common_iso,
            True,
        ),
        param(
            ZonedDateTime(),
            "ZONED DATE-TIME",
            zoned_datetimes(),
            whenever.ZonedDateTime.format_common_iso,
            True,
        ),
    ]

    @given(data=data(), use_value=booleans())
    @mark.parametrize(
        ("param", "exp_repr", "strategy", "serialize", "failable"), cases_odl
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

    @given(data=data(), use_value=booleans())
    @mark.parametrize(
        ("param", "exp_repr", "strategy", "serialize", "failable"), cases_odl
    )
    def test_default(
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
        default = data.draw(strategy)

        @command()
        @option("--value", type=param, default=default)
        def cli(*, value: Any) -> None:
            echo(f"value = {serialize(value)}")

        result = CliRunner().invoke(cli, args=[])
        assert result.exit_code == 0
        expected_str = serialize(default)
        assert result.stdout == f"value = {expected_str}\n"

    @given(data=data(), use_value=booleans())
    @mark.parametrize(
        ("param", "exp_repr", "strategy", "serialize", "failable"), cases_odl
    )
    def test_cli_value(
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
        @command()
        @option("--value", type=param)
        def cli(*, value: Any) -> None:
            echo(f"value = {serialize(value)}")

        value = data.draw(strategy)
        ser = serialize(value)
        result = CliRunner().invoke(cli, args=[f"--value={ser}"])
        assert result.exit_code == 0
        assert result.stdout == f"value = {ser}\n"

    @given(data=data(), use_value=booleans())
    @mark.parametrize(
        ("param", "exp_repr", "strategy", "serialize", "failable"), cases_odl
    )
    def test_cli_fail(
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
        if not failable:
            return

        @command()
        @option("--value", type=param)
        def cli(*, value: Any) -> None:
            echo(f"value = {serialize(value)}")

        result = CliRunner().invoke(cli, args=["--value=invalid"])
        assert result.exit_code == 2

    @given(data=data(), use_value=booleans())
    @mark.parametrize(
        ("param", "exp_repr", "strategy", "serialize", "failable"), cases_odl
    )
    @mark.skip
    def test_name(
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
        assert param.name == exp_repr

    @given(data=data(), use_value=booleans())
    @mark.parametrize(
        ("param", "exp_repr", "strategy", "serialize", "failable"), cases_odl
    )
    @mark.skip
    def test_repr(
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
        assert repr(param) == exp_repr.upper()

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
