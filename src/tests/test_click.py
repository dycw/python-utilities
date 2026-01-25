from __future__ import annotations

import enum
from dataclasses import dataclass
from enum import StrEnum, auto, unique
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
    booleans,
    data,
    frozensets,
    integers,
    ip_addresses,
    lists,
    sampled_from,
    uuids,
)
from pytest import mark, param

import utilities.click
from utilities.click import (
    _CONTEXT_SETTINGS_INNER,
    _MAX_CONTENT_WIDTH,
    CONTEXT_SETTINGS,
    UUID,
    Bool,
    Date,
    DateDelta,
    DateTimeDelta,
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
    Str,
    Time,
    TimeDelta,
    YearMonth,
    ZonedDateTime,
)
from utilities.core import get_class_name, normalize_multi_line_str
from utilities.hypothesis import (
    date_deltas,
    date_time_deltas,
    dates,
    month_days,
    paths,
    plain_date_times,
    text_ascii,
    time_deltas,
    times,
    year_months,
    zoned_date_times,
)
from utilities.parse import serialize_object
from utilities.text import join_strs

if TYPE_CHECKING:
    import pathlib
    from collections.abc import Callable, Iterable


@unique
class _ExampleEnum(enum.Enum):
    a = auto()
    b = auto()
    c = auto()


@unique
class _ExampleStrEnum(StrEnum):
    ak = "av"
    bk = "bv"
    ck = "cv"


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


##


class TestCLIHelp:
    @mark.parametrize(
        ("param", "expected"),
        [
            param(
                str,
                """
                Usage: cli [OPTIONS] VALUE

                Options:
                  --help  Show this message and exit.
                """,
            ),
            param(
                utilities.click.Enum(_ExampleEnum),
                """
                Usage: cli [OPTIONS] {a,b,c}

                Options:
                  --help  Show this message and exit.
                """,
            ),
            param(
                utilities.click.Enum(_ExampleEnum, value=True),
                """
                Usage: cli [OPTIONS] {1,2,3}

                Options:
                  --help  Show this message and exit.
                """,
            ),
            param(
                utilities.click.Enum(_ExampleStrEnum),
                """
                Usage: cli [OPTIONS] {av,bv,cv}

                Options:
                  --help  Show this message and exit.
                """,
            ),
            param(
                FrozenSetEnums(_ExampleEnum),
                """
                Usage: cli [OPTIONS] {FROZENSET{a,b,c} SEP=,}

                Options:
                  --help  Show this message and exit.
                """,
            ),
            param(
                FrozenSetStrs(),
                """
                Usage: cli [OPTIONS] {FROZENSET[TEXT] SEP=,}

                Options:
                  --help  Show this message and exit.
                """,
            ),
            param(
                ListEnums(_ExampleEnum),
                """
                Usage: cli [OPTIONS] {LIST{a,b,c} SEP=,}

                Options:
                  --help  Show this message and exit.
                """,
            ),
            param(
                ListStrs(),
                """
                Usage: cli [OPTIONS] {LIST[TEXT] SEP=,}

                Options:
                  --help  Show this message and exit.
                """,
            ),
        ],
    )
    def test_main(self, *, param: Any, expected: str) -> None:
        @command()
        @argument("value", type=param)
        def cli(*, value: Any) -> None:
            echo(f"value = {value}")

        result = CliRunner().invoke(cli, ["--help"])
        assert result.exit_code == 0, result.stderr
        expected = normalize_multi_line_str(expected)
        assert result.stdout == expected


class TestContextSettings:
    def test_max_content_width(self) -> None:
        @command(
            context_settings={
                "terminal_width": _MAX_CONTENT_WIDTH,
                **_CONTEXT_SETTINGS_INNER,
            }
        )
        @option(
            "--flag",
            help="Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.",
        )
        def cli(*, flag: bool) -> None:
            _ = flag

        result = CliRunner().invoke(cli, ["--help"])
        assert result.exit_code == 0, result.stderr
        expected = """\
Usage: cli [OPTIONS]

Options:
  --flag TEXT  Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et
               dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip
               ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu
               fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt
               mollit anim id est laborum.
  -h, --help   Show this message and exit.
"""
        assert result.stdout == expected

    @given(help_=sampled_from(["-h", "--help"]))
    def test_help_option_names(self, *, help_: str) -> None:
        @command(**CONTEXT_SETTINGS)
        def cli() -> None: ...

        result = CliRunner().invoke(cli, [help_])
        assert result.exit_code == 0, result.stderr

    def test_show_default(self) -> None:
        @command(**CONTEXT_SETTINGS)
        @option("--flag", default=False)
        def cli(*, flag: bool) -> None:
            _ = flag

        result = CliRunner().invoke(cli, ["--help"])
        assert result.exit_code == 0, result.stderr
        expected = """\
Usage: cli [OPTIONS]

Options:
  --flag BOOLEAN  [default: False]
  -h, --help      Show this message and exit.
"""
        assert result.stdout == expected


class TestEnum:
    def test_error(self) -> None:
        @command()
        @option(
            "--value",
            type=utilities.click.Enum(_ExampleEnum),
            default=_ExampleStrEnum.ak,
        )
        def cli(*, value: list[_ExampleEnum] | frozenset[_ExampleEnum]) -> None:
            _ = value

        result = CliRunner().invoke(cli)
        assert result.exit_code == 2, result.stderr
        assert search(
            "Invalid value for '--value': Enum member 'ak' of type '_ExampleStrEnum' is not an instance of '_ExampleEnum'",
            result.stderr,
        )


class TestFrozenSetAndList:
    @mark.parametrize(
        ("param", "default"),
        [
            param(ListEnums(_ExampleEnum), [], id=get_class_name(ListEnums)),
            param(FrozenSetEnums(_ExampleEnum), {}, id=get_class_name(FrozenSetEnums)),
        ],
        ids=str,
    )
    def test_empty(self, *, param: ParamType, default: Any) -> None:
        @command()
        @option("--value", type=param, default=default)
        def cli(*, value: list[_ExampleEnum] | frozenset[_ExampleEnum]) -> None:
            assert value is None

        result = CliRunner().invoke(cli)
        assert result.exit_code == 0, result.stderr

    @mark.parametrize(
        "param",
        [
            param(ListEnums(_ExampleEnum), id=get_class_name(ListEnums)),
            param(FrozenSetEnums(_ExampleEnum), id=get_class_name(FrozenSetEnums)),
        ],
        ids=str,
    )
    def test_error(self, *, param: ParamType) -> None:
        @command()
        @option("--value", type=param, default=0)
        def cli(*, value: list[_ExampleEnum] | frozenset[_ExampleEnum]) -> None:
            _ = value

        result = CliRunner().invoke(cli)
        assert result.exit_code == 2, result.stderr
        assert search(
            "Invalid value for '--value': Object '0' of type 'int' must be a (frozenset|list)",
            result.stderr,
        )


class TestParameters:
    cases: ClassVar[list[_Case]] = [
        _Case(
            param=Bool(), name="bool", strategy=booleans(), serialize=str, failable=True
        ),
        _Case(
            param=Date(),
            name="date",
            strategy=dates(),
            serialize=whenever.Date.format_iso,
            failable=True,
        ),
        _Case(
            param=DateDelta(),
            name="date delta",
            strategy=date_deltas(parsable=True),
            serialize=whenever.DateDelta.format_iso,
            failable=True,
        ),
        _Case(
            param=DateTimeDelta(),
            name="date-time delta",
            strategy=date_time_deltas(parsable=True),
            serialize=whenever.DateTimeDelta.format_iso,
            failable=True,
        ),
        _Case(
            param=utilities.click.Enum(_ExampleEnum),
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
            strategy=frozensets(integers(), min_size=1),
            serialize=_lift_serializer(str, sort=True),
            failable=True,
        ),
        _Case(
            param=FrozenSetChoices(["a", "b", "c"]),
            name="frozenset[choice]",
            repr="FROZENSET[Choice(['a', 'b', 'c'])]",
            strategy=frozensets(sampled_from(["a", "b", "c"]), min_size=1),
            serialize=_lift_serializer(str, sort=True),
            failable=True,
        ),
        _Case(
            param=FrozenSetEnums(_ExampleEnum),
            name="frozenset[enum[_ExampleEnum]]",
            repr="FROZENSET[ENUM[_ExampleEnum]]",
            strategy=frozensets(sampled_from(_ExampleEnum), min_size=1),
            serialize=_lift_serializer(attrgetter("name"), sort=True),
            failable=True,
        ),
        _Case(
            param=FrozenSetStrs(),
            name="frozenset[text]",
            repr="FROZENSET[STRING]",
            strategy=frozensets(text_ascii(), min_size=1),
            serialize=_lift_serializer(str, sort=True),
        ),
        _Case(
            param=IPv4Address(),
            name="ipv4",
            strategy=ip_addresses(v=4),
            serialize=str,
            failable=True,
        ),
        _Case(
            param=IPv6Address(),
            name="ipv6",
            strategy=ip_addresses(v=6),
            serialize=str,
            failable=True,
        ),
        _Case(
            param=ListChoices(["a", "b", "c"]),
            name="list[choice]",
            repr="LIST[Choice(['a', 'b', 'c'])]",
            strategy=lists(sampled_from(["a", "b", "c"]), min_size=1),
            serialize=_lift_serializer(str),
            failable=True,
        ),
        _Case(
            param=ListInts(),
            name="list[integer]",
            repr="LIST[INT]",
            strategy=lists(integers(), min_size=1),
            serialize=_lift_serializer(str),
            failable=True,
        ),
        _Case(
            param=ListEnums(_ExampleEnum),
            name="list[enum[_ExampleEnum]]",
            repr="LIST[ENUM[_ExampleEnum]]",
            strategy=lists(sampled_from(_ExampleEnum), min_size=1),
            serialize=_lift_serializer(attrgetter("name")),
            failable=True,
        ),
        _Case(
            param=MonthDay(),
            name="month-day",
            strategy=month_days(),
            serialize=whenever.MonthDay.format_iso,
            failable=True,
        ),
        _Case(
            param=utilities.click.Path(),
            name="path",
            strategy=paths(min_depth=1),
            serialize=str,
            failable=False,
        ),
        _Case(
            param=PlainDateTime(),
            name="plain date-time",
            strategy=plain_date_times(),
            serialize=whenever.PlainDateTime.format_iso,
            failable=True,
        ),
        _Case(
            param=Str(),
            name="text",
            strategy=text_ascii(min_size=1),
            serialize=serialize_object,
            failable=False,
        ),
        _Case(
            param=Time(),
            name="time",
            strategy=times(),
            serialize=whenever.Time.format_iso,
            failable=True,
        ),
        _Case(
            param=TimeDelta(),
            name="time-delta",
            strategy=time_deltas(),
            serialize=whenever.TimeDelta.format_iso,
            failable=True,
        ),
        _Case(
            param=UUID(), name="uuid", strategy=uuids(), serialize=str, failable=True
        ),
        _Case(
            param=YearMonth(),
            name="year-month",
            strategy=year_months(),
            serialize=whenever.YearMonth.format_iso,
            failable=True,
        ),
        _Case(
            param=ZonedDateTime(),
            name="zoned date-time",
            strategy=zoned_date_times(),
            serialize=whenever.ZonedDateTime.format_iso,
            failable=True,
        ),
    ]

    @given(data=data())
    @mark.parametrize(
        ("param", "strategy", "serialize"),
        [
            param(c.param, c.strategy, c.serialize, id=get_class_name(c.param))
            for c in cases
        ],
    )
    def test_main(
        self,
        *,
        data: DataObject,
        param: ParamType,
        strategy: SearchStrategy[Any],
        serialize: Callable[[Any], str],
    ) -> None:
        value_use = data.draw(strategy)

        @command()
        @option("--value", type=param)
        def cli(*, value: Any) -> None:
            assert value == value_use

        result = CliRunner().invoke(cli, args=[f"--value={serialize(value_use)}"])
        assert result.exit_code == 0, result.stderr

    @given(data=data())
    @mark.parametrize(
        ("param", "strategy"),
        [param(c.param, c.strategy, id=get_class_name(c.param)) for c in cases],
    )
    def test_default(
        self, *, data: DataObject, param: ParamType, strategy: SearchStrategy[Any]
    ) -> None:
        default = data.draw(strategy)

        @command()
        @option("--value", type=param, default=default)
        def cli(*, value: Any) -> None:
            assert value == default

        result = CliRunner().invoke(cli)
        assert result.exit_code == 0, result.stderr

    @mark.parametrize(
        "param", [param(c.param, id=get_class_name(c.param)) for c in cases]
    )
    def test_empty_string(self, *, param: ParamType) -> None:
        @command()
        @argument("value", type=param)
        def cli(*, value: Any) -> None:
            assert value is None

        result = CliRunner().invoke(cli, args=[""])
        assert result.exit_code == 0, result.stderr

    @mark.parametrize(
        "param", [param(c.param, id=get_class_name(c.param)) for c in cases]
    )
    def test_none(self, *, param: ParamType) -> None:
        @command()
        @option("--value", type=param, default=None)
        def cli(*, value: Any) -> None:
            assert value is None

        result = CliRunner().invoke(cli)
        assert result.exit_code == 0, result.stderr

    @mark.parametrize(
        "param",
        [param(c.param, id=get_class_name(c.param)) for c in cases if c.failable],
    )
    def test_cli_fail(self, *, param: ParamType) -> None:
        @command()
        @argument("value", type=param)
        def cli(*, value: Any) -> None:
            _ = value

        result = CliRunner().invoke(cli, args=["invalid"])
        assert result.exit_code == 2, result.stderr
        assert search("Invalid value for '.*':.*'invalid'", result.stderr)

    @mark.parametrize(
        ("param", "name"),
        [param(c.param, c.name, id=get_class_name(c.param)) for c in cases],
    )
    def test_name(self, *, param: ParamType, name: str) -> None:
        assert param.name == name

    @mark.parametrize(
        ("param", "repr_", "name"),
        [param(c.param, c.repr, c.name, id=get_class_name(c.param)) for c in cases],
    )
    def test_repr(self, *, param: ParamType, repr_: str | None, name: str) -> None:
        expected = name.upper() if repr_ is None else repr_
        assert repr(param) == expected


class TestPath:
    def test_exists(self, *, temp_file: pathlib.Path) -> None:
        @command()
        @option("--path", type=utilities.click.Path(exist=True), default=temp_file)
        def cli(*, path: pathlib.Path) -> None:
            assert path.exists()

        result = CliRunner().invoke(cli)
        assert result.exit_code == 0, result.stderr

    def test_not_exist(self, *, temp_path_not_exist: pathlib.Path) -> None:
        @command()
        @option(
            "--path",
            type=utilities.click.Path(exist=False),
            default=temp_path_not_exist,
        )
        def cli(*, path: pathlib.Path) -> None:
            assert not path.exists()

        result = CliRunner().invoke(cli)
        assert result.exit_code == 0, result.stderr

    def test_existing_file(self, *, temp_file: pathlib.Path) -> None:
        @command()
        @option(
            "--path",
            type=utilities.click.Path(exist="existing file"),
            default=temp_file,
        )
        def cli(*, path: pathlib.Path) -> None:
            assert path.is_file()

        result = CliRunner().invoke(cli)
        assert result.exit_code == 0, result.stderr

    def test_existing_dir(self, *, tmp_path: pathlib.Path) -> None:
        @command()
        @option(
            "--path", type=utilities.click.Path(exist="existing dir"), default=tmp_path
        )
        def cli(*, path: pathlib.Path) -> None:
            assert path.is_dir()

        result = CliRunner().invoke(cli)
        assert result.exit_code == 0, result.stderr

    @mark.parametrize("exists", [param(False), param(True)])
    def test_file_if_exists(
        self, *, temp_path_not_exist: pathlib.Path, exists: bool
    ) -> None:
        if exists:
            temp_path_not_exist.touch()

        @command()
        @option(
            "--path",
            type=utilities.click.Path(exist="file if exists"),
            default=temp_path_not_exist,
        )
        def cli(*, path: pathlib.Path) -> None:
            assert path.is_file() or not path.exists()

        result = CliRunner().invoke(cli)
        assert result.exit_code == 0, result.stderr

    @mark.parametrize("exists", [param(False), param(True)])
    def test_dir_if_exists(
        self, *, temp_path_not_exist: pathlib.Path, exists: bool
    ) -> None:
        if exists:
            temp_path_not_exist.mkdir()

        @command()
        @option(
            "--path",
            type=utilities.click.Path(exist="dir if exists"),
            default=temp_path_not_exist,
        )
        def cli(*, path: pathlib.Path) -> None:
            assert path.is_dir() or not path.exists()

        result = CliRunner().invoke(cli)
        assert result.exit_code == 0, result.stderr

    def test_error_exists(self, *, temp_path_not_exist: pathlib.Path) -> None:
        @command()
        @option(
            "--path", type=utilities.click.Path(exist=True), default=temp_path_not_exist
        )
        def cli(*, path: pathlib.Path) -> None:
            _ = path

        result = CliRunner().invoke(cli)
        assert result.exit_code == 2, result.stderr
        assert search("Invalid value for '--path': '.*' does not exist", result.stderr)

    def test_error_not_exist(self, *, temp_file: pathlib.Path) -> None:
        @command()
        @option("--path", type=utilities.click.Path(exist=False), default=temp_file)
        def cli(*, path: pathlib.Path) -> None:
            _ = path

        result = CliRunner().invoke(cli)
        assert result.exit_code == 2, result.stderr
        assert search("Invalid value for '--path': '.*' exists", result.stderr)

    def test_error_existing_file(self, *, tmp_path: pathlib.Path) -> None:
        @command()
        @option(
            "--path", type=utilities.click.Path(exist="existing file"), default=tmp_path
        )
        def cli(*, path: pathlib.Path) -> None:
            _ = path

        result = CliRunner().invoke(cli)
        assert result.exit_code == 2, result.stderr
        assert search("Invalid value for '--path': '.*' is not a file", result.stderr)

    def test_error_existing_dir(self, *, temp_file: pathlib.Path) -> None:
        @command()
        @option(
            "--path", type=utilities.click.Path(exist="existing dir"), default=temp_file
        )
        def cli(*, path: pathlib.Path) -> None:
            _ = path

        result = CliRunner().invoke(cli)
        assert result.exit_code == 2, result.stderr
        assert search(
            "Invalid value for '--path': '.*' is not a directory", result.stderr
        )

    def test_error_file_if_exists(self, *, tmp_path: pathlib.Path) -> None:
        @command()
        @option(
            "--path",
            type=utilities.click.Path(exist="file if exists"),
            default=tmp_path,
        )
        def cli(*, path: pathlib.Path) -> None:
            _ = path

        result = CliRunner().invoke(cli)
        assert result.exit_code == 2, result.stderr
        assert search(
            "Invalid value for '--path': '.*' exists but is not a file", result.stderr
        )

    def test_error_dir_if_exists(self, *, temp_file: pathlib.Path) -> None:
        @command()
        @option(
            "--path",
            type=utilities.click.Path(exist="dir if exists"),
            default=temp_file,
        )
        def cli(*, path: pathlib.Path) -> None:
            _ = path

        result = CliRunner().invoke(cli)
        assert result.exit_code == 2, result.stderr
        assert search(
            "Invalid value for '--path': '.*' exists but is not a directory",
            result.stderr,
        )
