from typing import Optional

from click import command, echo
from click.testing import CliRunner
from hypothesis import given
from hypothesis.strategies import integers, none
from pytest import mark, param

from utilities.click.luigi import (
    local_scheduler_option_default_central,
    local_scheduler_option_default_local,
    workers_option,
)


class TestLocalSchedulerOption:
    @mark.parametrize(
        ("args", "expected"),
        [param([], True), param(["-ls"], True), param(["-nls"], False)],
    )
    def test_default_local(self, args: list[str], expected: bool) -> None:
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
    def test_default_central(self, args: list[str], expected: bool) -> None:
        @command()
        @local_scheduler_option_default_central
        def cli(*, local_scheduler: bool) -> None:
            echo(f"local_scheduler = {local_scheduler}")

        result = CliRunner().invoke(cli, args)
        assert result.exit_code == 0
        assert result.stdout == f"local_scheduler = {expected}\n"


class TestWorkersOption:
    @given(workers=integers() | none())
    def test_main(self, workers: Optional[int]) -> None:
        @command()
        @workers_option
        def cli(*, workers: Optional[int]) -> None:
            echo(f"workers = {workers}")

        args = [] if workers is None else ["--workers", str(workers)]
        result = CliRunner().invoke(cli, args)
        assert result.exit_code == 0
        assert result.stdout == f"workers = {workers}\n"
