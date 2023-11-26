from __future__ import annotations

import sqlalchemy
from click import argument, command, echo, option
from click.testing import CliRunner
from hypothesis import given
from hypothesis.strategies import DataObject, data

import utilities.click
from utilities.hypothesis import sqlite_engines
from utilities.sqlalchemy import serialize_engine


class TestEngineParameter:
    @given(data=data())
    def test_argument(self, *, data: DataObject) -> None:
        runner = CliRunner()

        @command()
        @argument("engine", type=utilities.click.Engine())
        def cli(*, engine: sqlalchemy.Engine) -> None:
            echo(f"engine = {serialize_engine(engine)}")

        engine_str = serialize_engine(data.draw(sqlite_engines()))
        result = CliRunner().invoke(cli, [engine_str])
        assert result.exit_code == 0
        assert result.stdout == f"engine = {engine_str}\n"

        result = runner.invoke(cli, ["error"])
        assert result.exit_code == 2

    @given(data=data())
    def test_option(self, data: DataObject) -> None:
        engine = data.draw(sqlite_engines())

        @command()
        @option("--engine", type=utilities.click.Engine(), default=engine)
        def cli(*, engine: sqlalchemy.Engine) -> None:
            echo(f"engine = {serialize_engine(engine)}")

        result = CliRunner().invoke(cli)
        assert result.exit_code == 0
        assert result.stdout == f"engine = {serialize_engine(engine)}\n"
