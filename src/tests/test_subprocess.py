from __future__ import annotations

import sys  # do use `from sys import ...`
from re import escape, search
from subprocess import CalledProcessError
from typing import Any, ClassVar, cast

from loguru import logger
from pytest import CaptureFixture, raises

from utilities.loguru import HandlerConfiguration, LogLevel
from utilities.pytest import skipif_windows
from utilities.subprocess import stream_command


class TestStreamCommand:
    datetime: ClassVar[str] = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \| "

    @skipif_windows
    def test_main(self, *, capsys: CaptureFixture) -> None:
        handler: HandlerConfiguration = {"sink": sys.stdout, "level": LogLevel.TRACE}
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])

        output = stream_command(  # noqa: S604
            'echo "stdout message" && sleep 0.1 && echo "stderr message" >&2',
            shell=True,
        )
        assert output.returncode == 0
        assert output.stdout == "stdout message\n"
        assert output.stderr == "stderr message\n"

        out = capsys.readouterr().out
        line1, line2 = out.splitlines()
        expected1 = (
            self.datetime
            + r"INFO     \| utilities\.subprocess:_stream_command_write:\d+ - stdout message$"
        )
        assert search(expected1, line1), line1
        expected2 = (
            self.datetime
            + r"ERROR    \| utilities\.subprocess:_stream_command_write:\d+ - stderr message$"
        )
        assert search(expected2, line2), line2

    @skipif_windows
    def test_error(self) -> None:
        with raises(
            CalledProcessError,
            match=escape("Command '['false']' returned non-zero exit status 1."),
        ):
            _ = stream_command(["false"])


if __name__ == "__main__":
    _ = stream_command(  # noqa: S604
        'echo "stdout message" && sleep 2 && echo "stderr message" >&2', shell=True
    )
