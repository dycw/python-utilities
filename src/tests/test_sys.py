from __future__ import annotations

import sys
from subprocess import CalledProcessError, check_output

from pytest import raises

from utilities.sys import VERSION_MAJOR_MINOR


class TestExceptHook:
    def test_custom_excepthook(self) -> None:
        code = """
from sys import stdout
from io import StringIO
from logging import getLogger
from utilities.sys import log_exception_paths

sys.excepthook = log_exception_paths
_LOGGER = getLogger(__name__)
_LOGGER.addHandler(StreamHandler(stdout))


raise ValueError("Test exception")
    """

        # Run the code in a subprocess and capture the output
        with raises(CalledProcessError) as exc_info:
            _ = check_output([sys.executable, "-c", code], text=True)
        error = exc_info.value
        assert "Custom exception hook called" in error.stdout
        assert "Exception value: Test exception" in error.stdout


class TestVersionMajorMinor:
    def test_main(self) -> None:
        assert isinstance(VERSION_MAJOR_MINOR, tuple)
        expected = 2
        assert len(VERSION_MAJOR_MINOR) == expected
