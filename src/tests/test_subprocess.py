from __future__ import annotations

from subprocess import CalledProcessError
from typing import TYPE_CHECKING

from pytest import raises

from utilities.subprocess import run

if TYPE_CHECKING:
    from pytest import CaptureFixture


class TestRun:
    def test_main(self, *, capsys: CaptureFixture) -> None:
        result = run("echo stdout; echo stderr 1>&2", shell=True)  # noqa: S604
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == ""
        assert cap.err == ""

    def test_print(self, *, capsys: CaptureFixture) -> None:
        result = run("echo stdout; echo stderr 1>&2", shell=True, print=True)  # noqa: S604
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == "stdout\n"
        assert cap.err == "stderr\n"

    def test_print_stdout(self, *, capsys: CaptureFixture) -> None:
        result = run("echo stdout; echo stderr 1>&2", shell=True, print_stdout=True)  # noqa: S604
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == "stdout\n"
        assert cap.err == ""

    def test_print_stderr(self, *, capsys: CaptureFixture) -> None:
        result = run("echo stdout; echo stderr 1>&2", shell=True, print_stderr=True)  # noqa: S604
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == ""
        assert cap.err == "stderr\n"

    def test_return(self, *, capsys: CaptureFixture) -> None:
        result = run("echo stdout; echo stderr 1>&2", shell=True, return_=True)  # noqa: S604
        expected = "stdout\nstderr\n"
        assert result == expected
        cap = capsys.readouterr()
        assert cap.out == ""
        assert cap.err == ""

    def test_return_stdout(self, *, capsys: CaptureFixture) -> None:
        result = run("echo stdout; echo stderr 1>&2", shell=True, return_stdout=True)  # noqa: S604
        expected = "stdout\n"
        assert result == expected
        cap = capsys.readouterr()
        assert cap.out == ""
        assert cap.err == ""

    def test_return_stderr(self, *, capsys: CaptureFixture) -> None:
        result = run("echo stdout; echo stderr 1>&2", shell=True, return_stderr=True)  # noqa: S604
        expected = "stderr\n"
        assert result == expected
        cap = capsys.readouterr()
        assert cap.out == ""
        assert cap.err == ""

    def test_error(self, *, capsys: CaptureFixture) -> None:
        with raises(CalledProcessError) as exc_info:
            _ = run("echo stdout; echo stderr 1>&2; exit 1", shell=True)  # noqa: S604
        assert exc_info.value.returncode == 1
        assert exc_info.value.stdout == "stdout\n"
        assert exc_info.value.stderr == "stderr\n"
        cap = capsys.readouterr()
        assert cap.out == ""
        assert cap.err == ""

    def test_error_and_print(self, *, capsys: CaptureFixture) -> None:
        with raises(CalledProcessError) as exc_info:
            _ = run("echo stdout; echo stderr 1>&2; exit 1", shell=True, print=True)  # noqa: S604
        assert exc_info.value.returncode == 1
        assert exc_info.value.stdout == "stdout\n"
        assert exc_info.value.stderr == "stderr\n"
        cap = capsys.readouterr()
        assert cap.out == "stdout\n"
        assert cap.err == "stderr\n"
