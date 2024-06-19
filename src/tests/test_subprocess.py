from __future__ import annotations

from pathlib import Path
from re import search
from subprocess import CalledProcessError, check_call

from pytest import raises

from utilities.pytest import skipif_windows
from utilities.subprocess import (
    GetShellOutputError,
    _address_already_in_use_pattern,
    get_shell_output,
    tabulate_called_process_error,
)
from utilities.text import strip_and_dedent


class TestAddressAlreadyInUsePattern:
    def test_pattern(self) -> None:
        pattern = _address_already_in_use_pattern()
        text = "OSError: [Errno 98] Address already in use"
        assert search(pattern, text) is not None


class TestGetShellOutput:
    @skipif_windows
    def test_main(self) -> None:
        output = get_shell_output("ls")
        assert any(line == "pyproject.toml" for line in output.splitlines())

    @skipif_windows
    def test_activate(self, *, tmp_path: Path) -> None:
        venv = Path(tmp_path, ".venv")
        activate = Path(venv, "activate")
        activate.parent.mkdir(parents=True)
        activate.touch()
        _ = get_shell_output("ls", cwd=venv, activate=venv)

    def test_no_activate(self, *, tmp_path: Path) -> None:
        venv = Path(tmp_path, ".venv")
        with raises(GetShellOutputError):
            _ = get_shell_output("ls", cwd=venv, activate=venv)


class TestTabulateCalledProcessError:
    @skipif_windows
    def test_main(self) -> None:
        def which() -> None:
            _ = check_call(["which"], text=True)

        try:
            which()
        except CalledProcessError as error:
            result = tabulate_called_process_error(error)
            expected = """
                cmd        ['which']
                returncode 1
                stdout     None
                stderr     None
            """
            assert result == strip_and_dedent(expected)
        else:
            with raises(CalledProcessError):
                which()
