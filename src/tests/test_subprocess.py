from __future__ import annotations

from pathlib import Path
from subprocess import CalledProcessError
from typing import TYPE_CHECKING

from pytest import LogCaptureFixture, mark, param, raises

from utilities.iterables import one
from utilities.pytest import skipif_ci, skipif_mac, throttle
from utilities.subprocess import (
    BASH_LC,
    echo_cmd,
    expand_path,
    maybe_sudo_cmd,
    mkdir,
    mkdir_cmd,
    rm_cmd,
    run,
    ssh,
    ssh_cmd,
    touch_cmd,
    yield_ssh_temp_dir,
)
from utilities.text import strip_and_dedent, unique_str
from utilities.whenever import MINUTE

if TYPE_CHECKING:
    from pytest import CaptureFixture


class TestEchoCmd:
    def test_main(self) -> None:
        result = echo_cmd("'hello world'")
        expected = ["echo", "'hello world'"]
        assert result == expected


class TestExpandPath:
    def test_main(self) -> None:
        result = expand_path("~")
        expected = Path.home()
        assert result == expected

    def test_subs(self) -> None:
        result = expand_path("~/${dir}", subs={"dir": "foo"})
        expected = Path("~/foo").expanduser()
        assert result == expected


class TestMaybeSudoCmd:
    def test_main(self) -> None:
        result = maybe_sudo_cmd("echo", "hi")
        expected = ["echo", "hi"]
        assert result == expected

    def test_sudo(self) -> None:
        result = maybe_sudo_cmd("echo", "hi", sudo=True)
        expected = ["sudo", "echo", "hi"]
        assert result == expected


class TestMkDir:
    def test_main(self, *, tmp_path: Path) -> None:
        path = f"{tmp_path}/foo"
        mkdir(path)
        assert Path(path).is_dir()


class TestMkDirCmd:
    def test_main(self) -> None:
        result = mkdir_cmd("~/foo")
        expected = ["mkdir", "-p", "~/foo"]
        assert result == expected

    def test_parent(self) -> None:
        result = mkdir_cmd("~/foo", parent=True)
        expected = ["mkdir", "-p", "$(dirname ~/foo)"]
        assert result == expected


class TestRmCmd:
    def test_main(self) -> None:
        result = rm_cmd("~/foo")
        expected = ["rm", "-rf", "~/foo"]
        assert result == expected


class TestRun:
    def test_main(self, *, capsys: CaptureFixture) -> None:
        result = run("echo", "hi")
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == ""
        assert cap.err == ""

    @skipif_ci
    @skipif_mac
    def test_user(self, *, capsys: CaptureFixture) -> None:
        result = run("whoami", user="root", print=True)
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == "root\n"
        assert cap.err == ""

    @mark.parametrize("executable", [param("sh"), param("bash")])
    def test_executable(self, *, executable: str, capsys: CaptureFixture) -> None:
        result = run("echo $0", executable=executable, shell=True, print=True)  # noqa: S604
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == f"{executable}\n"
        assert cap.err == ""

    def test_shell(self, *, capsys: CaptureFixture) -> None:
        result = run("echo stdout; sleep 0.5; echo stderr 1>&2", shell=True, print=True)  # noqa: S604
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == "stdout\n"
        assert cap.err == "stderr\n"

    def test_cwd(self, *, capsys: CaptureFixture, tmp_path: Path) -> None:
        result = run("pwd", cwd=tmp_path, print=True)
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == f"{tmp_path}\n"
        assert cap.err == ""

    def test_env(self, *, capsys: CaptureFixture) -> None:
        result = run("env | grep KEY", env={"KEY": "value"}, shell=True, print=True)  # noqa: S604
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == "KEY=value\n"
        assert cap.err == ""

    def test_input_bash(self, *, capsys: CaptureFixture) -> None:
        input_ = strip_and_dedent("""
            key=value
            echo ${key}@stdout
            sleep 0.5
            echo ${key}@stderr 1>&2
        """)
        result = run(*BASH_LC, input_, print=True)
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == "value@stdout\n"
        assert cap.err == "value@stderr\n"

    def test_input_cat(self, *, capsys: CaptureFixture) -> None:
        input_ = strip_and_dedent("""
            foo
            bar
            baz
        """)
        result = run("cat", input=input_, print=True)
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == input_
        assert cap.err == ""

    def test_input_and_return(self, *, capsys: CaptureFixture) -> None:
        input_ = strip_and_dedent("""
            foo
            bar
            baz
        """)
        result = run("cat", input=input_, return_=True)
        assert result == input_
        cap = capsys.readouterr()
        assert cap.out == ""
        assert cap.err == ""

    def test_print(self, *, capsys: CaptureFixture) -> None:
        result = run("echo stdout; sleep 0.5; echo stderr 1>&2", shell=True, print=True)  # noqa: S604
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == "stdout\n"
        assert cap.err == "stderr\n"

    def test_print_stdout(self, *, capsys: CaptureFixture) -> None:
        result = run(  # noqa: S604
            "echo stdout; sleep 0.5; echo stderr 1>&2", shell=True, print_stdout=True
        )
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == "stdout\n"
        assert cap.err == ""

    def test_print_stderr(self, *, capsys: CaptureFixture) -> None:
        result = run(  # noqa: S604
            "echo stdout; sleep 0.5; echo stderr 1>&2", shell=True, print_stderr=True
        )
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == ""
        assert cap.err == "stderr\n"

    def test_return(self, *, capsys: CaptureFixture) -> None:
        result = run(  # noqa: S604
            "echo stdout; sleep 0.5; echo stderr 1>&2", shell=True, return_=True
        )
        expected = "stdout\nstderr"
        assert result == expected
        cap = capsys.readouterr()
        assert cap.out == ""
        assert cap.err == ""

    def test_return_stdout(self, *, capsys: CaptureFixture) -> None:
        result = run(  # noqa: S604
            "echo stdout; sleep 0.5; echo stderr 1>&2", shell=True, return_stdout=True
        )
        expected = "stdout"
        assert result == expected
        cap = capsys.readouterr()
        assert cap.out == ""
        assert cap.err == ""

    def test_return_stderr(self, *, capsys: CaptureFixture) -> None:
        result = run(  # noqa: S604
            "echo stdout; sleep 0.5; echo stderr 1>&2", shell=True, return_stderr=True
        )
        expected = "stderr"
        assert result == expected
        cap = capsys.readouterr()
        assert cap.out == ""
        assert cap.err == ""

    def test_print_and_return(self, *, capsys: CaptureFixture) -> None:
        result = run(  # noqa: S604
            "echo stdout; sleep 0.5; echo stderr 1>&2",
            shell=True,
            print=True,
            return_=True,
        )
        expected = "stdout\nstderr"
        assert result == expected
        cap = capsys.readouterr()
        assert cap.out == "stdout\n"
        assert cap.err == "stderr\n"

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

    def test_logger(self, *, caplog: LogCaptureFixture) -> None:
        name = unique_str()
        with raises(CalledProcessError):
            _ = run("echo stdout; echo stderr 1>&2; exit 1", shell=True, logger=name)  # noqa: S604
        record = one(r for r in caplog.records if r.name == name)
        expected = strip_and_dedent("""
'run' failed with:
 - cmd          = echo stdout; echo stderr 1>&2; exit 1
 - cmds_or_args = ()
 - user         = None
 - executable   = None
 - shell        = True
 - cwd          = None
 - env          = None
 - input        = None

-- stdout ---------------------------------------------------------------------
stdout
-------------------------------------------------------------------------------
-- stderr ---------------------------------------------------------------------
stderr
-------------------------------------------------------------------------------
""")
        assert record.message == expected


class TestSSH:
    @skipif_ci
    @throttle(delta=5 * MINUTE)
    def test_main(self, *, capsys: CaptureFixture) -> None:
        input_ = strip_and_dedent("""
            hostname
            whoami 1>&2
        """)
        result = ssh("root", "proxmox.main", input=input_, print=True)
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == "proxmox\n"
        assert cap.err == "root\n"


class TestSSHCmd:
    def test_main(self) -> None:
        result = ssh_cmd("user", "hostname", "true")
        expected = [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "HostKeyAlgorithms=ssh-ed25519",
            "-o",
            "StrictHostKeyChecking=yes",
            "-T",
            "user@hostname",
            "true",
        ]
        assert result == expected

    def test_batch_mode_disabled(self) -> None:
        result = ssh_cmd("user", "hostname", "true", batch_mode=False)
        expected = [
            "ssh",
            "-o",
            "HostKeyAlgorithms=ssh-ed25519",
            "-o",
            "StrictHostKeyChecking=yes",
            "-T",
            "user@hostname",
            "true",
        ]
        assert result == expected

    def test_host_key_algorithms(self) -> None:
        result = ssh_cmd(
            "user", "hostname", "true", host_key_algorithms=["rsa-sha-256"]
        )
        expected = [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "HostKeyAlgorithms=rsa-sha-256",
            "-o",
            "StrictHostKeyChecking=yes",
            "-T",
            "user@hostname",
            "true",
        ]
        assert result == expected

    def test_strict_host_key_checking_disabled(self) -> None:
        result = ssh_cmd("user", "hostname", "true", strict_host_key_checking=False)
        expected = [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "HostKeyAlgorithms=ssh-ed25519",
            "-T",
            "user@hostname",
            "true",
        ]
        assert result == expected


class TestTouchCmd:
    def test_main(self) -> None:
        result = touch_cmd("~/foo")
        expected = ["touch", "~/foo"]
        assert result == expected


class TestYieldDockerTempDir:
    @skipif_ci
    @throttle(delta=5 * MINUTE)
    def test_main(self) -> None:
        with yield_ssh_temp_dir("root", "proxmox.main") as temp_dir:
            raise_if_present = f"if [ -d {temp_dir} ]; then exit 1; fi"
            raise_if_missing = f"if ! [ -d {temp_dir} ]; then exit 1; fi"
            ssh("root", "proxmox.main", input=raise_if_missing)
            with raises(CalledProcessError):
                ssh("root", "proxmox.main", input=raise_if_present)
        ssh("root", "proxmox.main", input=raise_if_present)
        with raises(CalledProcessError):
            ssh("root", "proxmox.main", input=raise_if_missing)
