from __future__ import annotations

import re
from logging import INFO, getLogger
from re import MULTILINE, Pattern, search
from typing import TYPE_CHECKING

from pytest import CaptureFixture, LogCaptureFixture, mark, param, raises

from utilities.constants import MINUTE
from utilities.core import one, unique_str
from utilities.docker import (
    _docker_compose_cmd,
    docker_compose_down_cmd,
    docker_compose_pull_cmd,
    docker_compose_up_cmd,
    docker_cp,
    docker_cp_cmd,
    docker_exec,
    docker_exec_cmd,
    yield_docker_temp_dir,
)
from utilities.pytest import skipif_ci, throttle_test
from utilities.subprocess import BASH_LS, RunError, touch_cmd

if TYPE_CHECKING:
    from pathlib import Path

    from utilities.types import PathLike


class TestDockerComposeCmd:
    def test_main(self) -> None:
        result = _docker_compose_cmd("cmd")
        expected = ["docker", "compose", "cmd"]
        assert result == expected

    def test_single_file(self) -> None:
        result = _docker_compose_cmd("cmd", files="compose.yaml")
        expected = ["docker", "compose", "--file", "compose.yaml", "cmd"]
        assert result == expected

    def test_multiple_files(self) -> None:
        result = _docker_compose_cmd("cmd", files=["compose1.yaml", "compose2.yaml"])
        expected = [
            "docker",
            "compose",
            "--file",
            "compose1.yaml",
            "--file",
            "compose2.yaml",
            "cmd",
        ]
        assert result == expected

    def test_args(self) -> None:
        result = _docker_compose_cmd("cmd", "arg")
        expected = ["docker", "compose", "cmd", "arg"]
        assert result == expected


class TestDockerComposeDownCmd:
    def test_main(self) -> None:
        result = docker_compose_down_cmd()
        expected = ["docker", "compose", "down"]
        assert result == expected


class TestDockerComposePullCmd:
    def test_main(self) -> None:
        result = docker_compose_pull_cmd()
        expected = ["docker", "compose", "pull"]
        assert result == expected


class TestDockerComposeUpCmd:
    def test_main(self) -> None:
        result = docker_compose_up_cmd()
        expected = ["docker", "compose", "up", "--detach"]
        assert result == expected

    def test_detach(self) -> None:
        result = docker_compose_up_cmd(detach=False)
        expected = ["docker", "compose", "up"]
        assert result == expected


class TestDockerCp:
    @skipif_ci
    @throttle_test(duration=5 * MINUTE)
    def test_into_container(self, *, container: str, temp_file: Path) -> None:
        with yield_docker_temp_dir(container) as temp_dir:
            dest = temp_dir / temp_file.name
            docker_cp(temp_file, (container, dest))
            docker_exec(
                container, *BASH_LS, input=f"if ! [ -f {dest} ]; then exit 1; fi"
            )

    @skipif_ci
    @throttle_test(duration=5 * MINUTE)
    def test_from_container(self, *, container: str, temp_path_not_exist: Path) -> None:
        with yield_docker_temp_dir(container) as temp_dir:
            src = temp_dir / temp_path_not_exist.name
            docker_exec(container, *touch_cmd(src))
            docker_cp((container, src), temp_path_not_exist)
        assert temp_path_not_exist.is_file()


class TestDockerCpCmd:
    def test_src(self) -> None:
        result = docker_cp_cmd(("cont", "src"), "dest")
        expected = ["docker", "cp", "cont:src", "dest"]
        assert result == expected

    def test_dest(self) -> None:
        result = docker_cp_cmd("src", ("cont", "dest"))
        expected = ["docker", "cp", "src", "cont:dest"]
        assert result == expected


class TestDockerExec:
    @mark.parametrize(
        ("cmd", "expected"),
        [
            param("hostname", re.compile(r"^[0-9a-f]{12}$")),
            param("whoami", re.compile(r"^root$")),
        ],
    )
    @skipif_ci
    @throttle_test(duration=5 * MINUTE)
    def test_main(
        self,
        *,
        capsys: CaptureFixture,
        container: str,
        cmd: str,
        expected: Pattern[str],
    ) -> None:
        result = docker_exec(container, cmd, print=True)
        assert result is None
        cap = capsys.readouterr()
        assert expected.search(cap.out)
        assert cap.err == ""


class TestDockerExecCmd:
    def test_main(self) -> None:
        result = docker_exec_cmd("container", "cmd")
        expected = ["docker", "exec", "container", "cmd"]
        assert result == expected

    def test_env(self) -> None:
        result = docker_exec_cmd("container", "cmd", env={"KEY": "value"})
        expected = ["docker", "exec", "--env", "KEY=value", "container", "cmd"]
        assert result == expected

    def test_env_kwargs(self) -> None:
        result = docker_exec_cmd("container", "cmd", KEY="value")
        expected = ["docker", "exec", "--env", "KEY=value", "container", "cmd"]
        assert result == expected

    def test_interactive(self) -> None:
        result = docker_exec_cmd("container", "cmd", interactive=True)
        expected = ["docker", "exec", "--interactive", "container", "cmd"]
        assert result == expected

    def test_user(self) -> None:
        result = docker_exec_cmd("container", "cmd", user="user")
        expected = ["docker", "exec", "--user", "user", "container", "cmd"]
        assert result == expected

    def test_workdir(self, *, tmp_path: Path) -> None:
        result = docker_exec_cmd("container", "cmd", workdir=tmp_path)
        expected = ["docker", "exec", "--workdir", str(tmp_path), "container", "cmd"]
        assert result == expected


class TestYieldDockerTempDir:
    @skipif_ci
    @throttle_test(duration=5 * MINUTE)
    def test_main(self, *, container: str) -> None:
        with yield_docker_temp_dir(container) as temp:
            docker_exec(container, *BASH_LS, input=self._raise_missing(temp))
            with raises(RunError):
                docker_exec(container, *BASH_LS, input=self._raise_present(temp))
        docker_exec(container, *BASH_LS, input=self._raise_present(temp))
        with raises(RunError):
            docker_exec(container, *BASH_LS, input=self._raise_missing(temp))

    @skipif_ci
    @throttle_test(duration=5 * MINUTE)
    def test_keep(self, *, container: str) -> None:
        with yield_docker_temp_dir(container, keep=True) as temp:
            ...
        docker_exec(container, *BASH_LS, input=self._raise_missing(temp))

    @skipif_ci
    @throttle_test(duration=5 * MINUTE)
    def test_keep_and_logger(
        self, *, caplog: LogCaptureFixture, container: str
    ) -> None:
        name = unique_str()
        logger = getLogger(name=name)
        logger.setLevel(INFO)
        with yield_docker_temp_dir(container, keep=True, logger=name):
            ...
        record = one(r for r in caplog.records if r.name == name)
        assert search(
            r"^Keeping temporary directory '[/\.\w]+'...$",
            record.message,
            flags=MULTILINE,
        )

    def _raise_missing(self, path: PathLike, /) -> str:
        return f"if ! [ -d {path} ]; then exit 1; fi"

    def _raise_present(self, path: PathLike, /) -> str:
        return f"if [ -d {path} ]; then exit 1; fi"
