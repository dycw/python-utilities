from __future__ import annotations

from typing import TYPE_CHECKING

from tests.conftest import SKIPIF_CI
from utilities.docker import docker_cp_cmd, docker_exec, docker_exec_cmd
from utilities.subprocess import echo_cmd, maybe_sudo_cmd, mkdir_cmd

if TYPE_CHECKING:
    from pathlib import Path


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
    @SKIPIF_CI
    def test_main(self) -> None:
        result = docker_exec("postgres", "true")
        assert result is None


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

    def test_user(self) -> None:
        result = docker_exec_cmd("container", "cmd", user="user")
        expected = ["docker", "exec", "--user", "user", "container", "cmd"]
        assert result == expected

    def test_workdir(self, *, tmp_path: Path) -> None:
        result = docker_exec_cmd("container", "cmd", workdir=tmp_path)
        expected = ["docker", "exec", "--workdir", str(tmp_path), "container", "cmd"]
        assert result == expected
