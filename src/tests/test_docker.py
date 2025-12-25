from __future__ import annotations

from subprocess import CalledProcessError
from typing import TYPE_CHECKING

from pytest import raises

from utilities.docker import (
    docker_cp,
    docker_cp_cmd,
    docker_exec,
    docker_exec_cmd,
    yield_docker_temp_dir,
)
from utilities.pytest import skipif_ci
from utilities.subprocess import BASH_LC, BASH_LS, touch_cmd

if TYPE_CHECKING:
    from pathlib import Path


class TestDockerCp:
    @skipif_ci
    def test_into_container(self, *, tmp_path: Path) -> None:
        src = tmp_path / "file.txt"
        src.touch()
        with yield_docker_temp_dir("postgres") as temp_cont:
            dest = temp_cont / src.name
            docker_cp(src, ("postgres", dest))
            docker_exec(
                "postgres", *BASH_LC, input=f"if ! [ -f {dest} ]; then exit 1; fi"
            )

    @skipif_ci
    def test_from_container(self, *, tmp_path: Path) -> None:
        with yield_docker_temp_dir("postgres") as temp_cont:
            src = temp_cont / "file.txt"
            docker_exec("postgres", *touch_cmd(src))
            dest = tmp_path / src.name
            docker_cp(("postgres", src), dest)
        assert dest.is_file()


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
    @skipif_ci
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
    def test_main(self) -> None:
        with yield_docker_temp_dir("postgres") as temp_dir:
            raise_if_present = f"if [ -d {temp_dir} ]; then exit 1; fi"
            raise_if_missing = f"if ! [ -d {temp_dir} ]; then exit 1; fi"
            docker_exec("postgres", *BASH_LS, input=raise_if_missing)
            with raises(CalledProcessError):
                docker_exec("postgres", *BASH_LS, input=raise_if_present)
        docker_exec("postgres", *BASH_LS, input=raise_if_present)
        with raises(CalledProcessError):
            docker_exec("postgres", *BASH_LS, input=raise_if_missing)
