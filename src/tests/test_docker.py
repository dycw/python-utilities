from __future__ import annotations

from utilities.docker import docker_exec_cmd


class TestDockerExecCmd:
    def test_main(self) -> None:
        result = docker_exec_cmd("container", "pg_dump")
        expected = ["docker", "exec", "container", "pg_dump"]
        assert result == expected

    def test_env(self) -> None:
        result = docker_exec_cmd("container", "pg_dump", KEY="value")
        expected = [
            "docker",
            "exec",
            "--env=KEY=value",
            "--interactive",
            "container",
            "pg_dump",
        ]
        assert result == expected
