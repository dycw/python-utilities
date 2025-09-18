from __future__ import annotations

from utilities.docker import docker_exec


class TestDockerExec:
    def test_main(self) -> None:
        result = docker_exec("container", "pg_dump")
        expected = ["docker", "exec", "--interactive", "container", "pg_dump"]
        assert result == expected

    def test_env(self) -> None:
        result = docker_exec("container", "pg_dump", KEY="value")
        expected = [
            "docker",
            "exec",
            "--env=KEY=value",
            "--interactive",
            "container",
            "pg_dump",
        ]
        assert result == expected
